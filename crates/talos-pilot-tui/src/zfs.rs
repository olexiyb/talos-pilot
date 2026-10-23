//! ZFS pool capacity per node
//!
//! Talos can't run `zpool list`, and a pool's root dataset usually isn't
//! mounted, so the Mounts API only shows quota-limited datasets (PVCs) whose
//! sizes say nothing about the pool. OpenEBS ZFS LocalPV publishes each node's
//! pools with free/used space as a `ZFSNode` resource; we read those.
//! Clusters without OpenEBS ZFS simply report no pools.
//!
//! The volumes themselves (`ZFSVolume` resources) are listed too, for the
//! volumes view: one dataset (or zvol) on one node, no replication, with its
//! real size and usage from the Talos Mounts API.

use k8s_openapi::serde_json::Value;
use kube::Client;
use kube::api::{Api, ApiResource, DynamicObject, GroupVersionKind, ListParams};
use std::collections::HashMap;

/// Capacity of one ZFS pool (bytes)
#[derive(Debug, Clone, PartialEq)]
pub struct ZfsPool {
    pub name: String,
    pub free: u64,
    pub used: u64,
}

impl ZfsPool {
    pub fn size(&self) -> u64 {
        self.free + self.used
    }

    /// Percentage of the pool in use, `None` for an empty pool
    pub fn used_pct(&self) -> Option<f32> {
        let size = self.size();
        (size > 0).then(|| self.used as f32 / size as f32 * 100.0)
    }
}

/// One OpenEBS ZFS LocalPV volume
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ZfsVolume {
    /// Volume (and PV) name, e.g. `pvc-…`
    pub name: String,
    /// Namespace of the `ZFSVolume` resource (the OpenEBS namespace)
    pub namespace: String,
    /// `namespace/name` of the PVC bound to the PV (see `crate::orphans`)
    pub pvc: Option<String>,
    /// Node holding the dataset
    pub node: String,
    /// Parent dataset, e.g. `datapool/localpv`
    pub pool: String,
    /// Requested capacity in bytes
    pub capacity: Option<u64>,
    /// `Ready`, `Pending`, `Failed`
    pub state: String,
    /// `DATASET` or `ZVOL`
    pub volume_type: String,
    /// Mounted size and used bytes, from the node's mounts (datasets only)
    pub fs: Option<(u64, u64)>,
    /// Marked for deletion; the node destroys the dataset
    pub deleting: bool,
}

impl ZfsVolume {
    /// The dataset as it appears in the node's mounts
    pub fn dataset(&self) -> String {
        format!("{}/{}", self.pool, self.name)
    }
}

/// API group of the OpenEBS ZFS LocalPV CRDs
const ZFS_GROUP: &str = "zfs.openebs.io";

/// Whether the cluster serves the ZFS LocalPV API group.
///
/// Listing a CRD that isn't installed returns a plain-text 404, which kube
/// logs as a warning ("Unsuccessful data error parse: 404 page not found")
/// before we can treat it as "no ZFS"; the cluster view lists ZFS pools on
/// every refresh. The group list is always served, so ask it first.
async fn zfs_installed(client: &Client) -> Result<bool, kube::Error> {
    let groups = client.list_api_groups().await?;
    Ok(groups.groups.iter().any(|g| g.name == ZFS_GROUP))
}

/// OpenEBS ZFS LocalPV volumes, `None` when the `ZFSVolume` CRD isn't
/// installed. Their PVC names come from the PVs, set by the caller.
pub async fn fetch_zfs_volumes(client: &Client) -> Result<Option<Vec<ZfsVolume>>, kube::Error> {
    if !zfs_installed(client).await? {
        return Ok(None);
    }
    let gvk = GroupVersionKind::gvk(ZFS_GROUP, "v1", "ZFSVolume");
    let resource = ApiResource::from_gvk_with_plural(&gvk, "zfsvolumes");
    let api: Api<DynamicObject> = Api::all_with(client.clone(), &resource);
    match crate::kube_list::list(&api, &ListParams::default()).await {
        Ok(list) => Ok(Some(zfs_volumes_from(list.items))),
        Err(kube::Error::Api(e)) if e.code == 404 => Ok(None),
        Err(e) => Err(e),
    }
}

/// Parse `ZFSVolume` objects, sorted by name
fn zfs_volumes_from(objects: Vec<DynamicObject>) -> Vec<ZfsVolume> {
    let mut volumes: Vec<ZfsVolume> = objects
        .into_iter()
        .filter_map(|obj| {
            let name = obj.metadata.name?;
            let spec = obj.data.get("spec")?;
            let field = |key: &str| spec.get(key).and_then(Value::as_str).map(str::to_string);
            Some(ZfsVolume {
                deleting: obj.metadata.deletion_timestamp.is_some(),
                namespace: obj.metadata.namespace?,
                pvc: None,
                node: field("ownerNodeID")?,
                pool: field("poolName")?,
                capacity: field("capacity").and_then(|c| c.parse().ok()),
                state: obj
                    .data
                    .pointer("/status/state")
                    .and_then(Value::as_str)
                    .unwrap_or("Unknown")
                    .to_string(),
                volume_type: field("volumeType").unwrap_or_else(|| "DATASET".to_string()),
                fs: None,
                name,
            })
        })
        .collect();
    volumes.sort_by(|a, b| a.name.cmp(&b.name));
    volumes
}

/// Set each dataset's mounted size and usage from its node's mounts
pub fn apply_zfs_mounts(
    volumes: &mut [ZfsVolume],
    mounts: &HashMap<String, Vec<talos_rs::MountUsage>>,
) {
    for volume in volumes {
        let dataset = volume.dataset();
        if let Some(m) = mounts
            .get(&volume.node)
            .and_then(|ms| ms.iter().find(|m| m.filesystem == dataset))
        {
            volume.fs = Some((m.size, m.size.saturating_sub(m.available)));
        }
    }
}

/// A dataset whose quota is this far below its capacity wasn't resized
const QUOTA_BELOW_CAPACITY_PCT: u64 = 90;

/// Problems of a ZFS LocalPV volume, worst first
pub fn zfs_volume_issues(volume: &ZfsVolume) -> Vec<crate::linstor_volumes::Issue> {
    use crate::linstor_volumes::{Issue, Severity, format_kib, usage_issue};
    let mut issues = Vec::new();
    match volume.state.as_str() {
        "Ready" => {}
        "Pending" => issues.push(Issue {
            severity: Severity::Warning,
            text: format!("{}: volume Pending", volume.node),
        }),
        state => issues.push(Issue {
            severity: Severity::Error,
            text: format!("{}: volume {state}", volume.node),
        }),
    }
    if let Some((size, used)) = volume.fs {
        if let Some(issue) = usage_issue(&volume.node, used / 1024, size / 1024) {
            issues.push(issue);
        }
        if let Some(capacity) = volume.capacity
            && size < capacity / 100 * QUOTA_BELOW_CAPACITY_PCT
        {
            issues.push(Issue {
                severity: Severity::Warning,
                text: format!(
                    "{}: dataset {} below capacity {}: resize not applied",
                    volume.node,
                    format_kib(size / 1024),
                    format_kib(capacity / 1024)
                ),
            });
        }
    }
    issues.sort_by_key(|i| i.severity);
    issues
}

/// ZFS pools keyed by Kubernetes node name.
///
/// Returns an empty map when the `ZFSNode` CRD isn't installed.
pub async fn fetch_zfs_pools(
    client: &Client,
) -> Result<HashMap<String, Vec<ZfsPool>>, kube::Error> {
    if !zfs_installed(client).await? {
        return Ok(HashMap::new());
    }
    let gvk = GroupVersionKind::gvk(ZFS_GROUP, "v1", "ZFSNode");
    let resource = ApiResource::from_gvk_with_plural(&gvk, "zfsnodes");
    let api: Api<DynamicObject> = Api::all_with(client.clone(), &resource);

    let list = match crate::kube_list::list(&api, &ListParams::default()).await {
        Ok(list) => list,
        Err(kube::Error::Api(e)) if e.code == 404 => return Ok(HashMap::new()),
        Err(e) => return Err(e),
    };

    Ok(list
        .items
        .into_iter()
        .filter_map(|obj| {
            let node = obj.metadata.name?;
            let pools = pools_from(obj.data.get("pools")?);
            Some((node, pools))
        })
        .collect())
}

/// Parse the `pools` array of a ZFSNode, skipping entries without sizes
fn pools_from(pools: &Value) -> Vec<ZfsPool> {
    pools
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|p| {
            Some(ZfsPool {
                name: p.get("name")?.as_str()?.to_string(),
                free: parse_quantity(p.get("free")?.as_str()?)?,
                used: parse_quantity(p.get("used")?.as_str()?)?,
            })
        })
        .collect()
}

/// Parse a Kubernetes quantity such as `838259748Ki` or `1G` into bytes
fn parse_quantity(q: &str) -> Option<u64> {
    let q = q.trim();
    let split = q
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(q.len());
    let (number, suffix) = q.split_at(split);
    let multiplier: u64 = match suffix {
        "" => 1,
        "Ki" => 1 << 10,
        "Mi" => 1 << 20,
        "Gi" => 1 << 30,
        "Ti" => 1 << 40,
        "Pi" => 1 << 50,
        "k" => 1_000,
        "M" => 1_000_000,
        "G" => 1_000_000_000,
        "T" => 1_000_000_000_000,
        "P" => 1_000_000_000_000_000,
        _ => return None,
    };
    let value: f64 = number.parse().ok()?;
    Some((value * multiplier as f64) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::serde_json::json;

    fn zfs_volume(state: &str, capacity: u64, fs: Option<(u64, u64)>) -> ZfsVolume {
        ZfsVolume {
            name: "pvc-1".to_string(),
            node: "bc2".to_string(),
            pool: "datapool/localpv".to_string(),
            capacity: Some(capacity),
            state: state.to_string(),
            volume_type: "DATASET".to_string(),
            fs,
            ..Default::default()
        }
    }

    #[test]
    fn parses_zfs_volumes() {
        let obj: DynamicObject = k8s_openapi::serde_json::from_value(json!({
            "apiVersion": "zfs.openebs.io/v1", "kind": "ZFSVolume",
            "metadata": {"name": "pvc-1", "namespace": "openebs"},
            "spec": {"capacity": "1073741824", "ownerNodeID": "bc2",
                     "poolName": "datapool/localpv", "volumeType": "DATASET"},
            "status": {"state": "Ready"}
        }))
        .unwrap();
        let volumes = zfs_volumes_from(vec![obj]);
        assert_eq!(volumes.len(), 1);
        assert_eq!(volumes[0].namespace, "openebs");
        assert_eq!(volumes[0].capacity, Some(1 << 30));
        assert_eq!(volumes[0].dataset(), "datapool/localpv/pvc-1");
    }

    #[test]
    fn zfs_volume_issues_cover_state_usage_and_resize() {
        let gib = 1u64 << 30;
        // Ready, quota a little above capacity (ZFS rounds up), 10% used
        assert!(
            zfs_volume_issues(&zfs_volume("Ready", gib, Some((gib + gib / 16, gib / 10))))
                .is_empty()
        );
        // Not mounted: nothing to check but the state
        assert!(zfs_volume_issues(&zfs_volume("Ready", gib, None)).is_empty());

        let failed = zfs_volume_issues(&zfs_volume("Failed", gib, None));
        assert_eq!(failed[0].text, "bc2: volume Failed");

        let full = zfs_volume_issues(&zfs_volume("Ready", gib, Some((gib, gib * 96 / 100))));
        assert!(full[0].text.starts_with("bc2: filesystem 96% full"));

        let not_resized = zfs_volume_issues(&zfs_volume("Ready", 2 * gib, Some((gib, 0))));
        assert!(not_resized[0].text.contains("resize not applied"));
    }

    #[test]
    fn quantities() {
        assert_eq!(parse_quantity("838259748Ki"), Some(838_259_748 * 1024));
        assert_eq!(parse_quantity("1.5Gi"), Some(1_610_612_736));
        assert_eq!(parse_quantity("2G"), Some(2_000_000_000));
        assert_eq!(parse_quantity("512"), Some(512));
        assert_eq!(parse_quantity("12Xi"), None);
        assert_eq!(parse_quantity(""), None);
    }

    #[test]
    fn zfsnode_pools() {
        let pools = json!([
            {"name": "datapool", "free": "3Gi", "used": "1Gi", "uuid": "7"},
            {"name": "broken", "uuid": "8"}
        ]);
        let parsed = pools_from(&pools);
        assert_eq!(
            parsed,
            [ZfsPool {
                name: "datapool".to_string(),
                free: 3 << 30,
                used: 1 << 30,
            }]
        );
        assert_eq!(parsed[0].used_pct(), Some(25.0));
    }
}

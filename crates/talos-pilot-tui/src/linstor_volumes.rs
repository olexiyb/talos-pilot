//! LINSTOR volumes and the DRBD state behind them
//!
//! LINSTOR's view of a volume (`linstor resource list`) can say every replica
//! is UpToDate while the kernel disagrees: a DRBD device that never grew
//! after a resize, or blocks still out of sync on an "Established" link.
//! So besides LINSTOR's resource view, each node's DRBD state is read from the
//! kernel through the Talos API:
//!
//! - `/sys/block/drbd<minor>/size`: the device size actually in use
//! - DRBD debugfs `resources/<res>/connections/<peer>/<vol>/proc_drbd`:
//!   connection / disk state and `oos:` (KiB out of sync) per peer
//! - DRBD debugfs `resources/<res>/connections/<peer>/debug`: `ap_in_flight`
//! - the Mounts API on the Primary's node: size of the filesystem on the
//!   DRBD device, which a resize must grow too
//!
//! debugfs is marked "format will change without notice"; anything that
//! can't be read or parsed is reported as unknown, never as a problem.

use crate::linstor::{controller_base, get_json};
use futures::StreamExt;
use k8s_openapi::serde_json::Value;
use std::collections::HashMap;
use talos_rs::{MountUsage, TalosClient};

/// Volumes of the cluster's LINSTOR resources, with each replica's DRBD
/// state as LINSTOR reports it
#[derive(Debug, Clone, PartialEq, Default)]
pub struct LinstorVolume {
    /// Resource (definition) name, e.g. `pvc-…`
    pub resource: String,
    pub volume_number: u32,
    /// `namespace/name` of the PVC, from the CSI driver's properties
    pub pvc: Option<String>,
    /// Size of the volume definition (what LINSTOR thinks it is)
    pub size_kib: Option<u64>,
    /// Resource group, and how many diskful replicas it places
    pub resource_group: Option<String>,
    pub place_count: Option<u32>,
    /// The definition is being deleted (`DELETE` flag)
    pub deleting: bool,
    /// One per node the resource is placed on, sorted by node
    pub replicas: Vec<Replica>,
}

impl LinstorVolume {
    /// Replicas holding data (not diskless tie-breakers or clients)
    pub fn diskful_replicas(&self) -> usize {
        self.replicas.iter().filter(|r| !r.diskless).count()
    }

    /// Filesystem used and size (KiB) where the volume is mounted
    pub fn usage_kib(&self) -> Option<(u64, u64)> {
        self.replicas.iter().find_map(|r| {
            let d = r.drbd.as_ref()?;
            Some((d.fs_used_kib?, d.fs_size_kib?))
        })
    }
}

/// One node's replica of a volume
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Replica {
    pub node: String,
    /// Replicated through DRBD. Without it (a STORAGE-only resource, e.g. a
    /// single local volume for an app that replicates itself) LINSTOR
    /// reports no disk state and there is no DRBD state to check.
    pub drbd_layer: bool,
    /// DRBD Primary (the volume is mounted on this node)
    pub in_use: bool,
    /// No local disk (tie-breaker or diskless client)
    pub diskless: bool,
    /// e.g. `UpToDate`, `Outdated`, `Inconsistent`, `Diskless`
    pub disk_state: String,
    pub minor: Option<u32>,
    /// Peers as LINSTOR reports them
    pub peers: Vec<PeerLink>,
    /// What the node's kernel reports, when it could be read
    pub drbd: Option<DrbdObserved>,
}

/// A replica's link to one peer, as LINSTOR reports it
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PeerLink {
    pub peer: String,
    pub connected: bool,
    /// Connection message, e.g. `Connected`, `Connecting`, `StandAlone`
    pub message: String,
    /// e.g. `Established`, `SyncSource`, `SyncTarget`
    pub replication: Option<String>,
}

/// DRBD state read from a node's kernel
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DrbdObserved {
    /// Device size from `/sys/block`
    pub size_kib: Option<u64>,
    /// Size and used space of the filesystem mounted from the device
    /// (Mounts API), only where it is mounted (the Primary) and not for raw
    /// block volumes
    pub fs_size_kib: Option<u64>,
    pub fs_used_kib: Option<u64>,
    /// I/O frozen on the device (the `s` flag in debugfs `proc_drbd`):
    /// quorum lost with `on-no-quorum suspend-io`, fencing, or by hand.
    /// Writes of the app using it hang until it resumes.
    pub io_suspended: Option<bool>,
    /// Per peer, from debugfs
    pub peers: HashMap<String, PeerObserved>,
}

/// DRBD state of one peer device, from debugfs
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PeerObserved {
    /// Connection state (`cs:`), e.g. `Established`
    pub cs: Option<String>,
    /// KiB this node has marked out of sync with the peer (`oos:`)
    pub oos_kib: Option<u64>,
    /// Application writes in flight to the peer (`ap_in_flight`)
    pub ap_in_flight_kib: Option<u64>,
    /// Progress of a running resync or verify with the peer
    pub sync: Option<SyncProgress>,
}

/// A resync (or online verify) in progress, from the `sync'ed:` /
/// `finish:` lines DRBD adds to debugfs `proc_drbd` while one runs
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SyncProgress {
    /// Online verify rather than resync
    pub verify: bool,
    pub percent: f32,
    /// Estimated seconds left; `None` when DRBD shows `∞`: the out-of-sync
    /// blocks aren't shrinking under the current writes
    pub finish_secs: Option<u64>,
    /// Speed over the last ~18 s (KiB/s)
    pub speed_kib: Option<u64>,
    /// No progress for over 3 minutes
    pub stalled: bool,
}

impl SyncProgress {
    /// e.g. `resync 37.0%, 109 MiB/s, ~4m 12s left`
    pub fn describe(&self) -> String {
        let what = if self.verify { "verify" } else { "resync" };
        let mut text = format!("{what} {:.1}%", self.percent);
        if let Some(speed) = self.speed_kib {
            text.push_str(&format!(", {}/s", format_kib(speed)));
        }
        if self.stalled {
            text.push_str(", stalled for over 3 min");
        } else {
            match self.finish_secs {
                Some(secs) => text.push_str(&format!(", ~{} left", format_eta(secs))),
                None => text.push_str(", not converging under the current writes"),
            }
        }
        text
    }
}

/// Seconds as `1h 05m`, `4m 12s` or `9s`
fn format_eta(secs: u64) -> String {
    let (h, m, s) = (secs / 3600, (secs % 3600) / 60, secs % 60);
    if h > 0 {
        format!("{h}h {m:02}m")
    } else if m > 0 {
        format!("{m}m {s:02}s")
    } else {
        format!("{s}s")
    }
}

/// Resync progress from a debugfs `proc_drbd` file, `None` when no resync
/// or verify is running (or the lines can't be parsed):
///
/// ```text
///     [===>................] sync'ed: 18.4% (9012/11048)M
///     finish: 0:04:12 speed: 112,345 (110,000 -- 98,000) want: 250,000 K/sec
/// ```
fn parse_sync_progress(content: &str) -> Option<SyncProgress> {
    let mut words = content.split_whitespace().peekable();
    let mut progress = None::<SyncProgress>;
    while let Some(word) = words.next() {
        match word {
            "sync'ed:" | "verified:" => {
                let percent = words.next()?.strip_suffix('%')?.parse().ok()?;
                progress = Some(SyncProgress {
                    verify: word == "verified:",
                    percent,
                    ..Default::default()
                });
            }
            "finish:" => {
                let p = progress.as_mut()?;
                let value = words.next()?;
                p.finish_secs = if value == "∞" {
                    None
                } else {
                    let parts: Vec<u64> = value
                        .split(':')
                        .map(|n| n.parse().ok())
                        .collect::<Option<_>>()?;
                    match parts[..] {
                        [h, m, s] => Some(h * 3600 + m * 60 + s),
                        _ => return None,
                    }
                };
            }
            "speed:" => {
                let p = progress.as_mut()?;
                p.speed_kib = words.next()?.replace(',', "").parse().ok();
            }
            "(stalled)" => progress.as_mut()?.stalled = true,
            _ => {}
        }
    }
    progress
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    Error,
    Warning,
}

/// A problem found on a volume
#[derive(Debug, Clone, PartialEq)]
pub struct Issue {
    pub severity: Severity,
    pub text: String,
}

/// The LINSTOR satellite pod running on a node, where `drbdadm` can be run
#[derive(Debug, Clone, PartialEq)]
pub struct SatellitePod {
    pub namespace: String,
    pub name: String,
}

/// A suggested manual fix for a volume: what it does, and the commands
#[derive(Debug, Clone, PartialEq)]
pub struct Remedy {
    pub summary: String,
    pub commands: Vec<String>,
}

/// Label of the satellite pods created by the Piraeus operator
const SATELLITE_LABEL: &str = "app.kubernetes.io/component=linstor-satellite";
/// Container of a satellite pod that has `drbdadm`
const SATELLITE_CONTAINER: &str = "linstor-satellite";

/// Satellite pod per node name. Empty when they can't be listed; fix
/// suggestions then name the pod as a placeholder.
pub async fn fetch_satellites(client: &kube::Client) -> HashMap<String, SatellitePod> {
    let pods: kube::Api<k8s_openapi::api::core::v1::Pod> = kube::Api::all(client.clone());
    let params = kube::api::ListParams::default().labels(SATELLITE_LABEL);
    match pods.list(&params).await {
        Ok(list) => list
            .items
            .into_iter()
            .filter_map(|pod| {
                let node = pod.spec?.node_name?;
                Some((
                    node,
                    SatellitePod {
                        namespace: pod.metadata.namespace?,
                        name: pod.metadata.name?,
                    },
                ))
            })
            .collect(),
        Err(e) => {
            tracing::debug!("LINSTOR satellite pods unavailable: {}", e);
            HashMap::new()
        }
    }
}

/// LINSTOR volumes of the cluster, `None` when it doesn't run Piraeus.
/// `Err` when the Kubernetes or LINSTOR API can't be queried.
pub async fn fetch_volumes(client: &kube::Client) -> Result<Option<Vec<LinstorVolume>>, String> {
    let base = match controller_base(client).await.map_err(|e| e.to_string())? {
        None => return Ok(None),
        Some(base) => base?,
    };
    let resources_path = format!("{base}/v1/view/resources");
    let definitions_path = format!("{base}/v1/resource-definitions?with_volume_definitions=true");
    let groups_path = format!("{base}/v1/resource-groups");
    let (resources, definitions, groups) = tokio::join!(
        get_json(client, &resources_path),
        get_json(client, &definitions_path),
        get_json(client, &groups_path),
    );
    let mut volumes = volumes_from(&resources?, &definitions?);
    // Without the groups the replica count is shown without a target
    match groups {
        Ok(groups) => apply_place_counts(&mut volumes, &groups),
        Err(e) => tracing::debug!("LINSTOR resource groups unavailable: {}", e),
    }
    Ok(Some(volumes))
}

/// Set each volume's `place_count` from its resource group's select filter
pub fn apply_place_counts(volumes: &mut [LinstorVolume], groups: &Value) {
    let counts: HashMap<&str, u32> = groups
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|g| {
            Some((
                g.get("name")?.as_str()?,
                g.pointer("/select_filter/place_count")?.as_u64()? as u32,
            ))
        })
        .collect();
    for volume in volumes {
        volume.place_count = volume
            .resource_group
            .as_deref()
            .and_then(|g| counts.get(g).copied());
    }
}

/// Group LINSTOR's resource view (one entry per resource and node) into
/// volumes, with sizes and PVC names from the resource definitions
pub fn volumes_from(resources: &Value, definitions: &Value) -> Vec<LinstorVolume> {
    let str_at = |v: &Value, key: &str| v.get(key).and_then(Value::as_str).map(str::to_string);

    // resource -> (pvc, resource group, deleting, volume number -> size)
    type Definition = (Option<String>, Option<String>, bool, HashMap<u64, u64>);
    let mut defs: HashMap<String, Definition> = HashMap::new();
    for rd in definitions.as_array().into_iter().flatten() {
        let Some(name) = str_at(rd, "name") else {
            continue;
        };
        let props = rd.get("props");
        let prop = |key: &str| props.and_then(|p| p.get(key)).and_then(Value::as_str);
        let pvc = match (
            prop("Aux/csi.storage.k8s.io/pvc/namespace"),
            prop("Aux/csi.storage.k8s.io/pvc/name"),
        ) {
            (Some(ns), Some(name)) => Some(format!("{ns}/{name}")),
            _ => None,
        };
        let sizes = rd
            .get("volume_definitions")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|vd| {
                Some((
                    vd.get("volume_number")?.as_u64()?,
                    vd.get("size_kib")?.as_u64()?,
                ))
            })
            .collect();
        let deleting = rd
            .get("flags")
            .and_then(Value::as_array)
            .is_some_and(|flags| flags.iter().any(|f| f.as_str() == Some("DELETE")));
        defs.insert(
            name,
            (pvc, str_at(rd, "resource_group_name"), deleting, sizes),
        );
    }

    let mut volumes: HashMap<(String, u64), LinstorVolume> = HashMap::new();
    for res in resources.as_array().into_iter().flatten() {
        let (Some(name), Some(node)) = (str_at(res, "name"), str_at(res, "node_name")) else {
            continue;
        };
        let in_use = res
            .pointer("/state/in_use")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let flags: Vec<&str> = res
            .get("flags")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect();
        let drbd = res.pointer("/layer_object/drbd");
        let connections: Vec<(String, bool, String)> = drbd
            .and_then(|d| d.get("connections"))
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
            .map(|(peer, c)| {
                (
                    peer.clone(),
                    c.get("connected").and_then(Value::as_bool).unwrap_or(false),
                    str_at(c, "message").unwrap_or_default(),
                )
            })
            .collect();

        for vol in res
            .get("volumes")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let Some(number) = vol.get("volume_number").and_then(Value::as_u64) else {
                continue;
            };
            let diskless = flags.iter().any(|f| f.contains("DISKLESS"))
                || vol.get("provider_kind").and_then(Value::as_str) == Some("DISKLESS");
            let drbd_layer_data = vol
                .get("layer_data_list")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .find(|l| l.get("type").and_then(Value::as_str) == Some("DRBD"));
            let drbd_layer = drbd.is_some() || drbd_layer_data.is_some();
            let minor = drbd_layer_data
                .and_then(|l| l.pointer("/data/drbd_volume_definition/minor_number"))
                .and_then(Value::as_u64)
                .map(|m| m as u32);
            let replication = vol.pointer("/state/replication_states");
            let mut peers: Vec<PeerLink> = connections
                .iter()
                .map(|(peer, connected, message)| PeerLink {
                    peer: peer.clone(),
                    connected: *connected,
                    message: message.clone(),
                    replication: replication
                        .and_then(|r| r.get(peer))
                        .and_then(|r| str_at(r, "replication_state")),
                })
                .collect();
            peers.sort_by(|a, b| a.peer.cmp(&b.peer));

            let (pvc, resource_group, deleting, size_kib) = match defs.get(&name) {
                Some((pvc, group, deleting, sizes)) => (
                    pvc.clone(),
                    group.clone(),
                    *deleting,
                    sizes.get(&number).copied(),
                ),
                None => (None, None, false, None),
            };
            let volume = volumes
                .entry((name.clone(), number))
                .or_insert_with(|| LinstorVolume {
                    resource: name.clone(),
                    volume_number: number as u32,
                    pvc,
                    size_kib,
                    resource_group,
                    place_count: None,
                    deleting,
                    replicas: Vec::new(),
                });
            volume.replicas.push(Replica {
                node: node.clone(),
                drbd_layer,
                in_use,
                diskless,
                disk_state: vol
                    .pointer("/state/disk_state")
                    .and_then(Value::as_str)
                    .unwrap_or("Unknown")
                    .to_string(),
                minor,
                peers,
                drbd: None,
            });
        }
    }

    let mut volumes: Vec<LinstorVolume> = volumes.into_values().collect();
    for v in &mut volumes {
        v.replicas.sort_by(|a, b| a.node.cmp(&b.node));
    }
    volumes.sort_by(|a, b| {
        a.pvc
            .is_none()
            .cmp(&b.pvc.is_none())
            .then_with(|| a.pvc.cmp(&b.pvc))
            .then_with(|| a.resource.cmp(&b.resource))
            .then_with(|| a.volume_number.cmp(&b.volume_number))
    });
    volumes
}

/// How many kernel reads run at once across all nodes
const OBSERVE_CONCURRENCY: usize = 24;
/// Upper bound for one Talos file read
const READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// One file to read from a node for a replica
enum Probe {
    Size,
    PeerDevice(String),
    PeerConnection(String),
}

/// Fill in `Replica::drbd` from each node's kernel, for replicas on nodes in
/// `node_ips` (node name -> Talos address). Replicas on other nodes, or whose
/// reads all fail, stay `None`.
pub async fn observe_drbd(
    talos: &TalosClient,
    node_ips: &HashMap<String, String>,
    volumes: &mut [LinstorVolume],
) {
    let mut probes = Vec::new();
    for (vi, volume) in volumes.iter().enumerate() {
        for (ri, replica) in volume.replicas.iter().enumerate() {
            let Some(ip) = node_ips.get(&replica.node) else {
                continue;
            };
            let client = talos.with_node(ip);
            let debugfs = format!("/sys/kernel/debug/drbd/resources/{}", volume.resource);
            if let Some(minor) = replica.minor {
                probes.push((
                    (vi, ri),
                    Probe::Size,
                    client.clone(),
                    format!("/sys/block/drbd{minor}/size"),
                ));
            }
            for peer in &replica.peers {
                let conn = format!("{debugfs}/connections/{}", peer.peer);
                probes.push((
                    (vi, ri),
                    Probe::PeerDevice(peer.peer.clone()),
                    client.clone(),
                    format!("{conn}/{}/proc_drbd", volume.volume_number),
                ));
                probes.push((
                    (vi, ri),
                    Probe::PeerConnection(peer.peer.clone()),
                    client.clone(),
                    format!("{conn}/debug"),
                ));
            }
        }
    }

    let reads = futures::stream::iter(probes)
        .map(|(at, probe, client, path)| async move {
            let content = tokio::time::timeout(READ_TIMEOUT, client.read_file(&path))
                .await
                .ok()
                .and_then(|r| r.ok());
            (at, probe, content)
        })
        .buffer_unordered(OBSERVE_CONCURRENCY)
        .collect::<Vec<_>>();
    let results = reads.await;

    for ((vi, ri), probe, content) in results {
        let Some(content) = content else {
            continue;
        };
        let replica = &mut volumes[vi].replicas[ri];
        let observed = replica.drbd.get_or_insert_with(DrbdObserved::default);
        match probe {
            // 512-byte sectors
            Probe::Size => observed.size_kib = content.trim().parse::<u64>().ok().map(|s| s / 2),
            Probe::PeerDevice(peer) => {
                let parsed = parse_proc_drbd(&content);
                // Per device; every peer's file reports the same
                if let Some(suspended) = parsed.suspended {
                    observed.io_suspended = Some(observed.io_suspended == Some(true) || suspended);
                }
                let entry = observed.peers.entry(peer).or_default();
                entry.cs = parsed.cs;
                entry.oos_kib = parsed.oos_kib;
                entry.sync = parse_sync_progress(&content);
            }
            Probe::PeerConnection(peer) => {
                observed.peers.entry(peer).or_default().ap_in_flight_kib =
                    parse_ap_in_flight(&content);
            }
        }
    }
}

/// Mounted filesystems per node name, one Mounts call per node, each
/// bounded. A node whose call fails is left out.
pub async fn fetch_mounts(
    talos: &TalosClient,
    node_ips: &HashMap<String, String>,
    nodes: &[String],
) -> HashMap<String, Vec<MountUsage>> {
    let calls = nodes.iter().filter_map(|node| {
        let client = talos.with_node(node_ips.get(node)?);
        Some(async move {
            let mounts = tokio::time::timeout(READ_TIMEOUT, client.mounts())
                .await
                .ok()?
                .ok()?;
            Some((
                node.clone(),
                mounts.into_iter().flat_map(|m| m.mounts).collect(),
            ))
        })
    });
    futures::future::join_all(calls)
        .await
        .into_iter()
        .flatten()
        .collect()
}

/// Nodes whose mounts the LINSTOR volumes need: where they are in use
pub fn linstor_mount_nodes(volumes: &[LinstorVolume]) -> Vec<String> {
    volumes
        .iter()
        .flat_map(|v| &v.replicas)
        .filter(|r| r.in_use && r.drbd_layer)
        .map(|r| r.node.clone())
        .collect()
}

/// Set the filesystem size and usage of each in-use replica from the mount
/// of its DRBD device. A device mounted several times (pod mount, subPaths)
/// has the same size everywhere.
pub fn apply_mounts(volumes: &mut [LinstorVolume], mounts: &HashMap<String, Vec<MountUsage>>) {
    for volume in volumes {
        for replica in volume.replicas.iter_mut().filter(|r| r.in_use) {
            let (Some(minor), Some(node_mounts)) = (replica.minor, mounts.get(&replica.node))
            else {
                continue;
            };
            let device = format!("/dev/drbd{minor}");
            if let Some(m) = node_mounts.iter().find(|m| m.filesystem == device) {
                let observed = replica.drbd.get_or_insert_with(DrbdObserved::default);
                observed.fs_size_kib = Some(m.size / 1024);
                observed.fs_used_kib = Some(m.size.saturating_sub(m.available) / 1024);
            }
        }
    }
}

/// Filesystem fill levels that are a warning / an error (percent)
const USAGE_WARNING_PCT: f64 = 85.0;
const USAGE_ERROR_PCT: f64 = 95.0;

/// An issue for a filesystem filling up on `node`
pub fn usage_issue(node: &str, used_kib: u64, size_kib: u64) -> Option<Issue> {
    if size_kib == 0 {
        return None;
    }
    let pct = used_kib as f64 / size_kib as f64 * 100.0;
    let severity = if pct >= USAGE_ERROR_PCT {
        Severity::Error
    } else if pct >= USAGE_WARNING_PCT {
        Severity::Warning
    } else {
        return None;
    };
    Some(Issue {
        severity,
        text: format!(
            "{node}: filesystem {pct:.0}% full ({} of {})",
            format_kib(used_kib),
            format_kib(size_kib)
        ),
    })
}

/// What a debugfs `proc_drbd` file says about a peer device
#[derive(Debug, PartialEq, Default)]
struct ProcDrbd {
    cs: Option<String>,
    oos_kib: Option<u64>,
    /// First of the state flags: `s` suspended, `r` running
    suspended: Option<bool>,
}

/// `cs:`, `oos:` and the suspended flag from a debugfs `proc_drbd` file, e.g.
/// `1015: cs:Established ro:Secondary/Secondary ds:UpToDate/UpToDate C r-----`
/// followed by `ns:0 nr:0 … oos:0`
fn parse_proc_drbd(content: &str) -> ProcDrbd {
    let field = |key: &str| {
        content
            .split_whitespace()
            .find_map(|t| t.strip_prefix(key))
            .map(str::to_string)
    };
    // The flags are the last word of the first line, six characters
    let suspended = content
        .lines()
        .next()
        .filter(|l| l.contains("cs:"))
        .and_then(|l| l.split_whitespace().last())
        .filter(|f| f.len() == 6)
        .and_then(|f| match f.as_bytes()[0] {
            b's' => Some(true),
            b'r' => Some(false),
            _ => None,
        });
    ProcDrbd {
        cs: field("cs:"),
        oos_kib: field("oos:").and_then(|v| v.parse().ok()),
        suspended,
    }
}

/// `ap_in_flight` in KiB from a debugfs connection `debug` file
/// (`ap_in_flight: 0 KiB (0 sectors)`)
fn parse_ap_in_flight(content: &str) -> Option<u64> {
    content.lines().find_map(|line| {
        let rest = line.trim().strip_prefix("ap_in_flight:")?;
        rest.split_whitespace().next()?.parse().ok()
    })
}

/// Filesystem overhead allowed before a filesystem counts as not grown to
/// its device: fixed (journal/log, superblocks) plus proportional (inode
/// tables, allocation groups). Healthy volumes measured on three clusters
/// lose up to 187 MiB of 1 GiB and 64 MiB of 300 MiB.
const FS_OVERHEAD_FIXED_KIB: u64 = 160 * 1024;
const FS_OVERHEAD_PERCENT: u64 = 8;

/// In-flight writes beyond this are a wrapped-around (underflowed) counter
const AP_IN_FLIGHT_IMPLAUSIBLE_KIB: u64 = 1 << 40;

/// Problems of a volume, worst first
pub fn volume_issues(volume: &LinstorVolume) -> Vec<Issue> {
    let mut issues = Vec::new();
    let mut push = |severity, text: String| {
        if !issues.iter().any(|i: &Issue| i.text == text) {
            issues.push(Issue { severity, text });
        }
    };

    let syncing = |r: &Replica| {
        r.peers.iter().any(|p| {
            p.replication
                .as_deref()
                .is_some_and(|s| s.starts_with("Sync") || s.starts_with("PausedSync"))
        })
    };

    if let Some(wanted) = volume.place_count {
        let have = volume.diskful_replicas();
        if have < wanted as usize {
            push(
                Severity::Error,
                format!("only {have} of {wanted} replicas hold data"),
            );
        }
    }
    if let Some(node) = volume.replicas.iter().find(|r| r.in_use).map(|r| &r.node)
        && let Some((used, size)) = volume.usage_kib()
        && let Some(issue) = usage_issue(node, used, size)
    {
        push(issue.severity, issue.text);
    }

    for r in &volume.replicas {
        if !r.drbd_layer {
            continue;
        }
        if !r.diskless && r.disk_state != "UpToDate" {
            if r.disk_state == "Inconsistent" && syncing(r) {
                push(
                    Severity::Warning,
                    format!("{}: Inconsistent (syncing)", r.node),
                );
            } else {
                push(
                    Severity::Error,
                    format!("{}: disk {}", r.node, r.disk_state),
                );
            }
        }
        for p in &r.peers {
            // Both ends report a broken link; name the pair once
            let (a, b) = if r.node < p.peer {
                (&r.node, &p.peer)
            } else {
                (&p.peer, &r.node)
            };
            if !p.connected {
                push(Severity::Error, format!("{a} ↔ {b}: {}", p.message));
            } else if let Some(rep) = p.replication.as_deref()
                && rep != "Established"
            {
                push(
                    Severity::Warning,
                    format!("{} → {}: {}", r.node, p.peer, rep),
                );
            }
        }

        let Some(observed) = &r.drbd else {
            continue;
        };
        if observed.io_suspended == Some(true) {
            push(
                Severity::Error,
                format!(
                    "{}: I/O suspended (quorum lost, fencing or by hand): writes hang",
                    r.node
                ),
            );
        }
        if let (Some(actual), Some(expected)) = (observed.size_kib, volume.size_kib)
            && actual < expected
        {
            push(
                Severity::Error,
                format!(
                    "{}: DRBD device {} < LINSTOR {}",
                    r.node,
                    format_kib(actual),
                    format_kib(expected)
                ),
            );
        }
        // A resize must also grow the filesystem (the 2026-10-17 incident's
        // XFS stayed at 5.9 GiB on an 11 GiB device); briefly smaller while
        // a resize runs, hence a warning
        if let (Some(fs), Some(device)) = (observed.fs_size_kib, observed.size_kib)
            && fs + FS_OVERHEAD_FIXED_KIB + device * FS_OVERHEAD_PERCENT / 100 < device
        {
            push(
                Severity::Warning,
                format!(
                    "{}: filesystem {} on a {} DRBD device: not grown after a resize",
                    r.node,
                    format_kib(fs),
                    format_kib(device)
                ),
            );
        }
        let mut peers: Vec<_> = observed.peers.iter().collect();
        peers.sort_by(|a, b| a.0.cmp(b.0));
        for (peer, seen) in peers {
            if let Some(oos) = seen.oos_kib
                && oos > 0
            {
                let established = seen.cs.as_deref() == Some("Established")
                    && !r.peers.iter().any(|p| {
                        &p.peer == peer
                            && p.replication.as_deref().is_some_and(|s| s != "Established")
                    });
                let peer_is_primary = volume.replicas.iter().any(|p| &p.node == peer && p.in_use);
                if !established {
                    let progress = seen
                        .sync
                        .as_ref()
                        .map_or_else(|| "resyncing".to_string(), SyncProgress::describe);
                    push(
                        Severity::Warning,
                        format!(
                            "{} → {peer}: {} out of sync ({progress})",
                            r.node,
                            format_kib(oos)
                        ),
                    );
                } else if !r.in_use && peer_is_primary {
                    // LINBIT/drbd#153: with stale bits toward the Primary, a
                    // short loss of all links can make the Primary the
                    // SyncTarget, which forces it Secondary and fails its I/O
                    push(
                        Severity::Error,
                        format!(
                            "{} holds {} out-of-sync bits toward Primary {peer}: a link loss \
                             could make the Primary a SyncTarget (LINBIT/drbd#153)",
                            r.node,
                            format_kib(oos)
                        ),
                    );
                } else {
                    push(
                        Severity::Error,
                        format!("{} → {peer}: {} out of sync", r.node, format_kib(oos)),
                    );
                }
            }
            if seen
                .ap_in_flight_kib
                .is_some_and(|ap| ap >= AP_IN_FLIGHT_IMPLAUSIBLE_KIB)
            {
                push(
                    Severity::Error,
                    format!("{} → {peer}: in-flight counter underflowed", r.node),
                );
            }
        }
    }

    issues.sort_by_key(|i| i.severity);
    issues
}

/// Suggested manual fixes for stale out-of-sync bits on established links,
/// with each command run in the satellite pod of the node it must run on.
///
/// - Bits between the Primary and a peer: from the Primary, optionally verify
///   the peer (read-only), then `invalidate-remote` it, so the peer copies
///   the whole volume from the Primary. Never the other way round: making the
///   Primary the SyncTarget is the LINBIT/drbd#153 failure.
/// - Bits between two Secondaries: reconnect just that link from one of
///   them, never touching the Primary (as home-operations/miroir#500 does).
///   A reconnect with equal UUIDs drops the bits without copying data.
///
/// Nothing is suggested while a resync is running or without a Primary.
/// `kubectl` is the command pinned to the cluster's context.
pub fn remediation(
    volume: &LinstorVolume,
    satellites: &HashMap<String, SatellitePod>,
    kubectl: &str,
) -> Vec<Remedy> {
    let Some(primary) = volume.replicas.iter().find(|r| r.in_use) else {
        return Vec::new();
    };
    let target = |peer: &str| {
        if volume.volume_number == 0 {
            format!("{}:{peer}", volume.resource)
        } else {
            format!("{}:{peer}/{}", volume.resource, volume.volume_number)
        }
    };
    let exec = |node: &str, drbdadm: String| match satellites.get(node) {
        Some(pod) => format!(
            "{kubectl} -n {} exec {} -c {SATELLITE_CONTAINER} -- drbdadm {drbdadm}",
            pod.namespace, pod.name
        ),
        None => format!(
            "{kubectl} -n piraeus-datastore exec <linstor-satellite pod on {node}> \
             -c {SATELLITE_CONTAINER} -- drbdadm {drbdadm}"
        ),
    };

    // Established links with stale bits: (holder, peer)
    let mut stale: Vec<(&str, &str)> = Vec::new();
    for r in volume.replicas.iter().filter(|r| r.drbd_layer) {
        let Some(observed) = &r.drbd else {
            continue;
        };
        for (peer, seen) in &observed.peers {
            let established = seen.cs.as_deref() == Some("Established")
                && r.peers
                    .iter()
                    .any(|p| &p.peer == peer && p.replication.as_deref() == Some("Established"));
            if established && seen.oos_kib.is_some_and(|oos| oos > 0) {
                stale.push((r.node.as_str(), peer.as_str()));
            }
        }
    }

    let mut via_primary: Vec<&str> = Vec::new();
    let mut between_secondaries: Vec<(&str, &str)> = Vec::new();
    for (holder, peer) in stale {
        if holder == primary.node {
            via_primary.push(peer);
        } else if peer == primary.node {
            via_primary.push(holder);
        } else if holder < peer {
            between_secondaries.push((holder, peer));
        } else {
            between_secondaries.push((peer, holder));
        }
    }
    via_primary.sort_unstable();
    via_primary.dedup();
    between_secondaries.sort_unstable();
    between_secondaries.dedup();

    let size = volume
        .size_kib
        .map(format_kib)
        .unwrap_or_else(|| "the volume".to_string());
    let mut remedies = Vec::new();
    for peer in via_primary {
        remedies.push(Remedy {
            summary: format!(
                "Resync {peer} from Primary {}: full copy of {size}; {peer} is Inconsistent \
                 until it finishes. Verify is optional and read-only.",
                primary.node
            ),
            commands: vec![
                exec(&primary.node, format!("verify {}", target(peer))),
                exec(&primary.node, format!("invalidate-remote {}", target(peer))),
            ],
        });
    }
    for (a, b) in between_secondaries {
        remedies.push(Remedy {
            summary: format!(
                "Reconnect Secondaries {a} ↔ {b} only (not the Primary): clears the bits \
                 without copying data"
            ),
            commands: vec![
                exec(a, format!("disconnect {}", target(b))),
                exec(a, format!("connect {}", target(b))),
            ],
        });
    }
    remedies
}

/// KiB as a human size in binary units, e.g. `11.0 GiB`, `924 KiB`
pub fn format_kib(kib: u64) -> String {
    const UNITS: [&str; 5] = ["KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut value = kib as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{kib} KiB")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::serde_json::json;

    fn resource(node: &str, in_use: bool, disk: &str, peers: &[(&str, bool, &str)]) -> Value {
        let connections: serde_json::Map<String, Value> = peers
            .iter()
            .map(|(p, connected, _)| {
                (
                    p.to_string(),
                    json!({"connected": connected, "message": if *connected {"Connected"} else {"Connecting"}}),
                )
            })
            .collect();
        let replication: serde_json::Map<String, Value> = peers
            .iter()
            .map(|(p, _, rep)| (p.to_string(), json!({"replication_state": rep})))
            .collect();
        json!({
            "name": "pvc-1", "node_name": node,
            "state": {"in_use": in_use},
            "layer_object": {"drbd": {"connections": connections}},
            "volumes": [{
                "volume_number": 0, "provider_kind": "ZFS_THIN",
                "state": {"disk_state": disk, "replication_states": replication},
                "layer_data_list": [{"type": "DRBD",
                    "data": {"drbd_volume_definition": {"minor_number": 1002}}}]
            }]
        })
    }

    use k8s_openapi::serde_json;

    fn definitions() -> Value {
        json!([{
            "name": "pvc-1",
            "props": {"Aux/csi.storage.k8s.io/pvc/namespace": "monitoring",
                      "Aux/csi.storage.k8s.io/pvc/name": "vmsingle"},
            "volume_definitions": [{"volume_number": 0, "size_kib": 11 * 1024 * 1024}]
        }])
    }

    #[test]
    fn groups_replicas_into_volumes() {
        let resources = json!([
            resource("bc2", false, "UpToDate", &[("bc1", true, "Established")]),
            resource("bc1", true, "UpToDate", &[("bc2", true, "Established")]),
        ]);
        let volumes = volumes_from(&resources, &definitions());
        assert_eq!(volumes.len(), 1);
        let v = &volumes[0];
        assert_eq!(v.pvc.as_deref(), Some("monitoring/vmsingle"));
        assert_eq!(v.size_kib, Some(11 * 1024 * 1024));
        assert_eq!(v.replicas[0].node, "bc1");
        assert!(v.replicas[0].in_use);
        assert_eq!(v.replicas[0].minor, Some(1002));
        assert!(volume_issues(v).is_empty());
    }

    /// A single local volume without DRBD (STORAGE layer only) reports no
    /// disk state; that is not a problem
    #[test]
    fn storage_only_volume_has_no_issues() {
        let resources = json!([{
            "name": "pvc-1", "node_name": "khn1",
            "layer_object": {"type": "STORAGE"},
            "volumes": [{"volume_number": 0, "provider_kind": "FILE_THIN",
                         "layer_data_list": [{"type": "STORAGE", "data": {}}]}]
        }]);
        let volume = volumes_from(&resources, &definitions()).remove(0);
        assert!(!volume.replicas[0].drbd_layer);
        assert!(volume_issues(&volume).is_empty());
    }

    #[test]
    fn linstor_states_become_issues() {
        let resources = json!([
            resource(
                "bc1",
                true,
                "UpToDate",
                &[("bc2", false, "Off"), ("bc3", true, "SyncSource")]
            ),
            resource("bc2", false, "Outdated", &[("bc1", false, "Off")]),
            resource("bc3", false, "Inconsistent", &[("bc1", true, "SyncTarget")]),
        ]);
        let issues = volume_issues(&volumes_from(&resources, &definitions())[0]);
        let texts: Vec<&str> = issues.iter().map(|i| i.text.as_str()).collect();
        assert!(texts.contains(&"bc1 ↔ bc2: Connecting"));
        assert!(texts.contains(&"bc2: disk Outdated"));
        assert!(texts.contains(&"bc3: Inconsistent (syncing)"));
        // The broken link is reported once, though both ends see it
        assert_eq!(texts.iter().filter(|t| t.contains("↔")).count(), 1);
        assert_eq!(issues[0].severity, Severity::Error);
    }

    /// The 2026-10-17 incident: LINSTOR all UpToDate, DRBD on the Primary
    /// still at the old size
    #[test]
    fn drbd_smaller_than_linstor_is_an_error() {
        let resources = json!([resource(
            "bc1",
            true,
            "UpToDate",
            &[("bc2", true, "Established")]
        )]);
        let mut volume = volumes_from(&resources, &definitions()).remove(0);
        assert!(volume_issues(&volume).is_empty());
        volume.replicas[0].drbd = Some(DrbdObserved {
            fs_size_kib: None,
            fs_used_kib: None,
            io_suspended: None,
            size_kib: Some(6 * 1024 * 1024),
            peers: HashMap::new(),
        });
        let issues = volume_issues(&volume);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].severity, Severity::Error);
        assert!(issues[0].text.starts_with("bc1: DRBD device"));
    }

    #[test]
    fn missing_replica_and_full_filesystem_are_issues() {
        let resources = json!([
            resource("bc1", true, "UpToDate", &[("bc2", true, "Established")]),
            resource("bc2", false, "UpToDate", &[("bc1", true, "Established")]),
        ]);
        let mut volume = volumes_from(&resources, &definitions()).remove(0);
        let groups = json!([{"name": "sc-1", "select_filter": {"place_count": 3}}]);
        volume.resource_group = Some("sc-1".to_string());
        apply_place_counts(std::slice::from_mut(&mut volume), &groups);
        assert_eq!(volume.place_count, Some(3));
        let issues = volume_issues(&volume);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].text, "only 2 of 3 replicas hold data");

        volume.place_count = Some(2);
        let gib = 1024 * 1024;
        volume.replicas[0].drbd = Some(DrbdObserved {
            size_kib: Some(11 * gib + 8),
            fs_size_kib: Some(10 * gib),
            fs_used_kib: Some(9 * gib),
            io_suspended: None,
            peers: HashMap::new(),
        });
        let issues = volume_issues(&volume);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].severity, Severity::Warning);
        assert!(issues[0].text.starts_with("bc1: filesystem 90% full"));
        assert_eq!(volume.usage_kib(), Some((9 * gib, 10 * gib)));
    }

    #[test]
    fn parses_resync_progress() {
        let syncing = "1001: cs:SyncSource ro:Primary/Secondary ds:UpToDate/Inconsistent C r-----\n    ns:0 nr:0 dw:0 dr:0 al:0 bm:0 lo:0 pe:[0;0] ua:0 ap:[0;0] ep:1 wo:2 oos:9228288\n\t[===>................] sync'ed: 18.4% (9012/11048)M\n\tfinish: 0:04:12 speed: 112,345 (110,000 -- 98,000) K/sec\n\t 0% sector pos: 0/23068672\n";
        let progress = parse_sync_progress(syncing).unwrap();
        assert_eq!(
            progress,
            SyncProgress {
                verify: false,
                percent: 18.4,
                finish_secs: Some(252),
                speed_kib: Some(112_345),
                stalled: false,
            }
        );
        assert_eq!(
            progress.describe(),
            "resync 18.4%, 109.7 MiB/s, ~4m 12s left"
        );

        let stuck = "\t[>....................] verified:  2.0% (100/102)K\n\tfinish: ∞ speed: 0 (0 -- 0) want: 250,000 K/sec (stalled)\n";
        let progress = parse_sync_progress(stuck).unwrap();
        assert!(progress.verify && progress.stalled);
        assert_eq!(progress.finish_secs, None);
        assert_eq!(
            progress.describe(),
            "verify 2.0%, 0 KiB/s, stalled for over 3 min"
        );

        assert_eq!(parse_sync_progress("1015: cs:Established oos:0"), None);
        assert_eq!(format_eta(3_900), "1h 05m");
        assert_eq!(format_eta(9), "9s");
    }

    #[test]
    fn suspended_io_is_an_error() {
        let resources = json!([
            resource("bc1", true, "UpToDate", &[("bc2", true, "Established")]),
            resource("bc2", false, "UpToDate", &[("bc1", true, "Established")]),
        ]);
        let mut volume = volumes_from(&resources, &definitions()).remove(0);
        volume.replicas[0].drbd = Some(DrbdObserved {
            io_suspended: Some(false),
            ..Default::default()
        });
        assert!(volume_issues(&volume).is_empty());
        volume.replicas[0].drbd.as_mut().unwrap().io_suspended = Some(true);
        let issues = volume_issues(&volume);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].severity, Severity::Error);
        assert!(issues[0].text.starts_with("bc1: I/O suspended"));
    }

    #[test]
    fn filesystem_not_grown_is_a_warning() {
        let resources = json!([resource("bc1", true, "UpToDate", &[])]);
        let mut volume = volumes_from(&resources, &definitions()).remove(0);
        let gib = 1024 * 1024;
        let with_fs = |fs: u64, device: u64| DrbdObserved {
            fs_size_kib: Some(fs),
            fs_used_kib: None,
            io_suspended: None,
            size_kib: Some(device),
            peers: HashMap::new(),
        };
        // Healthy overheads measured on real volumes
        for (fs, device) in [
            (856_678, gib),         // 836.6 MiB on 1 GiB
            (241_664, 300 * 1024),  // 236 MiB on 300 MiB
            (10_380_902, 10 * gib), // 9.9 GiB on 10 GiB
        ] {
            volume.size_kib = Some(device);
            volume.replicas[0].drbd = Some(with_fs(fs, device));
            assert!(volume_issues(&volume).is_empty(), "{fs} on {device}");
        }
        // The incident: XFS at 5.9 GiB on the grown 11 GiB device
        volume.size_kib = Some(11 * gib);
        volume.replicas[0].drbd = Some(with_fs(6_186_598, 11 * gib + 8));
        let issues = volume_issues(&volume);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].severity, Severity::Warning);
        assert!(issues[0].text.contains("not grown"));
    }

    #[test]
    fn out_of_sync_on_an_established_link_is_an_error() {
        let resources = json!([resource(
            "bc1",
            true,
            "UpToDate",
            &[("bc2", true, "Established")]
        )]);
        let mut volume = volumes_from(&resources, &definitions()).remove(0);
        let observed = |oos, ap| DrbdObserved {
            fs_size_kib: None,
            fs_used_kib: None,
            io_suspended: None,
            size_kib: Some(11 * 1024 * 1024 + 8),
            peers: HashMap::from([(
                "bc2".to_string(),
                PeerObserved {
                    cs: Some("Established".to_string()),
                    oos_kib: Some(oos),
                    ap_in_flight_kib: Some(ap),
                    sync: None,
                },
            )]),
        };
        volume.replicas[0].drbd = Some(observed(0, 0));
        assert!(volume_issues(&volume).is_empty());

        volume.replicas[0].drbd = Some(observed(4096, 9_223_372_036_854_775_800));
        let issues = volume_issues(&volume);
        assert_eq!(issues.len(), 2);
        assert!(issues.iter().all(|i| i.severity == Severity::Error));
        assert!(issues[0].text.contains("out of sync"));
        assert!(issues[1].text.contains("underflowed"));
    }

    /// The bc aftermath: Primary bc1, bits bc1↔bc2 and bc4→bc2
    fn vmsingle_with_stale_bits() -> LinstorVolume {
        let links = |peers: [&'static str; 2]| -> Vec<(&'static str, bool, &'static str)> {
            peers.iter().map(|p| (*p, true, "Established")).collect()
        };
        let resources = json!([
            resource("bc1", true, "UpToDate", &links(["bc2", "bc4"])),
            resource("bc2", false, "UpToDate", &links(["bc1", "bc4"])),
            resource("bc4", false, "UpToDate", &links(["bc1", "bc2"])),
        ]);
        let mut volume = volumes_from(&resources, &definitions()).remove(0);
        let seen = |oos: &[(&str, u64)]| DrbdObserved {
            fs_size_kib: None,
            fs_used_kib: None,
            io_suspended: None,
            size_kib: Some(11 * 1024 * 1024 + 8),
            peers: oos
                .iter()
                .map(|(peer, oos)| {
                    (
                        peer.to_string(),
                        PeerObserved {
                            cs: Some("Established".to_string()),
                            oos_kib: Some(*oos),
                            ap_in_flight_kib: Some(0),
                            sync: None,
                        },
                    )
                })
                .collect(),
        };
        volume.replicas[0].drbd = Some(seen(&[("bc2", 924), ("bc4", 0)]));
        volume.replicas[1].drbd = Some(seen(&[("bc1", 924), ("bc4", 0)]));
        volume.replicas[2].drbd = Some(seen(&[("bc1", 0), ("bc2", 48)]));
        volume
    }

    #[test]
    fn secondary_bits_toward_the_primary_name_drbd_153() {
        let issues = volume_issues(&vmsingle_with_stale_bits());
        let texts: Vec<&str> = issues.iter().map(|i| i.text.as_str()).collect();
        assert!(texts.iter().any(|t| {
            t.starts_with("bc2 holds 924 KiB out-of-sync bits toward Primary bc1")
                && t.contains("LINBIT/drbd#153")
        }));
        assert!(texts.contains(&"bc1 → bc2: 924 KiB out of sync"));
        assert!(texts.contains(&"bc4 → bc2: 48 KiB out of sync"));
    }

    #[test]
    fn suggests_resync_from_the_primary_and_secondary_reconnect() {
        let satellites = HashMap::from([(
            "bc1".to_string(),
            SatellitePod {
                namespace: "piraeus-datastore".to_string(),
                name: "linstor-satellite.bc1-kxfwm".to_string(),
            },
        )]);
        let remedies = remediation(
            &vmsingle_with_stale_bits(),
            &satellites,
            "kubectl --context bc",
        );
        assert_eq!(remedies.len(), 2);
        assert!(
            remedies[0]
                .summary
                .starts_with("Resync bc2 from Primary bc1")
        );
        let exec = "kubectl --context bc -n piraeus-datastore exec linstor-satellite.bc1-kxfwm \
             -c linstor-satellite --";
        assert_eq!(
            remedies[0].commands,
            [
                format!("{exec} drbdadm verify pvc-1:bc2"),
                format!("{exec} drbdadm invalidate-remote pvc-1:bc2"),
            ]
        );
        // bc2 ↔ bc4 is between Secondaries: reconnect from bc2, pod unknown
        assert!(
            remedies[1]
                .summary
                .starts_with("Reconnect Secondaries bc2 ↔ bc4")
        );
        assert!(remedies[1].commands[0].contains("<linstor-satellite pod on bc2>"));
        assert!(remedies[1].commands[0].ends_with("drbdadm disconnect pvc-1:bc4"));
        assert!(remedies[1].commands.iter().all(|c| !c.contains("bc1")));
    }

    #[test]
    fn no_suggestion_while_resyncing() {
        let mut volume = vmsingle_with_stale_bits();
        for r in &mut volume.replicas {
            for p in &mut r.peers {
                p.replication = Some("SyncSource".to_string());
            }
        }
        assert!(remediation(&volume, &HashMap::new(), "kubectl").is_empty());
    }

    #[test]
    fn parses_debugfs() {
        let proc_drbd = "1015: cs:Established ro:Secondary/Secondary ds:UpToDate/UpToDate C r-----\n    ns:0 nr:0 dw:0 dr:7168 al:0 bm:2 lo:1 pe:[0;0] ua:0 ap:[0;0] ep:1 wo:2 oos:12\n";
        assert_eq!(
            parse_proc_drbd(proc_drbd),
            ProcDrbd {
                cs: Some("Established".to_string()),
                oos_kib: Some(12),
                suspended: Some(false),
            }
        );
        let frozen = proc_drbd.replace("C r-----", "C s----Q");
        assert_eq!(parse_proc_drbd(&frozen).suspended, Some(true));
        assert_eq!(parse_proc_drbd("garbage"), ProcDrbd::default());

        let debug = "flags: 0x180800 : RECONNECT\n            ap_in_flight: 16 KiB (32 sectors)\n            rs_in_flight: 0 KiB (0 sectors)\n";
        assert_eq!(parse_ap_in_flight(debug), Some(16));
        assert_eq!(parse_ap_in_flight("nothing"), None);
    }

    #[test]
    fn formats_kib() {
        assert_eq!(format_kib(924), "924 KiB");
        assert_eq!(format_kib(11 * 1024 * 1024), "11.0 GiB");
        assert_eq!(format_kib(1536), "1.5 MiB");
    }

    /// Scan a real cluster: the current KUBECONFIG context and the Talos
    /// context named by `TALOS_PILOT_LIVE_CONTEXT`, e.g.
    /// `TALOS_PILOT_LIVE_CONTEXT=bc cargo test live_scan -- --ignored --nocapture`
    #[tokio::test]
    #[ignore]
    async fn live_scan() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let context =
            std::env::var("TALOS_PILOT_LIVE_CONTEXT").expect("set TALOS_PILOT_LIVE_CONTEXT");
        let kube = kube::Client::try_default().await.unwrap();
        let talos = TalosClient::from_named_context(&context).await.unwrap();
        let nodes: kube::Api<k8s_openapi::api::core::v1::Node> = kube::Api::all(kube.clone());
        let node_ips: HashMap<String, String> = nodes
            .list(&Default::default())
            .await
            .unwrap()
            .items
            .into_iter()
            .filter_map(|n| {
                let ip = n
                    .status?
                    .addresses?
                    .into_iter()
                    .find(|a| a.type_ == "InternalIP")?
                    .address;
                Some((n.metadata.name?, ip))
            })
            .collect();
        let started = std::time::Instant::now();
        let mut volumes = fetch_volumes(&kube).await.unwrap().expect("no LINSTOR");
        let fetched = started.elapsed();
        observe_drbd(&talos, &node_ips, &mut volumes).await;
        let mut zfs = crate::zfs::fetch_zfs_volumes(&kube)
            .await
            .unwrap()
            .unwrap_or_default();
        let mut mount_nodes = linstor_mount_nodes(&volumes);
        mount_nodes.extend(zfs.iter().map(|z| z.node.clone()));
        mount_nodes.sort();
        mount_nodes.dedup();
        let mounts = fetch_mounts(&talos, &node_ips, &mount_nodes).await;
        apply_mounts(&mut volumes, &mounts);
        crate::zfs::apply_zfs_mounts(&mut zfs, &mounts);
        let pvs = crate::orphans::fetch_pv_index(&kube).await.unwrap();
        for v in volumes.iter().filter(|v| v.pvc.is_some()) {
            if let Some(o) = pvs.orphan(&v.resource) {
                println!("orphan LINSTOR {} ({:?}): {o:?}", v.resource, v.pvc);
            }
        }
        for v in &zfs {
            if let Some(o) = pvs.orphan(&v.name) {
                println!("orphan ZFS {} on {}: {o:?}", v.name, v.node);
            }
        }
        let place: Vec<String> = volumes
            .iter()
            .map(|v| format!("{}/{:?}", v.diskful_replicas(), v.place_count))
            .collect();
        let mut place_summary: HashMap<&String, usize> = HashMap::new();
        for p in &place {
            *place_summary.entry(p).or_default() += 1;
        }
        println!("replicas/place_count: {place_summary:?}");
        let used: Vec<f64> = volumes
            .iter()
            .filter_map(|v| v.usage_kib())
            .map(|(u, s)| u as f64 / s as f64 * 100.0)
            .collect();
        println!(
            "LINSTOR usage known for {}, max {:.0}%",
            used.len(),
            used.iter().cloned().fold(0.0, f64::max)
        );
        let mounted: Vec<&crate::zfs::ZfsVolume> = zfs.iter().filter(|z| z.fs.is_some()).collect();
        let ratio =
            |z: &crate::zfs::ZfsVolume| z.fs.unwrap().0 as f64 / z.capacity.unwrap_or(1) as f64;
        let (rmin, rmax) = mounted
            .iter()
            .map(|z| ratio(z))
            .fold((f64::MAX, 0.0f64), |(a, b), r| (a.min(r), b.max(r)));
        println!(
            "{} ZFS volumes, {} mounted, mount/capacity ratio {rmin:.3}..{rmax:.3}, states {:?}",
            zfs.len(),
            mounted.len(),
            zfs.iter()
                .map(|z| z.state.as_str())
                .collect::<std::collections::BTreeSet<_>>()
        );
        for z in &zfs {
            for issue in crate::zfs::zfs_volume_issues(z) {
                println!(
                    "ZFS {:?} {} {:?}: {}",
                    issue.severity, z.name, z.pvc, issue.text
                );
            }
        }
        let replicas: usize = volumes.iter().map(|v| v.replicas.len()).sum();
        let observed = volumes
            .iter()
            .flat_map(|v| &v.replicas)
            .filter(|r| r.drbd.as_ref().is_some_and(|d| d.size_kib.is_some()))
            .count();
        let peer_reads = volumes
            .iter()
            .flat_map(|v| &v.replicas)
            .filter_map(|r| r.drbd.as_ref())
            .flat_map(|d| d.peers.values())
            .filter(|p| p.cs.is_some() && p.oos_kib.is_some() && p.ap_in_flight_kib.is_some())
            .count();
        println!(
            "{} volumes, {} replicas, size read for {}, full peer state for {} peer links; LINSTOR {:?}, total {:?}",
            volumes.len(),
            replicas,
            observed,
            peer_reads,
            fetched,
            started.elapsed()
        );
        let satellites = fetch_satellites(&kube).await;
        for v in &volumes {
            for issue in volume_issues(v) {
                println!(
                    "{:?} {} {:?}: {}",
                    issue.severity, v.resource, v.pvc, issue.text
                );
            }
            for remedy in remediation(v, &satellites, "kubectl") {
                println!("  fix: {}", remedy.summary);
                for command in remedy.commands {
                    println!("       {command}");
                }
            }
        }
    }
}

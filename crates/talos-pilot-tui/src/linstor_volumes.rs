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
//!
//! debugfs is marked "format will change without notice"; anything that
//! can't be read or parsed is reported as unknown, never as a problem.

use crate::linstor::{controller_base, get_json};
use futures::StreamExt;
use k8s_openapi::serde_json::Value;
use std::collections::HashMap;
use talos_rs::TalosClient;

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
    /// One per node the resource is placed on, sorted by node
    pub replicas: Vec<Replica>,
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
    let (resources, definitions) = tokio::join!(
        get_json(client, &resources_path),
        get_json(client, &definitions_path),
    );
    Ok(Some(volumes_from(&resources?, &definitions?)))
}

/// Group LINSTOR's resource view (one entry per resource and node) into
/// volumes, with sizes and PVC names from the resource definitions
pub fn volumes_from(resources: &Value, definitions: &Value) -> Vec<LinstorVolume> {
    let str_at = |v: &Value, key: &str| v.get(key).and_then(Value::as_str).map(str::to_string);

    // resource -> (pvc, volume number -> size)
    let mut defs: HashMap<String, (Option<String>, HashMap<u64, u64>)> = HashMap::new();
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
        defs.insert(name, (pvc, sizes));
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

            let (pvc, size_kib) = match defs.get(&name) {
                Some((pvc, sizes)) => (pvc.clone(), sizes.get(&number).copied()),
                None => (None, None),
            };
            let volume = volumes
                .entry((name.clone(), number))
                .or_insert_with(|| LinstorVolume {
                    resource: name.clone(),
                    volume_number: number as u32,
                    pvc,
                    size_kib,
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

    let results: Vec<_> = futures::stream::iter(probes)
        .map(|(at, probe, client, path)| async move {
            let content = tokio::time::timeout(READ_TIMEOUT, client.read_file(&path))
                .await
                .ok()
                .and_then(|r| r.ok());
            (at, probe, content)
        })
        .buffer_unordered(OBSERVE_CONCURRENCY)
        .collect()
        .await;

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
                let (cs, oos_kib) = parse_proc_drbd(&content);
                let entry = observed.peers.entry(peer).or_default();
                entry.cs = cs;
                entry.oos_kib = oos_kib;
            }
            Probe::PeerConnection(peer) => {
                observed.peers.entry(peer).or_default().ap_in_flight_kib =
                    parse_ap_in_flight(&content);
            }
        }
    }
}

/// `cs:` and `oos:` from a debugfs `proc_drbd` file, e.g.
/// `1015: cs:Established ro:Secondary/Secondary ds:UpToDate/UpToDate C r-----`
/// followed by `ns:0 nr:0 … oos:0`
fn parse_proc_drbd(content: &str) -> (Option<String>, Option<u64>) {
    let field = |key: &str| {
        content
            .split_whitespace()
            .find_map(|t| t.strip_prefix(key))
            .map(str::to_string)
    };
    (field("cs:"), field("oos:").and_then(|v| v.parse().ok()))
}

/// `ap_in_flight` in KiB from a debugfs connection `debug` file
/// (`ap_in_flight: 0 KiB (0 sectors)`)
fn parse_ap_in_flight(content: &str) -> Option<u64> {
    content.lines().find_map(|line| {
        let rest = line.trim().strip_prefix("ap_in_flight:")?;
        rest.split_whitespace().next()?.parse().ok()
    })
}

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
                    push(
                        Severity::Warning,
                        format!(
                            "{} → {peer}: {} out of sync (resyncing)",
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
pub fn remediation(
    volume: &LinstorVolume,
    satellites: &HashMap<String, SatellitePod>,
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
            "kubectl -n {} exec {} -c {SATELLITE_CONTAINER} -- drbdadm {drbdadm}",
            pod.namespace, pod.name
        ),
        None => format!(
            "kubectl -n piraeus-datastore exec <linstor-satellite pod on {node}> \
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
            size_kib: Some(6 * 1024 * 1024),
            peers: HashMap::new(),
        });
        let issues = volume_issues(&volume);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].severity, Severity::Error);
        assert!(issues[0].text.starts_with("bc1: DRBD device"));
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
            size_kib: Some(11 * 1024 * 1024 + 8),
            peers: HashMap::from([(
                "bc2".to_string(),
                PeerObserved {
                    cs: Some("Established".to_string()),
                    oos_kib: Some(oos),
                    ap_in_flight_kib: Some(ap),
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
        let remedies = remediation(&vmsingle_with_stale_bits(), &satellites);
        assert_eq!(remedies.len(), 2);
        assert!(
            remedies[0]
                .summary
                .starts_with("Resync bc2 from Primary bc1")
        );
        let exec =
            "kubectl -n piraeus-datastore exec linstor-satellite.bc1-kxfwm -c linstor-satellite --";
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
        assert!(remediation(&volume, &HashMap::new()).is_empty());
    }

    #[test]
    fn parses_debugfs() {
        let proc_drbd = "1015: cs:Established ro:Secondary/Secondary ds:UpToDate/UpToDate C r-----\n    ns:0 nr:0 dw:0 dr:7168 al:0 bm:2 lo:1 pe:[0;0] ua:0 ap:[0;0] ep:1 wo:2 oos:12\n";
        assert_eq!(
            parse_proc_drbd(proc_drbd),
            (Some("Established".to_string()), Some(12))
        );
        assert_eq!(parse_proc_drbd("garbage"), (None, None));

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
            for remedy in remediation(v, &satellites) {
                println!("  fix: {}", remedy.summary);
                for command in remedy.commands {
                    println!("       {command}");
                }
            }
        }
    }
}

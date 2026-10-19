//! Volumes nothing uses any more
//!
//! A storage volume is orphaned when no PersistentVolume points at it (its
//! PVC was deleted while it was still being provisioned, or the PV was
//! deleted by hand), or when its PV is `Released`: the PVC is gone and the
//! reclaim policy kept the data. Either way it holds pool space until
//! someone deletes it.
//!
//! The CSI provisioner names a volume `pvc-<PVC uid>` before the PV exists,
//! so a volume without a PV whose PVC still exists is being provisioned, not
//! orphaned.

use crate::linstor_volumes::{Issue, Severity};
use k8s_openapi::api::core::v1::{PersistentVolume, PersistentVolumeClaim};
use kube::Client;
use kube::api::{Api, ListParams};
use std::collections::{HashMap, HashSet};

/// A PersistentVolume, by what the provisioner calls its volume
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PvRef {
    pub name: String,
    /// `Bound`, `Released`, `Available`, `Failed`
    pub phase: String,
    /// `namespace/name` of the claim it was bound to
    pub claim: Option<String>,
}

/// The cluster's PVs by CSI volume handle, and the uids of its PVCs
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PvIndex {
    pub by_handle: HashMap<String, PvRef>,
    pub pvc_uids: HashSet<String>,
}

/// Why a volume is orphaned
#[derive(Debug, Clone, PartialEq)]
pub enum Orphan {
    /// No PV points at the volume, and no PVC is waiting for it
    NoPv,
    /// Its PV outlived the PVC
    Released { pv: String, claim: Option<String> },
}

impl Orphan {
    pub fn issue(&self) -> Issue {
        let text = match self {
            Orphan::NoPv => "orphaned: no PersistentVolume uses it".to_string(),
            Orphan::Released { pv, claim } => match claim {
                Some(claim) => format!("orphaned: PV {pv} Released, PVC {claim} is gone"),
                None => format!("orphaned: PV {pv} Released"),
            },
        };
        Issue {
            severity: Severity::Warning,
            text,
        }
    }

    /// The PV to delete before the volume, if any
    pub fn pv(&self) -> Option<&str> {
        match self {
            Orphan::NoPv => None,
            Orphan::Released { pv, .. } => Some(pv),
        }
    }
}

/// All PVs and PVCs of the cluster. `Err` when either can't be listed, as
/// then nothing can be said about orphans.
pub async fn fetch_pv_index(client: &Client) -> Result<PvIndex, kube::Error> {
    let pvs: Api<PersistentVolume> = Api::all(client.clone());
    let pvcs: Api<PersistentVolumeClaim> = Api::all(client.clone());
    let params = ListParams::default();
    let (pvs, pvcs) = tokio::join!(
        crate::kube_list::list(&pvs, &params),
        crate::kube_list::list(&pvcs, &params),
    );
    Ok(pv_index_from(pvs?.items, pvcs?.items))
}

fn pv_index_from(pvs: Vec<PersistentVolume>, pvcs: Vec<PersistentVolumeClaim>) -> PvIndex {
    let by_handle = pvs
        .into_iter()
        .filter_map(|pv| {
            let spec = pv.spec?;
            let handle = spec.csi?.volume_handle;
            let claim = spec
                .claim_ref
                .and_then(|c| Some(format!("{}/{}", c.namespace?, c.name?)));
            let pv = PvRef {
                name: pv.metadata.name?,
                phase: pv.status.and_then(|s| s.phase).unwrap_or_default(),
                claim,
            };
            Some((handle, pv))
        })
        .collect();
    let pvc_uids = pvcs
        .into_iter()
        .filter_map(|pvc| pvc.metadata.uid)
        .collect();
    PvIndex {
        by_handle,
        pvc_uids,
    }
}

impl PvIndex {
    /// `namespace/name` of the claim of the PV using `handle`
    pub fn claim(&self, handle: &str) -> Option<String> {
        self.by_handle.get(handle)?.claim.clone()
    }

    /// Whether the CSI volume `handle` is orphaned
    pub fn orphan(&self, handle: &str) -> Option<Orphan> {
        match self.by_handle.get(handle) {
            Some(pv) if pv.phase == "Released" => Some(Orphan::Released {
                pv: pv.name.clone(),
                claim: pv.claim.clone(),
            }),
            Some(_) => None,
            None => {
                let provisioning = handle
                    .strip_prefix("pvc-")
                    .is_some_and(|uid| self.pvc_uids.contains(uid));
                (!provisioning).then_some(Orphan::NoPv)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::serde_json::{self, json};

    fn index() -> PvIndex {
        let pv = |name: &str, phase: &str| {
            serde_json::from_value(json!({
                "metadata": {"name": name},
                "spec": {
                    "csi": {"driver": "zfs.csi.openebs.io", "volumeHandle": name},
                    "claimRef": {"namespace": "db", "name": format!("data-{name}")}
                },
                "status": {"phase": phase}
            }))
            .unwrap()
        };
        let local: PersistentVolume = serde_json::from_value(json!({
            "metadata": {"name": "local-1"},
            "spec": {"local": {"path": "/var/mnt/x"}}
        }))
        .unwrap();
        let pvc: PersistentVolumeClaim = serde_json::from_value(json!({
            "metadata": {"name": "new", "namespace": "db", "uid": "1234"}
        }))
        .unwrap();
        pv_index_from(
            vec![pv("pvc-bound", "Bound"), pv("pvc-old", "Released"), local],
            vec![pvc],
        )
    }

    #[test]
    fn orphans_are_released_or_unclaimed_volumes() {
        let index = index();
        assert_eq!(index.by_handle.len(), 2);
        assert_eq!(index.orphan("pvc-bound"), None);
        assert_eq!(
            index.claim("pvc-bound").as_deref(),
            Some("db/data-pvc-bound")
        );
        assert_eq!(
            index.orphan("pvc-old"),
            Some(Orphan::Released {
                pv: "pvc-old".to_string(),
                claim: Some("db/data-pvc-old".to_string())
            })
        );
        // Being provisioned: the PVC exists, the PV doesn't yet
        assert_eq!(index.orphan("pvc-1234"), None);
        assert_eq!(index.orphan("pvc-gone"), Some(Orphan::NoPv));
        assert_eq!(
            Orphan::NoPv.issue().text,
            "orphaned: no PersistentVolume uses it"
        );
    }
}

//! Group views: diagnostics, storage or network of every node under a
//! cluster / control plane / workers header, in one table across nodes.
//!
//! - Diagnostics: one row per node with pass/warn/fail counts per category,
//!   and the problems below, each listing the nodes it was found on
//! - Storage: every node's disks, or (`v`) its Talos volumes; virtual block
//!   devices (loop, dm, drbd, ...) and directory / overlay volumes are
//!   hidden unless `a` shows all
//! - Network: every node's interfaces with RX/TX rates; loopback, pod veths
//!   and interfaces that never carried traffic are hidden unless `a` shows
//!   all
//!
//! Enter opens the single-node view of the selected row's node.

use crate::action::Action;
use crate::components::Component;
use crate::components::diagnostics::{CheckStatus, DiagnosticCheck, DiagnosticsComponent};
use crate::ui_ext::CheckStatusExt;
use color_eyre::Result;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState},
};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use talos_pilot_core::AsyncState;
use talos_rs::{
    DiskInfo, NetDevStats, TalosClient, VolumeStatus, get_disks_for_node,
    get_volume_status_for_node,
};

/// Upper bound for one node's diagnostics run
const DIAGNOSTICS_TIMEOUT: Duration = Duration::from_secs(60);
/// Upper bound for each per-node talosctl / API query
const NODE_QUERY_TIMEOUT: Duration = Duration::from_secs(20);
/// Rates need two samples: the first load takes a second one this long after
const FIRST_SAMPLE_GAP: Duration = Duration::from_secs(1);

/// What the group view shows
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupKind {
    Diagnostics,
    Storage,
    Network,
}

impl GroupKind {
    fn label(self) -> &'static str {
        match self {
            GroupKind::Diagnostics => "Diagnostics",
            GroupKind::Storage => "Storage",
            GroupKind::Network => "Network",
        }
    }

    fn refresh_interval(self) -> Duration {
        match self {
            GroupKind::Diagnostics | GroupKind::Storage => Duration::from_secs(60),
            GroupKind::Network => Duration::from_secs(5),
        }
    }
}

/// A node of the group
#[derive(Debug, Clone, PartialEq)]
pub struct GroupNode {
    pub hostname: String,
    pub address: String,
    /// "controlplane" or "worker"
    pub role: String,
}

/// One node's diagnostics: checks by category, or why they're missing
type NodeChecks = Result<Vec<(&'static str, Vec<DiagnosticCheck>)>, String>;

/// One interface of one node
#[derive(Debug, Clone, PartialEq)]
struct IfaceRow {
    node: usize,
    name: String,
    /// Bytes per second since the previous sample, `None` on the first
    rx_rate: Option<u64>,
    tx_rate: Option<u64>,
    rx_bytes: u64,
    tx_bytes: u64,
    errors: u64,
    dropped: u64,
}

#[derive(Debug, Clone, Default)]
pub struct GroupData {
    /// Per node, in node order
    diagnostics: Vec<NodeChecks>,
    disks: Vec<(usize, DiskInfo)>,
    volumes: Vec<(usize, VolumeStatus)>,
    ifaces: Vec<IfaceRow>,
    /// Interface counters of the last sample: (node, iface) -> (rx, tx, when)
    samples: HashMap<(usize, String), (u64, u64, Instant)>,
    /// Per node: why its data is missing this time
    node_errors: Vec<Option<String>>,
}

#[derive(Clone)]
pub struct GroupComponent {
    kind: GroupKind,
    title: String,
    nodes: Vec<GroupNode>,
    client: Option<TalosClient>,
    /// talosctl context and talosconfig, for storage queries and diagnostics
    context: Option<String>,
    config_path: Option<String>,
    /// A control plane address, for workers' kubeconfig in diagnostics
    cp_endpoint: Option<String>,
    state: AsyncState<GroupData>,
    table_state: TableState,
    /// Storage: volumes instead of disks
    show_volumes: bool,
    /// Storage and network: the rows hidden by default too
    show_all: bool,
}

/// Interfaces hidden by default: loopback, the per-pod ends of the CNI (one
/// per pod, so they would bury the node interfaces) and kernel stubs that
/// never carried traffic (dummy0, sit0, tunl0, ...)
fn is_hidden_iface(row: &IfaceRow) -> bool {
    row.name == "lo"
        || ["veth", "lxc", "cali"]
            .iter()
            .any(|prefix| row.name.starts_with(prefix))
        || row.rx_bytes + row.tx_bytes == 0
}

/// Block devices hidden by default: virtual ones layered on the disks
/// (loop images, device mapper, software RAID, DRBD replicas, zvols, ...),
/// which would also count the same capacity twice
fn is_virtual_disk(disk: &DiskInfo) -> bool {
    let name = disk.dev_path.trim_start_matches("/dev/");
    ["loop", "dm-", "md", "drbd", "zd", "nbd", "ram", "zram"]
        .iter()
        .any(|prefix| name.starts_with(prefix))
}

/// Talos volumes hidden by default: directories, overlays and symlinks in
/// the root filesystem, which are not storage of their own
fn is_pseudo_volume(volume: &VolumeStatus) -> bool {
    matches!(volume.size.as_str(), "directory" | "overlay" | "symlink")
}

/// Per-second rate between two counter samples; `None` when a counter went
/// backwards (interface reset) or no time passed
fn rate(previous: u64, current: u64, elapsed: Duration) -> Option<u64> {
    let secs = elapsed.as_secs_f64();
    (current >= previous && secs > 0.0).then(|| ((current - previous) as f64 / secs) as u64)
}

/// A bounded talosctl query's result, its error as text
fn flatten<T>(
    result: std::result::Result<
        std::result::Result<T, talos_rs::TalosError>,
        tokio::time::error::Elapsed,
    >,
) -> std::result::Result<T, String> {
    match result {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err(format!("timed out after {}s", NODE_QUERY_TIMEOUT.as_secs())),
    }
}

/// Pass / warn / fail / unknown counts of some checks
fn status_counts(checks: &[DiagnosticCheck]) -> [usize; 4] {
    let mut counts = [0; 4];
    for check in checks {
        let slot = match check.status {
            CheckStatus::Pass => 0,
            CheckStatus::Warn => 1,
            CheckStatus::Fail => 2,
            CheckStatus::Unknown | CheckStatus::Checking => 3,
        };
        counts[slot] += 1;
    }
    counts
}

/// A failing or warning check and the nodes it was found on
#[derive(Debug, Clone, PartialEq)]
struct Problem {
    status: CheckStatus,
    category: &'static str,
    name: String,
    message: String,
    nodes: Vec<String>,
}

/// Failures and warnings across nodes, the same finding on several nodes
/// listed once; failures first
fn problems(nodes: &[GroupNode], diagnostics: &[NodeChecks]) -> Vec<Problem> {
    let mut problems: Vec<Problem> = Vec::new();
    for (node, checks) in nodes.iter().zip(diagnostics) {
        let Ok(categories) = checks else { continue };
        for (category, checks) in categories {
            for check in checks {
                if !matches!(check.status, CheckStatus::Warn | CheckStatus::Fail) {
                    continue;
                }
                let same = problems.iter_mut().find(|p| {
                    p.category == *category
                        && p.name == check.name
                        && p.message == check.message
                        && p.status == check.status
                });
                match same {
                    Some(p) => p.nodes.push(node.hostname.clone()),
                    None => problems.push(Problem {
                        status: check.status.clone(),
                        category,
                        name: check.name.clone(),
                        message: check.message.clone(),
                        nodes: vec![node.hostname.clone()],
                    }),
                }
            }
        }
    }
    problems.sort_by_key(|p| !matches!(p.status, CheckStatus::Fail));
    problems
}

impl GroupComponent {
    pub fn new(
        kind: GroupKind,
        title: String,
        nodes: Vec<GroupNode>,
        context: Option<String>,
        config_path: Option<String>,
        cp_endpoint: Option<String>,
    ) -> Self {
        let mut table_state = TableState::default();
        table_state.select(Some(0));
        let mut state = AsyncState::new();
        state.start_loading();
        Self {
            kind,
            title,
            nodes,
            client: None,
            context,
            config_path,
            cp_endpoint,
            state,
            table_state,
            show_volumes: false,
            show_all: false,
        }
    }

    pub fn set_client(&mut self, client: TalosClient) {
        self.client = Some(client);
    }

    pub fn set_error(&mut self, error: String) {
        self.state.set_error(error);
    }

    pub async fn refresh(&mut self) -> Result<()> {
        let Some(client) = self.client.clone() else {
            self.state
                .set_error_with_retry("Not connected to the cluster — reconnecting");
            return Ok(());
        };
        self.state.start_loading();
        let mut data = self.state.take_data().unwrap_or_default();
        data.node_errors = vec![None; self.nodes.len()];
        match self.kind {
            GroupKind::Diagnostics => self.load_diagnostics(&client, &mut data).await,
            GroupKind::Storage => self.load_storage(&mut data).await,
            GroupKind::Network => {
                let first = data.samples.is_empty();
                self.load_network(&client, &mut data).await;
                // Without a previous sample there are no rates yet
                if first && !data.samples.is_empty() {
                    tokio::time::sleep(FIRST_SAMPLE_GAP).await;
                    self.load_network(&client, &mut data).await;
                }
            }
        }
        let failed = data.node_errors.iter().filter(|e| e.is_some()).count();
        if failed > 0 {
            tracing::warn!(
                "{} {}: {} of {} node(s) failed",
                self.title,
                self.kind.label(),
                failed,
                self.nodes.len()
            );
        }
        let all_failed = failed == self.nodes.len();
        let errors: Vec<String> = self
            .nodes
            .iter()
            .zip(&data.node_errors)
            .filter_map(|(n, e)| Some(format!("{}: {}", n.hostname, e.as_ref()?)))
            .collect();
        self.state.set_data(data);
        if all_failed {
            self.state.set_error(errors.join("; "));
        }
        Ok(())
    }

    /// Run the single-node diagnostics on every node at once
    async fn load_diagnostics(&self, client: &TalosClient, data: &mut GroupData) {
        let runs = self.nodes.iter().map(|node| {
            let mut diagnostics = DiagnosticsComponent::new(
                node.hostname.clone(),
                node.address.clone(),
                node.role.clone(),
                self.config_path.clone(),
            );
            if node.role == "worker" {
                diagnostics.set_controlplane_endpoint(self.cp_endpoint.clone());
            }
            diagnostics.set_talos_context(self.context.clone());
            diagnostics.set_client(client.with_node(&node.address));
            async move {
                match tokio::time::timeout(DIAGNOSTICS_TIMEOUT, diagnostics.refresh()).await {
                    Ok(Ok(())) => diagnostics.checks_by_category(),
                    Ok(Err(e)) => Err(e.to_string()),
                    Err(_) => Err(format!(
                        "no result within {}s",
                        DIAGNOSTICS_TIMEOUT.as_secs()
                    )),
                }
            }
        });
        data.diagnostics = futures::future::join_all(runs).await;
        for (i, result) in data.diagnostics.iter().enumerate() {
            if let Err(e) = result {
                data.node_errors[i] = Some(e.clone());
            }
        }
    }

    /// Disks and volumes of every node (talosctl, all nodes at once). A node
    /// that fails keeps what it showed before.
    async fn load_storage(&self, data: &mut GroupData) {
        let Some(context) = self.context.as_deref() else {
            data.node_errors = vec![Some("no talosctl context".to_string()); self.nodes.len()];
            return;
        };
        let config = self.config_path.as_deref();
        let fetches = self.nodes.iter().map(|node| async move {
            let (disks, volumes) = tokio::join!(
                tokio::time::timeout(
                    NODE_QUERY_TIMEOUT,
                    get_disks_for_node(context, &node.address, config)
                ),
                tokio::time::timeout(
                    NODE_QUERY_TIMEOUT,
                    get_volume_status_for_node(context, &node.address, config)
                ),
            );
            (flatten(disks), flatten(volumes))
        });
        let results = futures::future::join_all(fetches).await;
        for (i, (disks, volumes)) in results.into_iter().enumerate() {
            match disks {
                Ok(disks) => {
                    data.disks.retain(|(n, _)| *n != i);
                    data.disks.extend(disks.into_iter().map(|d| (i, d)));
                }
                Err(e) => data.node_errors[i] = Some(e),
            }
            match volumes {
                Ok(volumes) => {
                    data.volumes.retain(|(n, _)| *n != i);
                    data.volumes.extend(volumes.into_iter().map(|v| (i, v)));
                }
                Err(e) => data.node_errors[i] = Some(e),
            }
        }
        data.disks
            .sort_by(|a, b| (a.0, &a.1.dev_path).cmp(&(b.0, &b.1.dev_path)));
        data.volumes
            .sort_by(|a, b| (a.0, &a.1.id).cmp(&(b.0, &b.1.id)));
    }

    /// Interface counters of every node (Talos API, all nodes at once), with
    /// rates against the previous sample
    async fn load_network(&self, client: &TalosClient, data: &mut GroupData) {
        let fetches = self.nodes.iter().map(|node| {
            let node_client = client.with_node(&node.address);
            async move {
                tokio::time::timeout(NODE_QUERY_TIMEOUT, node_client.network_device_stats()).await
            }
        });
        let results = futures::future::join_all(fetches).await;
        let now = Instant::now();
        for (i, result) in results.into_iter().enumerate() {
            let devices: Vec<NetDevStats> = match result {
                Ok(Ok(stats)) => stats.into_iter().flat_map(|s| s.devices).collect(),
                Ok(Err(e)) => {
                    data.node_errors[i] = Some(e.to_string());
                    continue;
                }
                Err(_) => {
                    data.node_errors[i] =
                        Some(format!("timed out after {}s", NODE_QUERY_TIMEOUT.as_secs()));
                    continue;
                }
            };
            data.ifaces.retain(|r| r.node != i);
            for dev in devices {
                let key = (i, dev.name.clone());
                let (rx_rate, tx_rate) = match data.samples.get(&key) {
                    Some(&(rx, tx, at)) => (
                        rate(rx, dev.rx_bytes, now - at),
                        rate(tx, dev.tx_bytes, now - at),
                    ),
                    None => (None, None),
                };
                data.samples.insert(key, (dev.rx_bytes, dev.tx_bytes, now));
                data.ifaces.push(IfaceRow {
                    node: i,
                    rx_rate,
                    tx_rate,
                    rx_bytes: dev.rx_bytes,
                    tx_bytes: dev.tx_bytes,
                    errors: dev.total_errors(),
                    dropped: dev.total_dropped(),
                    name: dev.name,
                });
            }
        }
        // By node, the busiest interface first
        data.ifaces.sort_by(|a, b| {
            let traffic = |r: &IfaceRow| r.rx_rate.unwrap_or(0) + r.tx_rate.unwrap_or(0);
            a.node
                .cmp(&b.node)
                .then(traffic(b).cmp(&traffic(a)))
                .then(a.name.cmp(&b.name))
        });
    }

    /// Interfaces shown (see [`is_hidden_iface`])
    fn shown_ifaces<'a>(&self, data: &'a GroupData) -> Vec<&'a IfaceRow> {
        data.ifaces
            .iter()
            .filter(|r| self.show_all || !is_hidden_iface(r))
            .collect()
    }

    /// Disks shown (see [`is_virtual_disk`])
    fn shown_disks<'a>(&self, data: &'a GroupData) -> Vec<&'a (usize, DiskInfo)> {
        data.disks
            .iter()
            .filter(|(_, d)| self.show_all || !is_virtual_disk(d))
            .collect()
    }

    /// Volumes shown (see [`is_pseudo_volume`])
    fn shown_volumes<'a>(&self, data: &'a GroupData) -> Vec<&'a (usize, VolumeStatus)> {
        data.volumes
            .iter()
            .filter(|(_, v)| self.show_all || !is_pseudo_volume(v))
            .collect()
    }

    /// Width of the NODE column: the longest hostname
    fn node_width(&self) -> Constraint {
        let longest = self
            .nodes
            .iter()
            .map(|n| n.hostname.len())
            .max()
            .unwrap_or(4);
        Constraint::Length(longest.max(4) as u16 + 1)
    }

    /// Node index of each table row
    fn row_nodes(&self) -> Vec<usize> {
        let Some(data) = self.state.data() else {
            return Vec::new();
        };
        match self.kind {
            GroupKind::Diagnostics => (0..self.nodes.len()).collect(),
            GroupKind::Storage if self.show_volumes => {
                self.shown_volumes(data).iter().map(|(n, _)| *n).collect()
            }
            GroupKind::Storage => self.shown_disks(data).iter().map(|(n, _)| *n).collect(),
            GroupKind::Network => self.shown_ifaces(data).iter().map(|r| r.node).collect(),
        }
    }

    fn select(&mut self, delta: isize) {
        let len = self.row_nodes().len();
        if len == 0 {
            return;
        }
        let current = self.table_state.selected().unwrap_or(0) as isize;
        let next = (current + delta).clamp(0, len as isize - 1);
        self.table_state.select(Some(next as usize));
    }

    /// Single-node view of the selected row's node
    fn open_selected(&self) -> Option<Action> {
        let rows = self.row_nodes();
        let node = self.nodes.get(*rows.get(self.table_state.selected()?)?)?;
        let (name, address) = (node.hostname.clone(), node.address.clone());
        Some(match self.kind {
            GroupKind::Diagnostics => {
                let cp = (node.role == "worker")
                    .then(|| self.cp_endpoint.clone())
                    .flatten();
                Action::ShowDiagnostics(name, address, node.role.clone(), cp)
            }
            GroupKind::Storage => Action::ShowStorage(name, address),
            GroupKind::Network => Action::ShowNetwork(name, address),
        })
    }

    fn header_line(&self) -> Line<'static> {
        let mut spans = vec![
            Span::styled(
                format!("{}: ", self.kind.label()),
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::styled(self.title.clone(), Style::default().fg(Color::Cyan)),
            Span::raw(format!(" · {} nodes", self.nodes.len())),
        ];
        if self.kind == GroupKind::Storage
            && let Some(data) = self.state.data()
        {
            let disks = self.shown_disks(data);
            let total: u64 = disks.iter().map(|(_, d)| d.size).sum();
            spans.push(Span::raw(format!(
                " · {} disks, {}",
                disks.len(),
                talos_pilot_core::format_bytes(total)
            )));
        }
        if self.state.is_loading() {
            spans.push(Span::styled(
                "  refreshing…",
                Style::default().fg(Color::DarkGray),
            ));
        }
        // A failed refresh over the data still shown
        if let Some(error) = self.state.error() {
            spans.push(Span::styled(
                format!("  ⚠ {error}"),
                Style::default().fg(Color::Red),
            ));
        }
        Line::from(spans)
    }

    /// Nodes whose data is missing this time
    fn node_error_lines(&self, data: &GroupData) -> Vec<Line<'static>> {
        if self.state.error().is_some() {
            return Vec::new();
        }
        self.nodes
            .iter()
            .zip(&data.node_errors)
            .filter_map(|(n, e)| {
                Some(Line::styled(
                    format!("⚠ {}: {} (showing last known)", n.hostname, e.as_ref()?),
                    Style::default().fg(Color::Yellow),
                ))
            })
            .collect()
    }

    fn footer_line(&self) -> Line<'static> {
        let all = if self.show_all {
            "  [a] hide virtual"
        } else {
            "  [a] show all"
        };
        let toggle = match self.kind {
            GroupKind::Storage if self.show_volumes => format!("  [v] disks{all}"),
            GroupKind::Storage => format!("  [v] volumes{all}"),
            GroupKind::Network => all.to_string(),
            GroupKind::Diagnostics => String::new(),
        };
        Line::styled(
            format!("[j/k] select  [Enter] open node  [r] refresh{toggle}  [q] back"),
            Style::default().fg(Color::DarkGray),
        )
    }

    fn table<'a>(&self, data: &'a GroupData) -> (Row<'a>, Vec<Row<'a>>, Vec<Constraint>) {
        let bold = |s: &'a str| Cell::from(s).style(Style::default().add_modifier(Modifier::BOLD));
        let node_cell = |i: usize| {
            Cell::from(
                self.nodes
                    .get(i)
                    .map(|n| n.hostname.clone())
                    .unwrap_or_default(),
            )
            .style(Style::default().fg(Color::Cyan))
        };
        match self.kind {
            GroupKind::Diagnostics => {
                let categories: Vec<&'static str> = data
                    .diagnostics
                    .iter()
                    .find_map(|d| d.as_ref().ok())
                    .map(|c| c.iter().map(|(name, _)| *name).collect())
                    .unwrap_or_default();
                let mut header = vec![bold("NODE"), bold("ROLE")];
                header.extend(categories.iter().map(|c| bold(c)));
                let rows = self
                    .nodes
                    .iter()
                    .enumerate()
                    .map(|(i, node)| {
                        let mut cells = vec![node_cell(i), Cell::from(node.role.clone())];
                        match data.diagnostics.get(i) {
                            Some(Ok(by_category)) => cells.extend(
                                by_category
                                    .iter()
                                    .map(|(_, checks)| count_cell(status_counts(checks))),
                            ),
                            Some(Err(e)) => cells.push(
                                Cell::from(format!("✗ {e}")).style(Style::default().fg(Color::Red)),
                            ),
                            None => cells.push(Cell::from("…")),
                        }
                        Row::new(cells)
                    })
                    .collect();
                let mut widths = vec![self.node_width(), Constraint::Length(13)];
                widths.extend(categories.iter().map(|_| Constraint::Length(23)));
                (Row::new(header), rows, widths)
            }
            GroupKind::Storage if self.show_volumes => {
                let header = Row::new(vec![
                    bold("NODE"),
                    bold("VOLUME"),
                    bold("SIZE"),
                    bold("PHASE"),
                    bold("FS"),
                    bold("ENCRYPTION"),
                    bold("MOUNT"),
                ]);
                let rows = self
                    .shown_volumes(data)
                    .into_iter()
                    .map(|(i, vol)| {
                        let phase_color = match vol.phase.as_str() {
                            "ready" => Color::Green,
                            "waiting" => Color::Yellow,
                            _ => Color::Red,
                        };
                        Row::new(vec![
                            node_cell(*i),
                            Cell::from(vol.id.clone()),
                            Cell::from(vol.size.clone()),
                            Cell::from(vol.phase.clone()).style(Style::default().fg(phase_color)),
                            Cell::from(vol.filesystem.clone().unwrap_or_default()),
                            Cell::from(
                                vol.encryption_provider
                                    .clone()
                                    .unwrap_or_else(|| "none".to_string()),
                            ),
                            Cell::from(vol.mount_location.clone().unwrap_or_default()),
                        ])
                    })
                    .collect();
                let widths = vec![
                    self.node_width(),
                    Constraint::Length(16),
                    Constraint::Length(10),
                    Constraint::Length(9),
                    Constraint::Length(10),
                    Constraint::Length(11),
                    Constraint::Length(30),
                ];
                (header, rows, widths)
            }
            GroupKind::Storage => {
                let header = Row::new(vec![
                    bold("NODE"),
                    bold("DEVICE"),
                    bold("SIZE"),
                    bold("TYPE"),
                    bold("TRANSPORT"),
                    bold("MODEL"),
                    bold("SERIAL"),
                ]);
                let rows = self
                    .shown_disks(data)
                    .into_iter()
                    .map(|(i, disk)| {
                        let (kind, color) = if disk.cdrom {
                            ("CD-ROM", Color::Magenta)
                        } else if disk.rotational {
                            ("HDD", Color::Yellow)
                        } else {
                            ("SSD", Color::Green)
                        };
                        let device = if disk.readonly {
                            format!("{} (ro)", disk.dev_path)
                        } else {
                            disk.dev_path.clone()
                        };
                        Row::new(vec![
                            node_cell(*i),
                            Cell::from(device),
                            Cell::from(disk.size_pretty.clone()),
                            Cell::from(kind).style(Style::default().fg(color)),
                            Cell::from(disk.transport.clone().unwrap_or_default()),
                            Cell::from(disk.model.clone().unwrap_or_default()),
                            Cell::from(disk.serial.clone().unwrap_or_default()),
                        ])
                    })
                    .collect();
                let widths = vec![
                    self.node_width(),
                    Constraint::Length(16),
                    Constraint::Length(10),
                    Constraint::Length(7),
                    Constraint::Length(10),
                    Constraint::Length(36),
                    Constraint::Length(22),
                ];
                (header, rows, widths)
            }
            GroupKind::Network => {
                let header = Row::new(vec![
                    bold("NODE"),
                    bold("INTERFACE"),
                    bold("RX/s"),
                    bold("TX/s"),
                    bold("RX TOTAL"),
                    bold("TX TOTAL"),
                    bold("ERRORS"),
                    bold("DROPPED"),
                ]);
                let rate_cell = |r: Option<u64>| {
                    Cell::from(
                        r.map(NetDevStats::format_rate)
                            .unwrap_or_else(|| "-".to_string()),
                    )
                };
                let counter_cell = |n: u64| {
                    Cell::from(n.to_string()).style(Style::default().fg(if n > 0 {
                        Color::Yellow
                    } else {
                        Color::Reset
                    }))
                };
                let rows = self
                    .shown_ifaces(data)
                    .into_iter()
                    .map(|r| {
                        Row::new(vec![
                            node_cell(r.node),
                            Cell::from(r.name.clone()),
                            rate_cell(r.rx_rate).style(Style::default().fg(Color::Green)),
                            rate_cell(r.tx_rate).style(Style::default().fg(Color::Blue)),
                            Cell::from(NetDevStats::format_bytes(r.rx_bytes)),
                            Cell::from(NetDevStats::format_bytes(r.tx_bytes)),
                            counter_cell(r.errors),
                            counter_cell(r.dropped),
                        ])
                    })
                    .collect();
                let widths = vec![
                    self.node_width(),
                    Constraint::Length(14),
                    Constraint::Length(11),
                    Constraint::Length(11),
                    Constraint::Length(10),
                    Constraint::Length(10),
                    Constraint::Length(8),
                    Constraint::Length(9),
                ];
                (header, rows, widths)
            }
        }
    }

    /// Diagnostics problems across nodes, below the table
    fn problem_lines(&self, data: &GroupData) -> Vec<Line<'static>> {
        let problems = problems(&self.nodes, &data.diagnostics);
        if problems.is_empty() {
            return vec![Line::styled(
                "No warnings or failures",
                Style::default().fg(Color::Green),
            )];
        }
        problems
            .into_iter()
            .map(|p| {
                let (symbol, color) = p.status.indicator();
                let nodes = if p.nodes.len() == self.nodes.len() && self.nodes.len() > 1 {
                    "all nodes".to_string()
                } else {
                    p.nodes.join(", ")
                };
                Line::from(vec![
                    Span::styled(format!("{symbol} "), Style::default().fg(color)),
                    Span::styled(format!("{}: ", p.name), Style::default().fg(color)),
                    Span::raw(p.message),
                    Span::styled(
                        format!("  ({} · {nodes})", p.category),
                        Style::default().fg(Color::DarkGray),
                    ),
                ])
            })
            .collect()
    }
}

/// "✓ 8  ⚠ 1  ✗ 2" for a category, zero counts left out
fn count_cell(counts: [usize; 4]) -> Cell<'static> {
    let parts = [
        (counts[0], CheckStatus::Pass),
        (counts[1], CheckStatus::Warn),
        (counts[2], CheckStatus::Fail),
        (counts[3], CheckStatus::Unknown),
    ];
    let mut spans = Vec::new();
    for (n, status) in parts {
        if n == 0 {
            continue;
        }
        let (symbol, color) = status.indicator();
        spans.push(Span::styled(
            format!("{symbol} {n}  "),
            Style::default().fg(color),
        ));
    }
    if spans.is_empty() {
        spans.push(Span::styled("-", Style::default().fg(Color::DarkGray)));
    }
    Cell::from(Line::from(spans))
}

impl Component for GroupComponent {
    fn handle_key_event(&mut self, key: KeyEvent) -> Result<Option<Action>> {
        Ok(match key.code {
            KeyCode::Char('q') | KeyCode::Esc => Some(Action::Back),
            KeyCode::Char('j') | KeyCode::Down => {
                self.select(1);
                None
            }
            KeyCode::Char('k') | KeyCode::Up => {
                self.select(-1);
                None
            }
            KeyCode::Char('g') | KeyCode::Home => {
                self.table_state.select(Some(0));
                None
            }
            KeyCode::Char('G') | KeyCode::End => {
                self.select(isize::MAX / 2);
                None
            }
            KeyCode::Enter => self.open_selected(),
            KeyCode::Char('r') => Some(Action::Refresh),
            KeyCode::Char('v') if self.kind == GroupKind::Storage => {
                self.show_volumes = !self.show_volumes;
                self.table_state.select(Some(0));
                None
            }
            KeyCode::Char('a') if self.kind != GroupKind::Diagnostics => {
                self.show_all = !self.show_all;
                self.table_state.select(Some(0));
                None
            }
            _ => None,
        })
    }

    fn update(&mut self, action: Action) -> Result<Option<Action>> {
        if let Action::Tick = action
            && self
                .state
                .should_auto_refresh(true, self.kind.refresh_interval())
        {
            return Ok(Some(Action::Refresh));
        }
        Ok(None)
    }

    fn draw(&mut self, frame: &mut Frame, area: Rect) -> Result<()> {
        let Some(data) = self.state.data() else {
            let text = match self.state.error() {
                Some(e) => Line::styled(format!("Error: {e}"), Style::default().fg(Color::Red)),
                None => Line::styled(
                    format!("Loading {} of {}…", self.kind.label(), self.title),
                    Style::default().fg(Color::DarkGray),
                ),
            };
            frame.render_widget(Paragraph::new(text), area);
            return Ok(());
        };
        let errors = self.node_error_lines(data);
        let problems = if self.kind == GroupKind::Diagnostics {
            self.problem_lines(data)
        } else {
            Vec::new()
        };
        let (header, rows, widths) = self.table(data);
        let row_count = rows.len();
        let problems_height = if problems.is_empty() {
            0
        } else {
            (problems.len() as u16 + 2).min(area.height / 2)
        };
        let [title, warnings, table, problems_area, footer] = Layout::vertical([
            Constraint::Length(1),
            Constraint::Length(errors.len() as u16),
            Constraint::Min(4),
            Constraint::Length(problems_height),
            Constraint::Length(1),
        ])
        .areas(area);

        frame.render_widget(Paragraph::new(self.header_line()), title);
        frame.render_widget(Paragraph::new(errors), warnings);
        let table_widget = Table::new(rows, widths)
            // Columns keep their widths; spare room stays on the right
            .flex(ratatui::layout::Flex::Start)
            .header(header.bottom_margin(1))
            .block(Block::default().borders(Borders::TOP))
            .row_highlight_style(
                Style::default()
                    .bg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
            )
            .highlight_symbol("> ");
        if self.table_state.selected().unwrap_or(0) >= row_count {
            self.table_state.select(Some(row_count.saturating_sub(1)));
        }
        frame.render_stateful_widget(table_widget, table, &mut self.table_state);
        if !problems.is_empty() {
            frame.render_widget(
                Paragraph::new(problems).block(
                    Block::default()
                        .title(" Warnings and failures ")
                        .borders(Borders::TOP),
                ),
                problems_area,
            );
        }
        frame.render_widget(Paragraph::new(self.footer_line()), footer);
        Ok(())
    }
}

impl crate::components::BackgroundRefresh for GroupComponent {
    fn adopt_refresh(&mut self, refreshed: Self) {
        self.state = refreshed.state;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(name: &str) -> GroupNode {
        GroupNode {
            hostname: name.to_string(),
            address: format!("10.0.0.{}", name.len()),
            role: "worker".to_string(),
        }
    }

    fn check(name: &str, status: CheckStatus, message: &str) -> DiagnosticCheck {
        DiagnosticCheck {
            id: name.to_lowercase(),
            name: name.to_string(),
            status,
            message: message.to_string(),
            details: None,
            fix: None,
        }
    }

    fn iface(name: &str, bytes: u64) -> IfaceRow {
        IfaceRow {
            node: 0,
            name: name.to_string(),
            rx_rate: None,
            tx_rate: None,
            rx_bytes: bytes,
            tx_bytes: 0,
            errors: 0,
            dropped: 0,
        }
    }

    #[test]
    fn hides_pod_idle_and_loopback_interfaces() {
        for hidden in ["lo", "veth1a2b", "lxc12ab", "cali7f3"] {
            assert!(is_hidden_iface(&iface(hidden, 100)), "{hidden}");
        }
        // Kernel stubs that never carried traffic
        assert!(is_hidden_iface(&iface("sit0", 0)));
        for shown in [
            "eth0",
            "enp1s0",
            "bond0",
            "cni0",
            "flannel.1",
            "kubespan",
            "tailscale0",
        ] {
            assert!(!is_hidden_iface(&iface(shown, 100)), "{shown}");
        }
    }

    #[test]
    fn hides_virtual_disks_and_pseudo_volumes() {
        let disk = |path: &str| DiskInfo {
            id: path.trim_start_matches("/dev/").to_string(),
            dev_path: path.to_string(),
            size: 0,
            size_pretty: String::new(),
            model: None,
            serial: None,
            transport: None,
            rotational: false,
            readonly: false,
            cdrom: false,
            wwid: None,
            bus_path: None,
        };
        for hidden in [
            "/dev/loop3",
            "/dev/dm-0",
            "/dev/md127",
            "/dev/drbd1002",
            "/dev/zd16",
        ] {
            assert!(is_virtual_disk(&disk(hidden)), "{hidden}");
        }
        for shown in ["/dev/nvme0n1", "/dev/sda", "/dev/vda"] {
            assert!(!is_virtual_disk(&disk(shown)), "{shown}");
        }
        let volume = |size: &str| VolumeStatus {
            id: "x".to_string(),
            encryption_provider: None,
            phase: "ready".to_string(),
            size: size.to_string(),
            filesystem: None,
            mount_location: None,
        };
        assert!(is_pseudo_volume(&volume("directory")));
        assert!(is_pseudo_volume(&volume("overlay")));
        assert!(!is_pseudo_volume(&volume("100 GB")));
    }

    #[test]
    fn rates_from_counter_deltas() {
        assert_eq!(rate(1_000, 3_000, Duration::from_secs(2)), Some(1_000));
        // Counter reset (interface recreated): no rate rather than a huge one
        assert_eq!(rate(3_000, 1_000, Duration::from_secs(2)), None);
        assert_eq!(rate(1, 2, Duration::ZERO), None);
    }

    #[test]
    fn counts_check_statuses() {
        let checks = [
            check("A", CheckStatus::Pass, ""),
            check("B", CheckStatus::Pass, ""),
            check("C", CheckStatus::Warn, ""),
            check("D", CheckStatus::Fail, ""),
            check("E", CheckStatus::Unknown, ""),
        ];
        assert_eq!(status_counts(&checks), [2, 1, 1, 1]);
    }

    #[test]
    fn lists_each_problem_once_with_its_nodes_failures_first() {
        let nodes = [node("w1"), node("w2"), node("w3")];
        let memory_warn = check("Memory", CheckStatus::Warn, "90% used");
        let diagnostics: Vec<NodeChecks> = vec![
            Ok(vec![("System Health", vec![memory_warn.clone()])]),
            Ok(vec![(
                "System Health",
                vec![
                    memory_warn,
                    check("Disk", CheckStatus::Fail, "EPHEMERAL full"),
                    check("CPU", CheckStatus::Pass, "ok"),
                ],
            )]),
            Err("timed out".to_string()),
        ];
        let found = problems(&nodes, &diagnostics);
        assert_eq!(found.len(), 2);
        assert_eq!(found[0].name, "Disk");
        assert_eq!(found[0].nodes, ["w2"]);
        assert_eq!(found[1].name, "Memory");
        assert_eq!(found[1].nodes, ["w1", "w2"]);
    }
}

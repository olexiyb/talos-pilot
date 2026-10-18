//! Volumes view
//!
//! Every persistent volume the cluster's storage layers know about, in one
//! list, problems first:
//!
//! - LINSTOR (Piraeus) volumes with their replicas, checked against the DRBD
//!   state in each node's kernel (see `crate::linstor_volumes`): a DRBD device
//!   smaller than its volume, blocks out of sync on an "Established" link,
//!   fewer replicas than the resource group places
//! - OpenEBS ZFS LocalPV volumes (see `crate::zfs`): one dataset on one node
//!
//! For both, the filesystem's size and fill level come from the Talos Mounts
//! API on the node where the volume is mounted. Like the workloads view it
//! can be scoped to the selected node(s).

use crate::action::Action;
use crate::components::Component;
use crate::linstor_volumes::{
    Issue, LinstorVolume, Remedy, Replica, SatellitePod, Severity, apply_mounts, fetch_mounts,
    fetch_satellites, fetch_volumes, format_kib, linstor_mount_nodes, observe_drbd, remediation,
    volume_issues,
};
use crate::zfs::{ZfsVolume, apply_zfs_mounts, fetch_zfs_volumes, zfs_volume_issues};
use color_eyre::Result;
use crossterm::event::{KeyCode, KeyEvent};
use kube::Client;
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState, Wrap},
};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use talos_pilot_core::AsyncState;
use talos_rs::TalosClient;

/// Auto-refresh interval: a full scan reads a few hundred kernel files
const AUTO_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// Where a volume comes from
#[derive(Debug, Clone)]
enum Source {
    Linstor(LinstorVolume),
    Zfs(ZfsVolume),
}

/// A volume with the issues found on it
#[derive(Debug, Clone)]
struct VolumeRow {
    source: Source,
    issues: Vec<Issue>,
}

impl VolumeRow {
    fn worst(&self) -> Option<Severity> {
        self.issues.iter().map(|i| i.severity).min()
    }

    /// Stable identity: resource or volume name, and volume number
    fn key(&self) -> (String, u32) {
        match &self.source {
            Source::Linstor(v) => (v.resource.clone(), v.volume_number),
            Source::Zfs(v) => (v.name.clone(), 0),
        }
    }

    fn label(&self) -> &str {
        match &self.source {
            Source::Linstor(v) => v.pvc.as_deref().unwrap_or(&v.resource),
            Source::Zfs(v) => v.pvc.as_deref().unwrap_or(&v.name),
        }
    }

    fn name(&self) -> &str {
        match &self.source {
            Source::Linstor(v) => &v.resource,
            Source::Zfs(v) => &v.name,
        }
    }

    fn type_label(&self) -> &'static str {
        match &self.source {
            Source::Linstor(v) if v.replicas.iter().any(|r| r.drbd_layer) => "DRBD",
            Source::Linstor(_) => "LINSTOR",
            Source::Zfs(_) => "ZFS",
        }
    }

    fn kind(&self) -> TypeFilter {
        match &self.source {
            Source::Linstor(_) => TypeFilter::Linstor,
            Source::Zfs(_) => TypeFilter::Zfs,
        }
    }

    fn size_kib(&self) -> Option<u64> {
        match &self.source {
            Source::Linstor(v) => v.size_kib,
            Source::Zfs(v) => v.capacity.map(|b| b / 1024),
        }
    }

    /// Filesystem used and size (KiB), where the volume is mounted
    fn usage_kib(&self) -> Option<(u64, u64)> {
        match &self.source {
            Source::Linstor(v) => v.usage_kib(),
            Source::Zfs(v) => v.fs.map(|(size, used)| (used / 1024, size / 1024)),
        }
    }

    fn nodes(&self) -> Vec<&str> {
        match &self.source {
            Source::Linstor(v) => v.replicas.iter().map(|r| r.node.as_str()).collect(),
            Source::Zfs(v) => vec![v.node.as_str()],
        }
    }
}

/// Which kind of volume the list shows
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TypeFilter {
    #[default]
    All,
    Linstor,
    Zfs,
}

impl TypeFilter {
    fn next(self) -> Self {
        match self {
            TypeFilter::All => TypeFilter::Linstor,
            TypeFilter::Linstor => TypeFilter::Zfs,
            TypeFilter::Zfs => TypeFilter::All,
        }
    }

    fn label(self) -> &'static str {
        match self {
            TypeFilter::All => "all",
            TypeFilter::Linstor => "LINSTOR",
            TypeFilter::Zfs => "ZFS",
        }
    }
}

/// Data loaded asynchronously for the view
#[derive(Debug, Clone, Default)]
struct VolumesData {
    /// `false` when the cluster has neither LINSTOR nor ZFS LocalPV
    installed: bool,
    /// Issues first (errors, then warnings), then by name
    rows: Vec<VolumeRow>,
    /// DRBD replicas in scope, and those whose kernel state could be read
    replicas: usize,
    replicas_observed: usize,
    /// Satellite pod per node, for the `kubectl exec` of fix suggestions
    satellites: HashMap<String, SatellitePod>,
    /// A storage layer that couldn't be queried, while others could
    source_errors: Vec<String>,
}

/// What the view remembers between visits: the issues/all and type
/// toggles, the text filter and the selected volume (name, volume number)
#[derive(Debug, Clone, Default)]
pub struct VolumesViewState {
    pub show_all: bool,
    pub type_filter: TypeFilter,
    pub filter: String,
    pub selected: Option<(String, u32)>,
}

/// Volumes component
#[derive(Clone)]
pub struct VolumesComponent {
    state: AsyncState<VolumesData>,
    /// What the view covers, e.g. the cluster or node name
    title: String,
    /// Nodes the view is limited to (`None` for the whole cluster)
    scope: Option<HashSet<String>>,

    k8s_client: Option<Client>,
    /// Talos client and control plane node to get a Kubernetes client from
    k8s_source: Option<(TalosClient, Option<String>)>,
    /// Talos client and node name -> address, for kernel state and mounts
    talos: Option<TalosClient>,
    node_ips: HashMap<String, String>,

    /// Show healthy volumes too
    show_all: bool,
    type_filter: TypeFilter,
    /// Text filter on volume, resource and node names
    filter: String,
    filter_editing: bool,

    selected: usize,
    table_state: TableState,
    /// Volume to select once the first scan lands (from a previous visit)
    pending_select: Option<(String, u32)>,
    /// When the selected volume's fix commands were copied, for feedback
    copied_at: Option<std::time::Instant>,
    /// When the view opened, to show how long the first scan is taking
    opened_at: std::time::Instant,
}

impl VolumesComponent {
    /// Volumes view titled `title`, limited to volumes on `nodes` when given
    pub fn new(title: String, nodes: Option<Vec<String>>) -> Self {
        let mut table_state = TableState::default();
        table_state.select(Some(0));
        Self {
            state: AsyncState::new(),
            title,
            scope: nodes.map(|n| n.into_iter().collect()),
            k8s_client: None,
            k8s_source: None,
            talos: None,
            node_ips: HashMap::new(),
            show_all: true,
            type_filter: TypeFilter::All,
            filter: String::new(),
            filter_editing: false,
            selected: 0,
            table_state,
            pending_select: None,
            copied_at: None,
            opened_at: std::time::Instant::now(),
        }
    }

    /// Toggles, filter and selection to keep for the next visit
    pub fn view_state(&self) -> VolumesViewState {
        VolumesViewState {
            show_all: self.show_all,
            type_filter: self.type_filter,
            filter: self.filter.clone(),
            selected: self
                .visible()
                .get(self.selected)
                .map(|r| r.key())
                .or_else(|| self.pending_select.clone()),
        }
    }

    /// Restore a previous visit's toggles and filter; its selected volume is
    /// selected once the first scan lands, if it still exists
    pub fn set_view_state(&mut self, state: VolumesViewState) {
        self.show_all = state.show_all;
        self.type_filter = state.type_filter;
        self.filter = state.filter;
        self.pending_select = state.selected;
    }

    /// Talos client and node addresses for kernel state and mounts, and
    /// where to get the Kubernetes client from (the kubeconfig of `cp_ip`)
    pub fn set_clients(
        &mut self,
        talos: TalosClient,
        node_ips: HashMap<String, String>,
        cp_ip: Option<String>,
    ) {
        self.k8s_source = Some((talos.clone(), cp_ip));
        self.talos = Some(talos);
        self.node_ips = node_ips;
    }

    pub fn set_error(&mut self, error: String) {
        self.state.set_error(error);
    }

    /// Scan the storage layers, the nodes' DRBD state and mounts
    pub async fn refresh(&mut self) -> Result<()> {
        self.state.start_loading();

        if self.k8s_client.is_none()
            && let Some((talos_client, cp_ip)) = &self.k8s_source
        {
            let created = tokio::time::timeout(
                Duration::from_secs(15),
                crate::components::diagnostics::k8s::create_k8s_client_with_source(
                    talos_client,
                    cp_ip.as_deref(),
                    None,
                ),
            )
            .await;
            match created {
                Ok(Ok((client, _source))) => self.k8s_client = Some(client),
                Ok(Err(e)) => {
                    self.set_error(format!("Failed to connect to Kubernetes: {e}"));
                    return Ok(());
                }
                Err(_) => {
                    self.set_error(
                        "Failed to connect to Kubernetes: timed out fetching the kubeconfig"
                            .to_string(),
                    );
                    return Ok(());
                }
            }
        }
        let Some(client) = &self.k8s_client else {
            self.set_error("No Kubernetes client configured".to_string());
            return Ok(());
        };

        // All bounded: kube's own read timeout is minutes, so a dead API
        // connection would otherwise hold the scan that long
        let (linstor, zfs, satellites) = tokio::join!(
            tokio::time::timeout(Duration::from_secs(20), fetch_volumes(client)),
            tokio::time::timeout(Duration::from_secs(20), fetch_zfs_volumes(client)),
            tokio::time::timeout(Duration::from_secs(10), fetch_satellites(client)),
        );
        let satellites = satellites.unwrap_or_default();
        let mut source_errors = Vec::new();
        let linstor = match linstor {
            Ok(Ok(volumes)) => volumes,
            Ok(Err(e)) => {
                source_errors.push(format!("LINSTOR: {e}"));
                None
            }
            Err(_) => {
                source_errors.push("LINSTOR: timed out after 20s".to_string());
                None
            }
        };
        let zfs = match zfs {
            Ok(Ok(volumes)) => volumes,
            Ok(Err(e)) => {
                source_errors.push(format!("ZFS LocalPV: {e}"));
                None
            }
            Err(_) => {
                source_errors.push("ZFS LocalPV: timed out after 20s".to_string());
                None
            }
        };
        if linstor.is_none() && zfs.is_none() {
            if source_errors.is_empty() {
                self.state.set_data(VolumesData::default());
            } else {
                self.set_error(source_errors.join("; "));
                // Its connection may be dead; reconnect on the next refresh
                self.k8s_client = None;
            }
            return Ok(());
        }
        let mut linstor = linstor.unwrap_or_default();
        let mut zfs = zfs.unwrap_or_default();

        let in_scope = |node: &str| self.scope.as_ref().is_none_or(|s| s.contains(node));
        if self.scope.is_some() {
            linstor.retain(|v| v.replicas.iter().any(|r| in_scope(&r.node)));
            zfs.retain(|v| in_scope(&v.node));
        }
        if let Some(talos) = &self.talos {
            let node_ips: HashMap<String, String> = self
                .node_ips
                .iter()
                .filter(|(node, _)| in_scope(node))
                .map(|(n, ip)| (n.clone(), ip.clone()))
                .collect();
            let mut mount_nodes = linstor_mount_nodes(&linstor);
            mount_nodes.extend(zfs.iter().map(|v| v.node.clone()));
            mount_nodes.sort();
            mount_nodes.dedup();
            // Each read is bounded; these bound the whole scan
            let (_, mounts) = tokio::join!(
                tokio::time::timeout(
                    Duration::from_secs(45),
                    observe_drbd(talos, &node_ips, &mut linstor),
                ),
                tokio::time::timeout(
                    Duration::from_secs(20),
                    fetch_mounts(talos, &node_ips, &mount_nodes),
                ),
            );
            let mounts = mounts.unwrap_or_default();
            apply_mounts(&mut linstor, &mounts);
            apply_zfs_mounts(&mut zfs, &mounts);
        }

        // Only DRBD replicas have kernel state to read
        let scoped: Vec<&Replica> = linstor
            .iter()
            .flat_map(|v| &v.replicas)
            .filter(|r| r.drbd_layer && in_scope(&r.node))
            .collect();
        let replicas = scoped.len();
        let replicas_observed = scoped
            .iter()
            .filter(|r| r.drbd.as_ref().is_some_and(|d| d.size_kib.is_some()))
            .count();

        let mut rows: Vec<VolumeRow> = linstor
            .into_iter()
            .map(|v| VolumeRow {
                issues: volume_issues(&v),
                source: Source::Linstor(v),
            })
            .chain(zfs.into_iter().map(|v| VolumeRow {
                issues: zfs_volume_issues(&v),
                source: Source::Zfs(v),
            }))
            .collect();
        // Worst first; `None` (healthy) sorts after both severities
        rows.sort_by(|a, b| {
            let rank = |r: &VolumeRow| r.worst().map_or(2, |s| s as u8);
            rank(a).cmp(&rank(b)).then_with(|| a.label().cmp(b.label()))
        });

        self.state.set_data(VolumesData {
            installed: true,
            rows,
            replicas,
            replicas_observed,
            satellites,
            source_errors,
        });
        Ok(())
    }

    /// Rows shown under the current toggles and text filter
    fn visible(&self) -> Vec<&VolumeRow> {
        let Some(data) = self.state.data() else {
            return Vec::new();
        };
        let filter = self.filter.to_lowercase();
        data.rows
            .iter()
            .filter(|r| self.show_all || !r.issues.is_empty())
            .filter(|r| self.type_filter == TypeFilter::All || r.kind() == self.type_filter)
            .filter(|r| {
                filter.is_empty()
                    || r.label().to_lowercase().contains(&filter)
                    || r.name().to_lowercase().contains(&filter)
                    || r.nodes().iter().any(|n| n.to_lowercase().contains(&filter))
            })
            .collect()
    }

    fn clamp_selection(&mut self) {
        let len = self.visible().len();
        self.selected = self.selected.min(len.saturating_sub(1));
        self.table_state.select(Some(self.selected));
    }

    fn draw_summary(&self, frame: &mut Frame, area: Rect, data: &VolumesData) {
        let count = |kind: TypeFilter| data.rows.iter().filter(|r| r.kind() == kind).count();
        let (linstor, zfs) = (count(TypeFilter::Linstor), count(TypeFilter::Zfs));
        let errors = data
            .rows
            .iter()
            .filter(|r| r.worst() == Some(Severity::Error))
            .count();
        let warnings = data
            .rows
            .iter()
            .filter(|r| r.worst() == Some(Severity::Warning))
            .count();
        let dim = Style::default().dim();
        let mut spans = vec![
            Span::styled(format!("{}", data.rows.len()), Style::default().bold()),
            Span::raw(" volumes "),
            Span::styled(format!("({linstor} LINSTOR, {zfs} ZFS)  "), dim),
        ];
        if errors > 0 {
            spans.push(Span::styled(
                format!("✗ {errors} with errors  "),
                Style::default().fg(Color::Red),
            ));
        }
        if warnings > 0 {
            spans.push(Span::styled(
                format!("◐ {warnings} with warnings  "),
                Style::default().fg(Color::Yellow),
            ));
        }
        if errors == 0 && warnings == 0 {
            spans.push(Span::styled(
                "● no issues  ",
                Style::default().fg(Color::Green),
            ));
        }
        // Without the kernel's view, size and out-of-sync checks were skipped
        if data.replicas > 0 {
            let observed_style = if data.replicas_observed < data.replicas {
                Style::default().fg(Color::Yellow)
            } else {
                dim
            };
            spans.push(Span::styled(
                format!(
                    "DRBD state read for {}/{} replicas  ",
                    data.replicas_observed, data.replicas
                ),
                observed_style,
            ));
        }
        for e in &data.source_errors {
            spans.push(Span::styled(
                format!("{e}  "),
                Style::default().fg(Color::Yellow),
            ));
        }
        frame.render_widget(
            Paragraph::new(Line::from(spans)).block(Block::default().borders(Borders::BOTTOM)),
            area,
        );
    }

    fn draw_table(&mut self, frame: &mut Frame, area: Rect) {
        let dim = Style::default().dim();
        let visible = self.visible();
        let empty = visible.is_empty();
        let label_width = visible
            .iter()
            .map(|r| r.label().chars().count())
            .max()
            .unwrap_or(6)
            .clamp(6, 60);
        let nodes_cells: Vec<Line> = visible.iter().map(|r| nodes_line(r)).collect();
        let nodes_width = nodes_cells
            .iter()
            .map(Line::width)
            .max()
            .unwrap_or(5)
            .max(5);
        let rows: Vec<Row> = visible
            .iter()
            .zip(nodes_cells)
            .map(|(r, nodes)| {
                let (symbol, color) = match r.worst() {
                    Some(Severity::Error) => ("✗", Color::Red),
                    Some(Severity::Warning) => ("◐", Color::Yellow),
                    None => ("●", Color::Green),
                };
                let size = r.size_kib().map(format_kib).unwrap_or("?".to_string());
                let issue = match r.issues.as_slice() {
                    [] => Span::raw(""),
                    [first, rest @ ..] => Span::styled(
                        if rest.is_empty() {
                            first.text.clone()
                        } else {
                            format!("{} (+{})", first.text, rest.len())
                        },
                        Style::default().fg(color),
                    ),
                };
                Row::new(vec![
                    Cell::from(Span::styled(symbol, Style::default().fg(color))),
                    Cell::from(r.label().to_string()),
                    Cell::from(Span::styled(r.type_label(), dim)),
                    Cell::from(Span::styled(size, dim)),
                    Cell::from(used_span(r.usage_kib())),
                    Cell::from(copies_span(r)),
                    Cell::from(nodes),
                    Cell::from(issue),
                ])
            })
            .collect();

        let widths = [
            Constraint::Length(1),
            Constraint::Length(label_width as u16),
            Constraint::Length(7),
            Constraint::Length(10),
            Constraint::Length(4),
            Constraint::Length(6),
            Constraint::Length(nodes_width as u16),
            Constraint::Fill(1),
        ];
        let header = Row::new(vec![
            "", "Volume", "Type", "Size", "Used", "Copies", "Nodes", "Issues",
        ])
        .style(Style::default().fg(Color::Gray).bold());
        let table = Table::new(rows, widths)
            .header(header)
            .column_spacing(2)
            .row_highlight_style(
                Style::default()
                    .bg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
            );
        frame.render_stateful_widget(table, area, &mut self.table_state);

        if empty {
            let message = if !self.show_all && self.filter.is_empty() {
                "No volumes with issues ([a] shows all)"
            } else {
                "No volumes match"
            };
            let [_, message_area] =
                Layout::vertical([Constraint::Length(1), Constraint::Fill(1)]).areas(area);
            frame.render_widget(Paragraph::new(Span::styled(message, dim)), message_area);
        }
    }

    /// The selected volume: its issues and fixes first, then its replicas
    /// (LINSTOR) or dataset (ZFS)
    fn detail_lines(&self) -> Vec<Line<'static>> {
        let visible = self.visible();
        let Some(row) = visible.get(self.selected) else {
            return Vec::new();
        };
        let dim = Style::default().dim();
        let size = row.size_kib().map(format_kib).unwrap_or("?".to_string());
        let used = match row.usage_kib() {
            Some((used, fs)) => format!("  filesystem {} of {}", format_kib(used), format_kib(fs)),
            None => String::new(),
        };
        let description = match &row.source {
            Source::Linstor(v) => {
                let placed = v
                    .place_count
                    .map(|n| format!("  placed ×{n}"))
                    .unwrap_or_default();
                format!(
                    "  {}  vol {}  LINSTOR size {size}{placed}{used}",
                    v.resource, v.volume_number
                )
            }
            Source::Zfs(v) => format!(
                "  {}  ZFS {} on {}  {}  capacity {size}{used}",
                v.name,
                v.volume_type.to_lowercase(),
                v.node,
                v.dataset()
            ),
        };
        let mut lines = vec![Line::from(vec![
            Span::styled(row.label().to_string(), Style::default().bold()),
            Span::styled(description, dim),
        ])];
        // Issues and fixes first, so they stay visible when the pane is cut off
        for issue in &row.issues {
            let color = match issue.severity {
                Severity::Error => Color::Red,
                Severity::Warning => Color::Yellow,
            };
            lines.push(Line::from(Span::styled(
                format!("  {}", issue.text),
                Style::default().fg(color),
            )));
        }
        let remedies = self.remedies();
        if !remedies.is_empty() {
            lines.push(Line::from(vec![
                Span::styled(
                    "  Suggested fix: review, run by hand, one volume at a time ",
                    Style::default().fg(Color::Cyan).bold(),
                ),
                Span::styled("[c] copy", dim),
            ]));
            for remedy in &remedies {
                lines.push(Line::from(Span::styled(
                    format!("  • {}", remedy.summary),
                    Style::default().fg(Color::Cyan),
                )));
                for command in &remedy.commands {
                    lines.push(Line::from(Span::raw(format!("      {command}"))));
                }
            }
        }
        match &row.source {
            Source::Linstor(v) => lines.extend(replica_lines(v)),
            Source::Zfs(v) => lines.push(Line::from(vec![
                Span::raw(format!("  {:<10}", v.node)),
                Span::styled(
                    format!("{:<10}", v.state),
                    Style::default().fg(if v.state == "Ready" {
                        Color::Green
                    } else {
                        Color::Red
                    }),
                ),
                Span::styled("local dataset, not replicated", dim),
            ])),
        }
        lines
    }

    /// Suggested fixes for the selected volume (LINSTOR only)
    fn remedies(&self) -> Vec<Remedy> {
        let visible = self.visible();
        let (Some(data), Some(row)) = (self.state.data(), visible.get(self.selected)) else {
            return Vec::new();
        };
        match &row.source {
            Source::Linstor(v) => remediation(v, &data.satellites),
            Source::Zfs(_) => Vec::new(),
        }
    }

    /// Copy the selected volume's fix commands, each step preceded by its
    /// summary as a shell comment
    fn copy_remedies(&mut self) {
        let remedies = self.remedies();
        if remedies.is_empty() {
            return;
        }
        let text = remedies
            .iter()
            .map(|r| format!("# {}\n{}", r.summary, r.commands.join("\n")))
            .collect::<Vec<_>>()
            .join("\n\n");
        // A thread keeps the clipboard alive long enough for clipboard
        // managers to read it
        std::thread::spawn(move || {
            if let Ok(mut clipboard) = arboard::Clipboard::new() {
                let _ = clipboard.set_text(text);
                std::thread::sleep(Duration::from_millis(100));
            }
        });
        self.copied_at = Some(std::time::Instant::now());
    }

    fn draw_footer(&self, frame: &mut Frame, area: Rect) {
        let key = |k: &'static str| Span::styled(k, Style::default().bold());
        let spans = if self.filter_editing {
            vec![
                Span::raw("Filter: "),
                Span::styled(
                    format!("{}▏", self.filter),
                    Style::default().fg(Color::Cyan),
                ),
                Span::raw("  "),
                key("[Enter]"),
                Span::raw(" done  "),
                key("[Esc]"),
                Span::raw(" clear"),
            ]
        } else {
            let mut spans = vec![
                key("[j/k]"),
                Span::raw(" select  "),
                key("[a]"),
                Span::raw(if self.show_all {
                    " issues only  "
                } else {
                    " show all  "
                }),
                key("[t]"),
                Span::raw(format!(" type: {}  ", self.type_filter.label())),
                key("[/]"),
                Span::raw(" filter  "),
                key("[r]"),
                Span::raw(" refresh  "),
                key("[q]"),
                Span::raw(" back"),
            ];
            if !self.remedies().is_empty() {
                spans.extend([Span::raw("  "), key("[c]"), Span::raw(" copy fix")]);
            }
            if self
                .copied_at
                .is_some_and(|at| at.elapsed() < Duration::from_secs(3))
            {
                spans.push(Span::styled(
                    "   copied to clipboard",
                    Style::default().fg(Color::Green),
                ));
            }
            if !self.filter.is_empty() {
                spans.push(Span::styled(
                    format!("   filter: {}", self.filter),
                    Style::default().fg(Color::Cyan),
                ));
            }
            spans
        };
        frame.render_widget(Paragraph::new(Line::from(spans)), area);
    }
}

/// Each replica of a LINSTOR volume and its links
fn replica_lines(volume: &LinstorVolume) -> Vec<Line<'static>> {
    let dim = Style::default().dim();
    let mut lines = Vec::new();
    for r in &volume.replicas {
        if !r.drbd_layer {
            lines.push(Line::from(vec![
                Span::raw(format!("  {:<10}", r.node)),
                Span::styled("local volume, not replicated by DRBD", dim),
            ]));
            continue;
        }
        let role = if r.in_use { "Primary" } else { "Secondary" };
        let mut drbd_size = match r.drbd.as_ref().and_then(|d| d.size_kib) {
            Some(kib) => format!("DRBD {}", format_kib(kib)),
            None => "DRBD state unknown".to_string(),
        };
        if let Some(fs) = r.drbd.as_ref().and_then(|d| d.fs_size_kib) {
            drbd_size.push_str(&format!("  filesystem {}", format_kib(fs)));
        }
        lines.push(Line::from(vec![
            Span::raw(format!("  {:<10}", r.node)),
            Span::raw(format!("{role:<10}")),
            Span::styled(
                format!("{:<13}", r.disk_state),
                Style::default().fg(disk_color(r)),
            ),
            Span::styled(
                format!(
                    "{:<14}{drbd_size}",
                    r.minor.map(|m| format!("/dev/drbd{m}")).unwrap_or_default()
                ),
                dim,
            ),
        ]));
        for p in &r.peers {
            let seen = r.drbd.as_ref().and_then(|d| d.peers.get(&p.peer));
            let oos = seen
                .and_then(|s| s.oos_kib)
                .map(|k| format!("oos {}", format_kib(k)))
                .unwrap_or_else(|| "oos ?".to_string());
            let link_color = if p.connected {
                Color::Green
            } else {
                Color::Red
            };
            lines.push(Line::from(vec![
                Span::styled(format!("      → {:<10}", p.peer), dim),
                Span::styled(
                    format!("{:<12}", p.message),
                    Style::default().fg(link_color),
                ),
                Span::raw(format!(
                    "{:<14}",
                    p.replication.clone().unwrap_or_else(|| "?".to_string())
                )),
                Span::styled(oos, dim),
            ]));
        }
    }
    lines
}

/// Color of a replica's own disk state
fn disk_color(r: &Replica) -> Color {
    match r.disk_state.as_str() {
        _ if !r.drbd_layer => Color::Reset,
        _ if r.diskless => Color::DarkGray,
        "UpToDate" => Color::Green,
        "Inconsistent" => Color::Yellow,
        _ => Color::Red,
    }
}

/// Fill level as `51%`, colored like the usage issues; `-` when unknown
fn used_span(usage: Option<(u64, u64)>) -> Span<'static> {
    match usage {
        Some((used, size)) if size > 0 => {
            let pct = used as f64 / size as f64 * 100.0;
            let color = if pct >= 95.0 {
                Color::Red
            } else if pct >= 85.0 {
                Color::Yellow
            } else {
                Color::Green
            };
            Span::styled(format!("{pct:.0}%"), Style::default().fg(color))
        }
        _ => Span::styled("-", Style::default().dim()),
    }
}

/// Copies holding data against how many are placed, e.g. `3/3`, `2/3` in
/// red; a ZFS LocalPV volume is always the one local copy
fn copies_span(row: &VolumeRow) -> Span<'static> {
    match &row.source {
        Source::Linstor(v) => {
            let have = v.diskful_replicas();
            match v.place_count {
                Some(want) if have < want as usize => {
                    Span::styled(format!("{have}/{want}"), Style::default().fg(Color::Red))
                }
                Some(want) => Span::raw(format!("{have}/{want}")),
                None => Span::raw(have.to_string()),
            }
        }
        Source::Zfs(_) => Span::styled("1/1", Style::default().dim()),
    }
}

/// Nodes of a volume. LINSTOR: `bc1* bc2 bc4 bc3~` (`*` the Primary, `~`
/// diskless, `?` DRBD state unread), colored by disk state; `local` when
/// not replicated
fn nodes_line(row: &VolumeRow) -> Line<'static> {
    let replicas = match &row.source {
        Source::Zfs(v) => {
            return Line::from(vec![
                Span::raw(v.node.clone()),
                Span::styled(" local", Style::default().dim()),
            ]);
        }
        Source::Linstor(v) => &v.replicas,
    };
    let mut spans = Vec::new();
    for (i, r) in replicas.iter().enumerate() {
        if i > 0 {
            spans.push(Span::raw(" "));
        }
        let mut text = r.node.clone();
        if r.in_use {
            text.push('*');
        }
        if r.diskless {
            text.push('~');
        }
        spans.push(Span::styled(text, Style::default().fg(disk_color(r))));
        if r.drbd_layer && r.drbd.as_ref().is_none_or(|d| d.size_kib.is_none()) {
            spans.push(Span::styled("?", Style::default().dim()));
        }
    }
    if replicas.iter().all(|r| !r.drbd_layer) {
        spans.push(Span::styled(" local", Style::default().dim()));
    }
    Line::from(spans)
}

impl Component for VolumesComponent {
    fn handle_key_event(&mut self, key: KeyEvent) -> Result<Option<Action>> {
        if self.filter_editing {
            match key.code {
                KeyCode::Enter => self.filter_editing = false,
                KeyCode::Esc => {
                    self.filter.clear();
                    self.filter_editing = false;
                }
                KeyCode::Backspace => {
                    self.filter.pop();
                }
                KeyCode::Char(c) => self.filter.push(c),
                _ => {}
            }
            self.clamp_selection();
            return Ok(None);
        }
        match key.code {
            KeyCode::Char('q') => return Ok(Some(Action::Back)),
            KeyCode::Esc => {
                if self.filter.is_empty() {
                    return Ok(Some(Action::Back));
                }
                self.filter.clear();
            }
            KeyCode::Char('j') | KeyCode::Down => self.selected += 1,
            KeyCode::Char('k') | KeyCode::Up => self.selected = self.selected.saturating_sub(1),
            KeyCode::PageDown => self.selected += 10,
            KeyCode::PageUp => self.selected = self.selected.saturating_sub(10),
            KeyCode::Home | KeyCode::Char('g') => self.selected = 0,
            KeyCode::End | KeyCode::Char('G') => self.selected = usize::MAX,
            KeyCode::Char('a') => {
                self.show_all = !self.show_all;
                self.selected = 0;
            }
            KeyCode::Char('t') => {
                self.type_filter = self.type_filter.next();
                self.selected = 0;
            }
            KeyCode::Char('/') => self.filter_editing = true,
            KeyCode::Char('r') => return Ok(Some(Action::Refresh)),
            KeyCode::Char('c') => self.copy_remedies(),
            _ => return Ok(None),
        }
        self.clamp_selection();
        Ok(None)
    }

    fn update(&mut self, action: Action) -> Result<Option<Action>> {
        if let Action::Tick = action
            && self.state.should_auto_refresh(true, AUTO_REFRESH_INTERVAL)
        {
            return Ok(Some(Action::Refresh));
        }
        Ok(None)
    }

    fn draw(&mut self, frame: &mut Frame, area: Rect) -> Result<()> {
        let block = Block::default()
            .title(format!(" Volumes: {} ", self.title))
            .borders(Borders::ALL);
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let dim = Style::default().fg(Color::DarkGray);

        if self.state.is_loading() && !self.state.has_data() {
            frame.render_widget(
                Paragraph::new(format!(
                    "Scanning volumes, DRBD state and mounts... {}s",
                    self.opened_at.elapsed().as_secs()
                ))
                .style(dim),
                inner,
            );
            return Ok(());
        }
        if let Some(err) = self.state.error() {
            frame.render_widget(
                Paragraph::new(format!("Error: {err}")).style(Style::default().fg(Color::Red)),
                inner,
            );
            return Ok(());
        }
        let Some(data) = self.state.data().cloned() else {
            return Ok(());
        };
        if !data.installed {
            frame.render_widget(
                Paragraph::new(
                    "No LINSTOR (Piraeus) or OpenEBS ZFS LocalPV volumes found in this cluster",
                )
                .style(dim),
                inner,
            );
            return Ok(());
        }

        let details = self.detail_lines();
        // Long lines (fix commands) wrap; size the pane for the wrapped rows
        let width = inner.width.max(1) as usize;
        let wrapped_rows: usize = details
            .iter()
            .map(|line| line.width().max(1).div_ceil(width))
            .sum();
        let detail_height = (wrapped_rows as u16 + 1).min(inner.height * 3 / 5);
        let [summary_area, table_area, detail_area, footer_area] = Layout::vertical([
            Constraint::Length(2),
            Constraint::Fill(1),
            Constraint::Length(detail_height),
            Constraint::Length(1),
        ])
        .areas(inner);

        self.draw_summary(frame, summary_area, &data);
        self.draw_table(frame, table_area);
        frame.render_widget(
            Paragraph::new(details)
                .wrap(Wrap { trim: false })
                .block(Block::default().borders(Borders::TOP)),
            detail_area,
        );
        self.draw_footer(frame, footer_area);
        Ok(())
    }
}

impl crate::components::BackgroundRefresh for VolumesComponent {
    fn adopt_refresh(&mut self, refreshed: Self) {
        // Keep the selected volume selected as rows reorder; on the first
        // scan, select the one from the previous visit
        let selected = match self.pending_select.take() {
            Some(key) => Some(key),
            None => self.visible().get(self.selected).map(|r| r.key()),
        };
        self.state = refreshed.state;
        // Including `None` after a failure, so the next refresh reconnects
        self.k8s_client = refreshed.k8s_client;
        if let Some(key) = selected
            && let Some(index) = self.visible().iter().position(|r| r.key() == key)
        {
            self.selected = index;
        }
        self.clamp_selection();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::linstor_volumes::{DrbdObserved, PeerLink, PeerObserved};
    use ratatui::{Terminal, backend::TestBackend};

    fn replica(node: &str, in_use: bool, peers: &[&str], oos: u64) -> Replica {
        Replica {
            node: node.to_string(),
            drbd_layer: true,
            in_use,
            diskless: false,
            disk_state: "UpToDate".to_string(),
            minor: Some(1002),
            peers: peers
                .iter()
                .map(|p| PeerLink {
                    peer: p.to_string(),
                    connected: true,
                    message: "Connected".to_string(),
                    replication: Some("Established".to_string()),
                })
                .collect(),
            drbd: Some(DrbdObserved {
                size_kib: Some(11 * 1024 * 1024),
                fs_size_kib: in_use.then_some(10 * 1024 * 1024),
                fs_used_kib: in_use.then_some(5 * 1024 * 1024),
                peers: peers
                    .iter()
                    .map(|p| {
                        (
                            p.to_string(),
                            PeerObserved {
                                cs: Some("Established".to_string()),
                                oos_kib: Some(oos),
                                ap_in_flight_kib: Some(0),
                            },
                        )
                    })
                    .collect(),
            }),
        }
    }

    fn component() -> VolumesComponent {
        let linstor = |pvc: &str, oos: u64| LinstorVolume {
            resource: format!("pvc-{pvc}"),
            volume_number: 0,
            pvc: Some(format!("monitoring/{pvc}")),
            size_kib: Some(11 * 1024 * 1024),
            resource_group: None,
            place_count: Some(3),
            replicas: vec![
                replica("bc1", true, &["bc2", "bc4"], oos),
                replica("bc2", false, &["bc1", "bc4"], 0),
                replica("bc4", false, &["bc1", "bc2"], 0),
            ],
        };
        let gib = 1u64 << 30;
        let zfs = ZfsVolume {
            name: "pvc-mongo".to_string(),
            pvc: Some("db/data-mongo-0".to_string()),
            node: "bc2".to_string(),
            pool: "datapool/localpv".to_string(),
            capacity: Some(10 * gib),
            state: "Ready".to_string(),
            volume_type: "DATASET".to_string(),
            fs: Some((10 * gib, 9 * gib + gib / 2)),
        };
        let mut rows: Vec<VolumeRow> = [linstor("vmsingle", 924), linstor("grafana", 0)]
            .into_iter()
            .map(|v| VolumeRow {
                issues: volume_issues(&v),
                source: Source::Linstor(v),
            })
            .collect();
        rows.push(VolumeRow {
            issues: zfs_volume_issues(&zfs),
            source: Source::Zfs(zfs),
        });
        let mut c = VolumesComponent::new("bc".to_string(), None);
        c.state.set_data(VolumesData {
            installed: true,
            satellites: HashMap::new(),
            rows,
            replicas: 6,
            replicas_observed: 6,
            source_errors: Vec::new(),
        });
        c
    }

    fn render(c: &mut VolumesComponent) -> String {
        let mut terminal = Terminal::new(TestBackend::new(170, 30)).unwrap();
        terminal.draw(|f| c.draw(f, f.area()).unwrap()).unwrap();
        let buffer = terminal.backend().buffer();
        (0..buffer.area.height)
            .map(|y| {
                (0..buffer.area.width)
                    .map(|x| buffer[(x, y)].symbol())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn key(c: &mut VolumesComponent, code: KeyCode) {
        c.handle_key_event(KeyEvent::from(code)).unwrap();
    }

    #[test]
    fn shows_all_volumes_by_default_and_toggles_to_issues() {
        let mut c = component();
        let screen = render(&mut c);
        println!("{screen}");
        assert!(screen.contains("monitoring/vmsingle"));
        assert!(screen.contains("924 KiB out of sync"));
        assert!(screen.contains("monitoring/grafana"));
        // Bits between the Primary (bc1) and its peers: resync from bc1
        assert!(screen.contains("Suggested fix"));
        assert!(screen.contains("drbdadm invalidate-remote pvc-vmsingle:bc2"));
        // Usage and copies columns
        assert!(screen.contains("50%"));
        assert!(screen.contains("3/3"));

        key(&mut c, KeyCode::Char('a'));
        assert!(!render(&mut c).contains("monitoring/grafana"));
        key(&mut c, KeyCode::Char('a'));

        key(&mut c, KeyCode::Char('/'));
        for ch in "graf".chars() {
            key(&mut c, KeyCode::Char(ch));
        }
        let screen = render(&mut c);
        assert!(screen.contains("monitoring/grafana"));
        assert!(!screen.contains("monitoring/vmsingle"));
    }

    #[test]
    fn lists_zfs_volumes_and_filters_by_type() {
        let mut c = component();
        let screen = render(&mut c);
        // 95% full ZFS dataset: an error, listed with the LINSTOR volumes
        assert!(screen.contains("db/data-mongo-0"));
        assert!(screen.contains("bc2: filesystem 95% full"));
        assert!(screen.contains("1/1"));

        key(&mut c, KeyCode::Char('t'));
        let linstor_only = render(&mut c);
        assert!(!linstor_only.contains("db/data-mongo-0"));
        assert!(linstor_only.contains("monitoring/vmsingle"));

        key(&mut c, KeyCode::Char('t'));
        let zfs_only = render(&mut c);
        assert!(zfs_only.contains("db/data-mongo-0"));
        assert!(!zfs_only.contains("monitoring/vmsingle"));
        assert!(zfs_only.contains("local dataset, not replicated"));
    }

    #[test]
    fn restores_the_previous_visits_selection() {
        let mut first = component();
        key(&mut first, KeyCode::Char('j'));
        let remembered = first.view_state();
        let selected = first.visible()[1].key();
        assert_eq!(remembered.selected, Some(selected.clone()));

        // A new visit: the selection applies once the first scan lands
        let mut next = VolumesComponent::new("bc".to_string(), None);
        next.set_view_state(remembered);
        crate::components::BackgroundRefresh::adopt_refresh(&mut next, component());
        assert_eq!(next.visible()[next.selected].key(), selected);

        // A volume that no longer exists leaves the first row selected
        let mut gone = VolumesComponent::new("bc".to_string(), None);
        gone.set_view_state(VolumesViewState {
            show_all: true,
            selected: Some(("pvc-deleted".to_string(), 0)),
            ..Default::default()
        });
        crate::components::BackgroundRefresh::adopt_refresh(&mut gone, component());
        assert_eq!(gone.selected, 0);
    }
}

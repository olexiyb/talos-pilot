//! LINSTOR volumes view
//!
//! Every LINSTOR (Piraeus) volume of the cluster with its replicas, checked
//! against the DRBD state in each node's kernel (see `crate::linstor_volumes`).
//! Shows only volumes with issues by default, so a problem LINSTOR itself
//! doesn't report (a DRBD device smaller than its volume, blocks out of sync
//! on an "Established" link) stands out among dozens of healthy volumes.
//!
//! Like the workloads view it can be scoped to the selected node(s).

use crate::action::Action;
use crate::components::Component;
use crate::linstor_volumes::{
    Issue, LinstorVolume, Remedy, Replica, SatellitePod, Severity, fetch_satellites, fetch_volumes,
    format_kib, observe_drbd, remediation, volume_issues,
};
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

/// A volume with the issues found on it
#[derive(Debug, Clone)]
struct VolumeRow {
    volume: LinstorVolume,
    issues: Vec<Issue>,
}

impl VolumeRow {
    fn worst(&self) -> Option<Severity> {
        self.issues.iter().map(|i| i.severity).min()
    }

    fn key(&self) -> (String, u32) {
        (self.volume.resource.clone(), self.volume.volume_number)
    }

    fn label(&self) -> &str {
        self.volume.pvc.as_deref().unwrap_or(&self.volume.resource)
    }
}

/// Data loaded asynchronously for the view
#[derive(Debug, Clone, Default)]
struct VolumesData {
    /// `false` when the cluster has no LINSTOR controller
    installed: bool,
    /// Issues first (errors, then warnings), then by name
    rows: Vec<VolumeRow>,
    /// Replicas in scope, and those whose DRBD state could be read
    replicas: usize,
    replicas_observed: usize,
    /// Satellite pod per node, for the `kubectl exec` of fix suggestions
    satellites: HashMap<String, SatellitePod>,
}

/// What the view remembers between visits: the issues/all toggle, the text
/// filter and the selected volume (resource, volume number)
#[derive(Debug, Clone, Default)]
pub struct VolumesViewState {
    pub show_all: bool,
    pub filter: String,
    pub selected: Option<(String, u32)>,
}

/// LINSTOR volumes component
#[derive(Clone)]
pub struct LinstorVolumesComponent {
    state: AsyncState<VolumesData>,
    /// What the view covers, e.g. the cluster or node name
    title: String,
    /// Nodes the view is limited to (`None` for the whole cluster)
    scope: Option<HashSet<String>>,

    k8s_client: Option<Client>,
    /// Talos client and control plane node to get a Kubernetes client from
    k8s_source: Option<(TalosClient, Option<String>)>,
    /// Talos client and node name -> address, for reading DRBD state
    talos: Option<TalosClient>,
    node_ips: HashMap<String, String>,

    /// Show healthy volumes too
    show_all: bool,
    /// Text filter on volume, resource and node names
    filter: String,
    filter_editing: bool,

    selected: usize,
    table_state: TableState,
    /// Volume to select once the first scan lands (from a previous visit)
    pending_select: Option<(String, u32)>,
    /// When the selected volume's fix commands were copied, for feedback
    copied_at: Option<std::time::Instant>,
}

impl LinstorVolumesComponent {
    /// Volumes view titled `title`, limited to replicas on `nodes` when given
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
            filter: String::new(),
            filter_editing: false,
            selected: 0,
            table_state,
            pending_select: None,
            copied_at: None,
        }
    }

    /// Toggle, filter and selection to keep for the next visit
    pub fn view_state(&self) -> VolumesViewState {
        VolumesViewState {
            show_all: self.show_all,
            filter: self.filter.clone(),
            selected: self
                .visible()
                .get(self.selected)
                .map(|r| r.key())
                .or_else(|| self.pending_select.clone()),
        }
    }

    /// Restore a previous visit's toggle and filter; its selected volume is
    /// selected once the first scan lands, if it still exists
    pub fn set_view_state(&mut self, state: VolumesViewState) {
        self.show_all = state.show_all;
        self.filter = state.filter;
        self.pending_select = state.selected;
    }

    /// Talos client and node addresses for the DRBD checks, and where to get
    /// the Kubernetes client from (the kubeconfig of `cp_ip`)
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

    /// Scan LINSTOR and the nodes' DRBD state
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

        let (fetched, satellites) = tokio::join!(
            tokio::time::timeout(Duration::from_secs(20), fetch_volumes(client)),
            fetch_satellites(client),
        );
        let mut volumes = match fetched {
            Ok(Ok(Some(volumes))) => volumes,
            Ok(Ok(None)) => {
                self.state.set_data(VolumesData::default());
                return Ok(());
            }
            Ok(Err(e)) => {
                self.set_error(format!("Failed to query LINSTOR: {e}"));
                return Ok(());
            }
            Err(_) => {
                self.set_error("LINSTOR query timed out after 20s".to_string());
                return Ok(());
            }
        };

        let in_scope = |node: &str| self.scope.as_ref().is_none_or(|s| s.contains(node));
        if self.scope.is_some() {
            volumes.retain(|v| v.replicas.iter().any(|r| in_scope(&r.node)));
        }
        if let Some(talos) = &self.talos {
            let node_ips: HashMap<String, String> = self
                .node_ips
                .iter()
                .filter(|(node, _)| in_scope(node))
                .map(|(n, ip)| (n.clone(), ip.clone()))
                .collect();
            // Each read is bounded; this bounds the whole scan
            let _ = tokio::time::timeout(
                Duration::from_secs(60),
                observe_drbd(talos, &node_ips, &mut volumes),
            )
            .await;
        }

        // Only DRBD replicas have kernel state to read
        let scoped: Vec<&Replica> = volumes
            .iter()
            .flat_map(|v| &v.replicas)
            .filter(|r| r.drbd_layer && in_scope(&r.node))
            .collect();
        let replicas = scoped.len();
        let replicas_observed = scoped.iter().filter(|r| r.drbd.is_some()).count();

        let mut rows: Vec<VolumeRow> = volumes
            .into_iter()
            .map(|volume| VolumeRow {
                issues: volume_issues(&volume),
                volume,
            })
            .collect();
        // Worst first; `None` (healthy) sorts after both severities
        rows.sort_by(|a, b| {
            let rank = |r: &VolumeRow| r.worst().map_or(2, |s| s as u8);
            rank(a).cmp(&rank(b)).then_with(|| a.label().cmp(b.label()))
        });

        self.state.set_data(VolumesData {
            installed: true,
            satellites,
            rows,
            replicas,
            replicas_observed,
        });
        Ok(())
    }

    /// Rows shown under the current issues/all toggle and text filter
    fn visible(&self) -> Vec<&VolumeRow> {
        let Some(data) = self.state.data() else {
            return Vec::new();
        };
        let filter = self.filter.to_lowercase();
        data.rows
            .iter()
            .filter(|r| self.show_all || !r.issues.is_empty())
            .filter(|r| {
                filter.is_empty()
                    || r.label().to_lowercase().contains(&filter)
                    || r.volume.resource.to_lowercase().contains(&filter)
                    || r.volume
                        .replicas
                        .iter()
                        .any(|rep| rep.node.to_lowercase().contains(&filter))
            })
            .collect()
    }

    fn clamp_selection(&mut self) {
        let len = self.visible().len();
        self.selected = self.selected.min(len.saturating_sub(1));
        self.table_state.select(Some(self.selected));
    }

    fn draw_summary(&self, frame: &mut Frame, area: Rect, data: &VolumesData) {
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
            Span::raw(" volumes  "),
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
        let observed_style = if data.replicas_observed < data.replicas {
            Style::default().fg(Color::Yellow)
        } else {
            dim
        };
        spans.push(Span::styled(
            format!(
                "DRBD state read for {}/{} replicas",
                data.replicas_observed, data.replicas
            ),
            observed_style,
        ));
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
        let rows: Vec<Row> = visible
            .iter()
            .map(|r| {
                let (symbol, color) = match r.worst() {
                    Some(Severity::Error) => ("✗", Color::Red),
                    Some(Severity::Warning) => ("◐", Color::Yellow),
                    None => ("●", Color::Green),
                };
                let size = r.volume.size_kib.map(format_kib).unwrap_or("?".to_string());
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
                    Cell::from(Span::styled(size, dim)),
                    Cell::from(replicas_line(&r.volume.replicas)),
                    Cell::from(issue),
                ])
            })
            .collect();

        let replicas_width = visible
            .iter()
            .map(|r| replicas_line(&r.volume.replicas).width())
            .max()
            .unwrap_or(8)
            .max(8);
        let widths = [
            Constraint::Length(1),
            Constraint::Length(label_width as u16),
            Constraint::Length(10),
            Constraint::Length(replicas_width as u16),
            Constraint::Fill(1),
        ];
        let header = Row::new(vec!["", "Volume", "Size", "Replicas", "Issues"])
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
            let message = if self.show_all || !self.filter.is_empty() {
                "No volumes match"
            } else {
                "No volumes with issues ([a] shows all)"
            };
            let [_, message_area] =
                Layout::vertical([Constraint::Length(1), Constraint::Fill(1)]).areas(area);
            frame.render_widget(Paragraph::new(Span::styled(message, dim)), message_area);
        }
    }

    /// Replicas and their links of the selected volume, then its issues
    fn detail_lines(&self) -> Vec<Line<'static>> {
        let visible = self.visible();
        let Some(row) = visible.get(self.selected) else {
            return Vec::new();
        };
        let dim = Style::default().dim();
        let mut lines = vec![Line::from(vec![
            Span::styled(row.label().to_string(), Style::default().bold()),
            Span::styled(format!("  {}", row.volume.resource), dim),
            Span::styled(
                format!(
                    "  vol {}  LINSTOR size {}",
                    row.volume.volume_number,
                    row.volume
                        .size_kib
                        .map(format_kib)
                        .unwrap_or("?".to_string())
                ),
                dim,
            ),
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
        for r in &row.volume.replicas {
            if !r.drbd_layer {
                lines.push(Line::from(vec![
                    Span::raw(format!("  {:<10}", r.node)),
                    Span::styled("local volume, not replicated by DRBD", dim),
                ]));
                continue;
            }
            let role = if r.in_use { "Primary" } else { "Secondary" };
            let drbd_size = match r.drbd.as_ref().and_then(|d| d.size_kib) {
                Some(kib) => format!("DRBD {}", format_kib(kib)),
                None => "DRBD state unknown".to_string(),
            };
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

    /// Suggested fixes for the selected volume
    fn remedies(&self) -> Vec<Remedy> {
        let visible = self.visible();
        let (Some(data), Some(row)) = (self.state.data(), visible.get(self.selected)) else {
            return Vec::new();
        };
        remediation(&row.volume, &data.satellites)
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

/// Replicas as `bc1* bc2 bc4 bc3~`: `*` the Primary, `~` diskless, `?` when
/// the node's DRBD state couldn't be read; colored by disk state. A volume
/// without DRBD is marked `local`.
fn replicas_line(replicas: &[Replica]) -> Line<'static> {
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
        if r.drbd_layer && r.drbd.is_none() {
            spans.push(Span::styled("?", Style::default().dim()));
        }
    }
    if replicas.iter().all(|r| !r.drbd_layer) {
        spans.push(Span::styled(" local", Style::default().dim()));
    }
    Line::from(spans)
}

impl Component for LinstorVolumesComponent {
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
            .title(format!(" LINSTOR Volumes: {} ", self.title))
            .borders(Borders::ALL);
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let dim = Style::default().fg(Color::DarkGray);

        if self.state.is_loading() && !self.state.has_data() {
            frame.render_widget(
                Paragraph::new("Scanning LINSTOR volumes and DRBD state...").style(dim),
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
                Paragraph::new("LINSTOR not found: this cluster has no linstor-controller Service")
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

impl crate::components::BackgroundRefresh for LinstorVolumesComponent {
    fn adopt_refresh(&mut self, refreshed: Self) {
        // Keep the selected volume selected as rows reorder; on the first
        // scan, select the one from the previous visit
        let selected = match self.pending_select.take() {
            Some(key) => Some(key),
            None => self.visible().get(self.selected).map(|r| r.key()),
        };
        self.state = refreshed.state;
        if self.k8s_client.is_none() {
            self.k8s_client = refreshed.k8s_client;
        }
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

    fn component() -> LinstorVolumesComponent {
        let volume = |pvc: &str, oos: u64| LinstorVolume {
            resource: format!("pvc-{pvc}"),
            volume_number: 0,
            pvc: Some(format!("monitoring/{pvc}")),
            size_kib: Some(11 * 1024 * 1024),
            replicas: vec![
                replica("bc1", true, &["bc2", "bc4"], oos),
                replica("bc2", false, &["bc1", "bc4"], 0),
                replica("bc4", false, &["bc1", "bc2"], 0),
            ],
        };
        let rows: Vec<VolumeRow> = [volume("vmsingle", 924), volume("grafana", 0)]
            .into_iter()
            .map(|volume| VolumeRow {
                issues: volume_issues(&volume),
                volume,
            })
            .collect();
        let mut c = LinstorVolumesComponent::new("bc".to_string(), None);
        c.state.set_data(VolumesData {
            installed: true,
            satellites: HashMap::new(),
            rows,
            replicas: 6,
            replicas_observed: 6,
        });
        c
    }

    fn render(c: &mut LinstorVolumesComponent) -> String {
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

    #[test]
    fn shows_all_volumes_by_default_and_toggles_to_issues() {
        let mut c = component();
        let screen = render(&mut c);
        assert!(screen.contains("monitoring/vmsingle"));
        assert!(screen.contains("924 KiB out of sync"));
        assert!(screen.contains("monitoring/grafana"));
        // Bits between the Primary (bc1) and its peers: resync from bc1
        println!("{screen}");
        assert!(screen.contains("Suggested fix"));
        assert!(screen.contains("drbdadm invalidate-remote pvc-vmsingle:bc2"));

        c.handle_key_event(KeyEvent::from(KeyCode::Char('a')))
            .unwrap();
        assert!(!render(&mut c).contains("monitoring/grafana"));
        c.handle_key_event(KeyEvent::from(KeyCode::Char('a')))
            .unwrap();

        c.handle_key_event(KeyEvent::from(KeyCode::Char('/')))
            .unwrap();
        for ch in "graf".chars() {
            c.handle_key_event(KeyEvent::from(KeyCode::Char(ch)))
                .unwrap();
        }
        let screen = render(&mut c);
        assert!(screen.contains("monitoring/grafana"));
        assert!(!screen.contains("monitoring/vmsingle"));
    }

    #[test]
    fn restores_the_previous_visits_selection() {
        let mut first = component();
        first
            .handle_key_event(KeyEvent::from(KeyCode::Char('j')))
            .unwrap();
        let remembered = first.view_state();
        assert_eq!(remembered.selected, Some(("pvc-grafana".to_string(), 0)));

        // A new visit: the selection applies once the first scan lands
        let mut next = LinstorVolumesComponent::new("bc".to_string(), None);
        next.set_view_state(remembered);
        crate::components::BackgroundRefresh::adopt_refresh(&mut next, component());
        assert_eq!(next.selected, 1);

        // A volume that no longer exists leaves the first row selected
        let mut gone = LinstorVolumesComponent::new("bc".to_string(), None);
        gone.set_view_state(VolumesViewState {
            show_all: true,
            filter: String::new(),
            selected: Some(("pvc-deleted".to_string(), 0)),
        });
        crate::components::BackgroundRefresh::adopt_refresh(&mut gone, component());
        assert_eq!(gone.selected, 0);
    }
}

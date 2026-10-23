//! Etcd cluster status component
//!
//! Displays etcd cluster health with member list and details, and checks
//! each member's status for what etcd's own health doesn't flag yet:
//! a fragmented database (space a defrag would reclaim), entries committed
//! but not applied, a member far behind the leader, and a database close to
//! etcd's default quota.
//!
//! The selected member's etcd log streams live under the table.
//!
//! The addresses each member listens on come from its running etcd process
//! (its command line, via the Processes API); the node's EtcdSpec (via
//! `talosctl get etcdspec`: COSI is not reachable over the API) is what the
//! config asks for. Both matter while moving etcd to another network.

use crate::action::Action;
use crate::components::Component;
use crate::ui_ext::QuorumStateExt;
use color_eyre::Result;
use crossterm::event::{KeyCode, KeyEvent};
use k8s_openapi::serde_json::{self, Value};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState},
};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
use talos_pilot_core::{
    AsyncState, QuorumState, SelectableList, format_bytes_signed, format_talos_error,
};
use talos_rs::{EtcdAlarm, EtcdMemberInfo, EtcdMemberStatus, EtcdSpecInfo, TalosClient};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Combined etcd member data (from member list + status)
#[derive(Debug, Clone)]
pub struct EtcdMember {
    /// Member info from member list
    pub info: EtcdMemberInfo,
    /// Status from status call (None if unreachable)
    pub status: Option<EtcdMemberStatus>,
    /// Addresses the node's config asks for (EtcdSpec; None until fetched)
    pub spec: Option<EtcdSpecInfo>,
    /// Addresses the running etcd process uses (None until fetched)
    pub running: Option<EtcdFlags>,
}

/// Addresses (hosts) a running etcd was started with
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EtcdFlags {
    pub advertise_client: Vec<String>,
    pub listen_peer: Vec<String>,
    pub listen_client: Vec<String>,
}

impl EtcdFlags {
    /// From etcd's command line, e.g.
    /// `/usr/local/bin/etcd --listen-peer-urls=https://10.0.0.1:2380,...`
    fn parse(args: &str) -> Self {
        let mut flags = Self::default();
        for arg in args.split_whitespace() {
            let Some((name, value)) = arg.split_once('=') else {
                continue;
            };
            let target = match name {
                "--advertise-client-urls" => &mut flags.advertise_client,
                "--listen-peer-urls" => &mut flags.listen_peer,
                "--listen-client-urls" => &mut flags.listen_client,
                _ => continue,
            };
            *target = value.split(',').filter_map(url_host).collect();
        }
        flags
    }
}

/// Host of `https://10.0.0.1:2380` or `https://[fd00::1]:2380`
fn url_host(url: &str) -> Option<String> {
    let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
    let host = match rest.strip_prefix('[') {
        Some(v6) => v6.split(']').next()?,
        None => rest.split(':').next()?,
    };
    (!host.is_empty()).then(|| host.to_string())
}

/// EtcdSpecs and process lists change only with the machine config or a
/// restart, so they are fetched less often than the status
const ADDRESS_REFRESH: Duration = Duration::from_secs(60);

fn is_loopback(address: &str) -> bool {
    address
        .parse::<std::net::IpAddr>()
        .is_ok_and(|ip| ip.is_loopback())
}

/// Host of a member's first peer or client URL
fn first_host(urls: &[String]) -> Option<String> {
    url_host(urls.first()?)
}

/// Addresses without loopback, comma separated
fn address_list(addresses: &[String]) -> String {
    let list: Vec<&str> = addresses
        .iter()
        .filter(|a| !is_loopback(a))
        .map(String::as_str)
        .collect();
    if list.is_empty() {
        "-".to_string()
    } else {
        list.join(", ")
    }
}

fn same_set(a: &[String], b: &[String]) -> bool {
    a.iter().all(|x| b.contains(x)) && b.iter().all(|x| a.contains(x))
}

/// What is off with a member's addresses: etcd still running with addresses
/// its config no longer asks for (they apply when etcd restarts), or else a
/// member list that doesn't show the address the config advertises
fn address_check(member: &EtcdMember) -> Option<String> {
    let name = &member.info.hostname;
    let spec = member.spec.as_ref()?;
    if let Some(running) = &member.running {
        let mut differences = Vec::new();
        if !same_set(&running.advertise_client, &spec.advertised) {
            differences.push(format!(
                "advertises {} (config: {})",
                running.advertise_client.join(", "),
                spec.advertised.join(", ")
            ));
        }
        if !same_set(&running.listen_peer, &spec.listen_peer)
            || !same_set(&running.listen_client, &spec.listen_client)
        {
            differences.push(format!(
                "listens on peer {} / client {} (config: peer {} / client {})",
                address_list(&running.listen_peer),
                address_list(&running.listen_client),
                address_list(&spec.listen_peer),
                address_list(&spec.listen_client)
            ));
        }
        if !differences.is_empty() {
            return Some(format!(
                "{name}: etcd {}; the config applies when etcd restarts",
                differences.join(", ")
            ));
        }
    }
    let stale: Vec<String> = [
        ("peer", &member.info.peer_urls),
        ("client", &member.info.client_urls),
    ]
    .into_iter()
    .filter_map(|(kind, urls)| {
        let host = url_host(urls.first()?)?;
        (!spec.advertised.contains(&host)).then(|| format!("{kind} URL {host}"))
    })
    .collect();
    (!spec.advertised.is_empty() && !stale.is_empty()).then(|| {
        format!(
            "{name}: config advertises {} but the member list has {}",
            spec.advertised.join(", "),
            stale.join(" and ")
        )
    })
}

/// Default auto-refresh interval in seconds
const AUTO_REFRESH_INTERVAL_SECS: u64 = 5;

/// A defrag is worth it when at least this share of the DB file is free...
const DEFRAG_MIN_FREE_PCT: i64 = 50;
/// ...and that is at least this much space
const DEFRAG_MIN_FREE_BYTES: i64 = 200 << 20;
/// Committed but not applied entries worth a look; etcd itself rejects new
/// requests ("too many requests") past 5000
const APPLY_LAG_WARN: u64 = 1_000;
const APPLY_LAG_ERROR: u64 = 5_000;
/// Raft entries a member may trail the leader by. Statuses are read a few
/// milliseconds apart, so a busy cluster shows small gaps even when healthy.
const BEHIND_LEADER_WARN: u64 = 10_000;
/// etcd's default `quota-backend-bytes`; past it etcd raises NOSPACE and
/// stops accepting writes
const DEFAULT_QUOTA_BYTES: i64 = 2 << 30;
const QUOTA_WARN_PCT: i64 = 80;

/// Something to look at on a member
#[derive(Debug, Clone, PartialEq)]
pub struct EtcdCheck {
    pub error: bool,
    pub text: String,
}

/// Checks of every reachable member's status, errors first
pub fn member_checks(members: &[EtcdMember]) -> Vec<EtcdCheck> {
    let statuses: Vec<(&EtcdMember, &EtcdMemberStatus)> = members
        .iter()
        .filter_map(|m| Some((m, m.status.as_ref()?)))
        .collect();
    let leader_index = statuses
        .iter()
        .find(|(_, s)| s.is_leader())
        .map(|(_, s)| s.raft_index);
    let mut checks: Vec<EtcdCheck> = members
        .iter()
        .filter_map(address_check)
        .map(|text| EtcdCheck { error: false, text })
        .collect();
    for (member, status) in &statuses {
        let name = &member.info.hostname;
        let lag = status.raft_index.saturating_sub(status.raft_applied_index);
        if lag >= APPLY_LAG_WARN {
            checks.push(EtcdCheck {
                error: lag >= APPLY_LAG_ERROR,
                text: format!("{name}: {lag} entries committed but not applied (slow disk?)"),
            });
        }
        if let Some(leader) = leader_index {
            let behind = leader.saturating_sub(status.raft_index);
            if behind >= BEHIND_LEADER_WARN {
                checks.push(EtcdCheck {
                    error: false,
                    text: format!("{name}: {behind} entries behind the leader"),
                });
            }
        }
        if let Some(free) = defrag_reclaims(status) {
            checks.push(EtcdCheck {
                error: false,
                text: format!(
                    "{name}: defrag would reclaim {} of {} ({}% of the file unused)",
                    format_bytes_signed(free),
                    format_bytes_signed(status.db_size),
                    free * 100 / status.db_size
                ),
            });
        }
        if status.db_size * 100 >= DEFAULT_QUOTA_BYTES * QUOTA_WARN_PCT {
            checks.push(EtcdCheck {
                error: false,
                text: format!(
                    "{name}: DB {} is {}% of etcd's default 2 GiB quota (unless \
                     quota-backend-bytes is raised)",
                    format_bytes_signed(status.db_size),
                    status.db_size * 100 / DEFAULT_QUOTA_BYTES
                ),
            });
        }
    }
    checks.sort_by_key(|c| !c.error);
    checks
}

/// Bytes a defrag would give back, when it is worth one
fn defrag_reclaims(status: &EtcdMemberStatus) -> Option<i64> {
    let free = status.db_size - status.db_size_in_use;
    (status.db_size > 0
        && free >= DEFRAG_MIN_FREE_BYTES
        && free * 100 >= status.db_size * DEFRAG_MIN_FREE_PCT)
        .then_some(free)
}

/// `talosctl etcd defrag` for each member worth defragmenting, followers
/// first and the leader last: a defrag blocks the member while it runs, so
/// one at a time
pub fn defrag_commands(members: &[EtcdMember], context: Option<&str>) -> Vec<String> {
    let mut targets: Vec<(bool, String)> = members
        .iter()
        .filter_map(|m| {
            let status = m.status.as_ref()?;
            defrag_reclaims(status)?;
            Some((status.is_leader(), m.info.ip_address()?))
        })
        .collect();
    targets.sort();
    let context = context
        .map(|c| format!(" --context {c}"))
        .unwrap_or_default();
    targets
        .into_iter()
        .map(|(_, ip)| format!("talosctl{context} -n {ip} etcd defrag"))
        .collect()
}

/// Loaded etcd data (wrapped by AsyncState)
#[derive(Debug, Clone, Default)]
pub struct EtcdData {
    /// Combined member data with selection
    pub members: SelectableList<EtcdMember>,
    /// Alarms
    pub alarms: Vec<EtcdAlarm>,
    /// Quorum state
    pub quorum_state: QuorumState,
    /// Total DB size (from largest member)
    pub total_db_size: i64,
    /// Current revision (from leader)
    pub revision: u64,
    /// EtcdSpec and running etcd addresses per member IP, and when they
    /// were fetched
    pub specs: HashMap<String, EtcdSpecInfo>,
    pub running: HashMap<String, EtcdFlags>,
    pub addresses_at: Option<Instant>,
}

/// Log lines of the selected member fetched before following
const LOG_TAIL: i32 = 100;
/// Log lines kept for the log pane
const LOG_KEEP: usize = 500;

enum LogEvent {
    Line(String),
    Failed(String),
    Ended,
}

#[derive(Debug, Clone, Default, PartialEq)]
enum LogStatus {
    /// Stream open, no line yet: a quiet log is not a failure
    #[default]
    Waiting,
    Streaming,
    Failed(String),
    Ended,
}

/// The live etcd log of one member. Dropping it stops the stream.
#[derive(Default)]
struct MemberLog {
    member: String,
    lines: VecDeque<String>,
    status: LogStatus,
    rx: Option<mpsc::UnboundedReceiver<LogEvent>>,
    task: Option<JoinHandle<()>>,
}

/// A background refresh works on a clone, which needs no stream: the
/// component keeps its own
impl Clone for MemberLog {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl Drop for MemberLog {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl MemberLog {
    /// Take the lines and status the stream sent since the last call
    fn drain(&mut self) {
        let Some(rx) = &mut self.rx else {
            return;
        };
        while let Ok(event) = rx.try_recv() {
            match event {
                LogEvent::Line(line) => {
                    self.status = LogStatus::Streaming;
                    self.lines.push_back(line);
                    if self.lines.len() > LOG_KEEP {
                        self.lines.pop_front();
                    }
                }
                LogEvent::Failed(e) => self.status = LogStatus::Failed(e),
                LogEvent::Ended => self.status = LogStatus::Ended,
            }
        }
    }
}

/// An etcd log line (zap JSON): time, level and message, colored by level,
/// then its other fields. Lines that aren't JSON are shown as they are.
fn log_line(line: &str) -> Line<'static> {
    let Ok(Value::Object(mut fields)) = serde_json::from_str::<Value>(line) else {
        return Line::raw(line.to_string());
    };
    let text = |value: Value| match value {
        Value::String(s) => s,
        other => other.to_string(),
    };
    let mut take = |key: &str| fields.remove(key).map(text).unwrap_or_default();
    let level = take("level");
    let ts = take("ts");
    let msg = take("msg");
    fields.remove("caller");
    // 2026-10-20T14:52:47.096285Z -> 14:52:47
    let time = ts.get(11..19).unwrap_or(&ts).to_string();
    let color = match level.as_str() {
        "error" | "dpanic" | "panic" | "fatal" => Color::Red,
        "warn" => Color::Yellow,
        "debug" => Color::DarkGray,
        _ => Color::Reset,
    };
    let extra: Vec<String> = fields
        .into_iter()
        .map(|(key, value)| format!("{key}={}", text(value)))
        .collect();
    let dim = Style::default().add_modifier(Modifier::DIM);
    Line::from(vec![
        Span::styled(format!("{time} "), dim),
        Span::styled(format!("{level:<5} "), Style::default().fg(color)),
        Span::styled(msg, Style::default().fg(color)),
        Span::styled(format!("  {}", extra.join(" ")), dim),
    ])
}

/// "42s", "5m", "3h"
fn short_age(age: Duration) -> String {
    match age.as_secs() {
        s if s < 60 => format!("{s}s"),
        s if s < 3600 => format!("{}m", s / 60),
        s => format!("{}h", s / 3600),
    }
}

/// Members reporting errors, one line each
fn member_errors(members: &[EtcdMember]) -> Vec<String> {
    members
        .iter()
        .filter_map(|m| {
            let errors = &m.status.as_ref()?.errors;
            (!errors.is_empty()).then(|| format!("{}: {}", m.info.hostname, errors.join(", ")))
        })
        .collect()
}

/// Etcd cluster status component
#[derive(Clone)]
pub struct EtcdComponent {
    /// Async state wrapping all etcd data (loading, error, data)
    state: AsyncState<EtcdData>,

    /// Table state for rendering (synced with members selection)
    table_state: TableState,

    /// Auto-refresh enabled
    auto_refresh: bool,

    /// Client for API calls
    client: Option<TalosClient>,

    /// talosctl context of the cluster, for the copied defrag commands and
    /// the EtcdSpec queries
    context: Option<String>,

    /// talosconfig path given with `--config`
    config_path: Option<String>,

    /// Live etcd log of the selected member
    log: MemberLog,
}

impl Default for EtcdComponent {
    fn default() -> Self {
        Self::new()
    }
}

impl EtcdComponent {
    pub fn new() -> Self {
        let mut table_state = TableState::default();
        table_state.select(Some(0));

        Self {
            state: AsyncState::new(),
            table_state,
            auto_refresh: true,
            client: None,
            context: None,
            config_path: None,
            log: MemberLog::default(),
        }
    }

    /// Set the client for making API calls
    pub fn set_client(&mut self, client: TalosClient) {
        self.client = Some(client);
    }

    /// Whether a client was set (the cluster may still be reconnecting)
    pub fn has_client(&self) -> bool {
        self.client.is_some()
    }

    /// Name the talosctl context in copied commands
    pub fn set_context(&mut self, context: Option<String>) {
        self.context = context;
    }

    /// talosconfig to run talosctl with
    pub fn set_config_path(&mut self, config_path: Option<String>) {
        self.config_path = config_path;
    }

    /// Stream the selected member's etcd log, unless it already is
    fn follow_selected_log(&mut self) {
        let Some(client) = &self.client else {
            return;
        };
        let Some(member) = self.data().and_then(|d| d.members.selected()) else {
            return;
        };
        // A stream that failed or ended (e.g. its connection died) is
        // opened again on the next refresh
        let open = matches!(self.log.status, LogStatus::Waiting | LogStatus::Streaming);
        if self.log.member == member.info.hostname && open {
            return;
        }
        let name = member.info.hostname.clone();
        // By IP: hostnames may not resolve
        let Some(ip) = member.info.ip_address() else {
            self.log = MemberLog {
                member: name,
                lines: VecDeque::new(),
                status: LogStatus::Failed("no address to reach the member".to_string()),
                rx: None,
                task: None,
            };
            return;
        };
        let node = client.with_node(&ip);
        let (tx, rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            match node.logs_stream("etcd", LOG_TAIL).await {
                Ok(mut lines) => {
                    while let Some(line) = lines.recv().await {
                        if tx.send(LogEvent::Line(line)).is_err() {
                            return;
                        }
                    }
                    let _ = tx.send(LogEvent::Ended);
                }
                Err(e) => {
                    let _ = tx.send(LogEvent::Failed(format_talos_error(&e)));
                }
            }
        });
        // Replacing the previous log stops its stream
        self.log = MemberLog {
            member: name,
            lines: VecDeque::new(),
            status: LogStatus::Waiting,
            rx: Some(rx),
            task: Some(task),
        };
    }

    /// Copy the defrag commands, one per line, with a reminder to run them
    /// one at a time
    fn copy_defrag(&self) {
        let Some(data) = self.data() else {
            return;
        };
        let commands = defrag_commands(data.members.items(), self.context.as_deref());
        if commands.is_empty() {
            return;
        }
        let text = format!(
            "# One member at a time, followers first and the leader last;\n\
             # each blocks that member until it finishes\n{}",
            commands.join("\n")
        );
        if let Err(e) = crate::clipboard::copy_to_clipboard(text) {
            tracing::warn!("Failed to copy defrag commands: {}", e);
        }
    }

    /// Set an error message
    pub fn set_error(&mut self, error: String) {
        self.state.set_error(error);
    }

    /// Helper to get data reference (convenience method)
    fn data(&self) -> Option<&EtcdData> {
        self.state.data()
    }

    /// Refresh etcd data from the cluster
    pub async fn refresh(&mut self) -> Result<()> {
        let Some(client) = &self.client else {
            // Retried on the next refresh, which picks up the client once
            // the cluster has reconnected
            tracing::warn!("etcd: no connection to the cluster yet");
            self.state
                .set_error_with_retry("Not connected to the cluster — reconnecting");
            return Ok(());
        };

        self.state.start_loading();

        let timeout = Duration::from_secs(10);

        // Step 1: Fetch member list first (we need hostnames to target status calls)
        let members_result = tokio::time::timeout(timeout, client.etcd_members()).await;

        let member_infos = match members_result {
            Ok(Ok(members)) => members,
            Ok(Err(e)) => {
                let msg = format_talos_error(&e);
                tracing::warn!("etcd: failed to fetch members: {}", msg);
                self.state
                    .set_error_with_retry(format!("Failed to fetch members: {}", msg));
                return Ok(());
            }
            Err(_) => {
                tracing::warn!("etcd: request timed out after {}s", timeout.as_secs());
                self.state.set_error_with_retry(format!(
                    "Request timed out after {}s",
                    timeout.as_secs()
                ));
                return Ok(());
            }
        };

        // Step 2: Extract control plane IPs from members (hostnames may not be resolvable)
        let cp_ips: Vec<String> = member_infos.iter().filter_map(|m| m.ip_address()).collect();

        tracing::debug!("Fetching etcd status from control planes: {:?}", cp_ips);

        // Step 3: Fetch status (targeting all CPs by IP) and alarms in
        // parallel, and the members' addresses when due (each query has its
        // own timeout; a failed one keeps what was fetched before)
        let mut specs = self.data().map(|d| d.specs.clone()).unwrap_or_default();
        let mut running = self.data().map(|d| d.running.clone()).unwrap_or_default();
        let mut addresses_at = self.data().and_then(|d| d.addresses_at);
        let addresses_due = addresses_at.is_none_or(|at| at.elapsed() >= ADDRESS_REFRESH);
        let (fetch_result, fetched) = tokio::join!(
            tokio::time::timeout(timeout, async {
                tokio::join!(client.etcd_status_for_nodes(&cp_ips), client.etcd_alarms())
            }),
            async {
                if !addresses_due {
                    return Vec::new();
                }
                let context = self.context.as_deref();
                let config_path = self.config_path.as_deref();
                futures::future::join_all(cp_ips.iter().map(|ip| async move {
                    let node = client.with_node(ip);
                    let (spec, processes) = tokio::join!(
                        talos_rs::get_etcd_spec_for_node(context, ip, config_path),
                        tokio::time::timeout(timeout, node.processes())
                    );
                    (ip.clone(), spec, processes)
                }))
                .await
            }
        );
        if addresses_due {
            addresses_at = Some(Instant::now());
            for (ip, spec, processes) in fetched {
                match spec {
                    Ok(spec) => {
                        specs.insert(ip.clone(), spec);
                    }
                    Err(e) => tracing::warn!("Failed to fetch etcd spec of {}: {}", ip, e),
                }
                let etcd = match processes {
                    Ok(Ok(nodes)) => nodes
                        .into_iter()
                        .flat_map(|n| n.processes)
                        .find(|p| p.executable.ends_with("/etcd") || p.command == "etcd"),
                    Ok(Err(e)) => {
                        tracing::warn!("Failed to list processes of {}: {}", ip, e);
                        continue;
                    }
                    Err(_) => {
                        tracing::warn!("Listing processes of {} timed out", ip);
                        continue;
                    }
                };
                // No etcd process (stopped): nothing runs with any address
                match etcd {
                    Some(p) => running.insert(ip, EtcdFlags::parse(&p.args)),
                    None => running.remove(&ip),
                };
            }
        }

        let (status_result, alarms_result) = match fetch_result {
            Ok(results) => results,
            Err(_) => {
                tracing::warn!("etcd: request timed out after {}s", timeout.as_secs());
                self.state.set_error_with_retry(format!(
                    "Request timed out after {}s",
                    timeout.as_secs()
                ));
                return Ok(());
            }
        };

        // Process status - non-critical, just log warning
        let statuses = match status_result {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("Failed to fetch etcd status: {}", e);
                Vec::new()
            }
        };

        // Process alarms - non-critical
        let alarms = match alarms_result {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!("Failed to fetch etcd alarms: {}", e);
                Vec::new()
            }
        };

        // Combine member info with status
        let members: Vec<EtcdMember> = member_infos
            .into_iter()
            .map(|info| {
                let status = statuses.iter().find(|s| s.member_id == info.id).cloned();
                let ip = info.ip_address().unwrap_or_default();
                let spec = specs.get(&ip).cloned();
                let running = running.get(&ip).cloned();
                EtcdMember {
                    info,
                    status,
                    spec,
                    running,
                }
            })
            .collect();

        // Build EtcdData with calculated fields
        let mut data = if let Some(existing) = self.state.data_mut() {
            // Preserve selection state from existing data
            existing.members.update_items(members);
            std::mem::take(existing)
        } else {
            EtcdData {
                members: SelectableList::new(members),
                alarms: Vec::new(),
                quorum_state: QuorumState::Unknown,
                total_db_size: 0,
                revision: 0,
                specs: HashMap::new(),
                running: HashMap::new(),
                addresses_at: None,
            }
        };
        data.specs = specs;
        data.running = running;
        data.addresses_at = addresses_at;

        // Update alarms
        data.alarms = alarms;

        // Calculate quorum state
        let total = data.members.len();
        let healthy = data
            .members
            .items()
            .iter()
            .filter(|m| m.status.is_some())
            .count();
        data.quorum_state = QuorumState::from_counts(healthy, total);

        // Calculate totals
        data.total_db_size = data
            .members
            .items()
            .iter()
            .filter_map(|m| m.status.as_ref())
            .map(|s| s.db_size)
            .max()
            .unwrap_or(0);

        data.revision = data
            .members
            .items()
            .iter()
            .filter_map(|m| m.status.as_ref())
            .map(|s| s.raft_index)
            .max()
            .unwrap_or(0);

        tracing::info!("Loaded {} etcd members", data.members.len());

        // Sync table state with SelectableList
        self.table_state.select(Some(data.members.selected_index()));

        // Set the data (clears error, resets retry count, updates last_refresh)
        self.state.set_data(data);

        Ok(())
    }

    /// Navigate to previous member
    fn select_prev(&mut self) {
        if let Some(data) = self.state.data_mut() {
            data.members.select_prev_no_wrap();
            self.table_state.select(Some(data.members.selected_index()));
        }
        self.follow_selected_log();
    }

    /// Navigate to next member
    fn select_next(&mut self) {
        if let Some(data) = self.state.data_mut() {
            data.members.select_next_no_wrap();
            self.table_state.select(Some(data.members.selected_index()));
        }
        self.follow_selected_log();
    }

    /// Draw the status bar
    fn draw_status_bar(&self, frame: &mut Frame, area: Rect) {
        let Some(data) = self.data() else {
            return;
        };

        let (indicator, color) = data.quorum_state.indicator_with_color();
        let (state_text, _) = data.quorum_state.display_with_color();

        let member_count = match &data.quorum_state {
            QuorumState::Healthy => format!("{}/{}", data.members.len(), data.members.len()),
            QuorumState::Degraded { healthy, total } => format!("{}/{}", healthy, total),
            QuorumState::NoQuorum { healthy, total } => format!("{}/{}", healthy, total),
            QuorumState::Unknown => "?/?".to_string(),
        };

        let db_size = format_bytes_signed(data.total_db_size);

        let line = Line::from(vec![
            Span::styled(format!("{} ", indicator), Style::default().fg(color)),
            Span::styled(
                state_text,
                Style::default().fg(color).add_modifier(Modifier::BOLD),
            ),
            Span::raw(format!("  {} members    ", member_count)),
            Span::raw(format!(
                "Quorum: {} ",
                if matches!(data.quorum_state, QuorumState::NoQuorum { .. }) {
                    "No"
                } else {
                    "Yes"
                }
            )),
            Span::raw(format!("   DB: {}    Rev: {}", db_size, data.revision)),
        ]);
        let mut line = line;
        // The same for every member, unless an election is running
        let term = data
            .members
            .items()
            .iter()
            .filter_map(|m| m.status.as_ref())
            .find(|s| s.is_leader())
            .map(|s| s.raft_term);
        if let Some(term) = term {
            line.spans.push(Span::raw(format!("    Term: {term}")));
        }
        // A failed refresh over data shown: say so, or the screen just
        // freezes on old numbers
        if let Some(error) = self.state.error() {
            let age = self
                .state
                .elapsed_since_refresh()
                .map(|d| format!(" — data from {} ago", short_age(d)))
                .unwrap_or_default();
            line.spans.push(Span::styled(
                format!("    ⚠ {error}{age}"),
                Style::default().fg(Color::Red),
            ));
        }

        let para = Paragraph::new(line).block(Block::default().borders(Borders::BOTTOM));
        frame.render_widget(para, area);
    }

    /// Draw the member table
    fn draw_member_table(&mut self, frame: &mut Frame, area: Rect) {
        let Some(data) = self.data() else {
            return;
        };

        let rows: Vec<Row> = data
            .members
            .items()
            .iter()
            .map(|member| {
                let (status_indicator, status_text, status_color) = match &member.status {
                    Some(s) if s.is_leader() => ("*", "Leader", Color::Magenta),
                    Some(s) if s.is_learner || member.info.is_learner => {
                        ("~", "Learner", Color::Yellow)
                    }
                    Some(_) => ("o", "Follow", Color::Green),
                    None => ("x", "DOWN", Color::Red),
                };

                let db_size = member
                    .status
                    .as_ref()
                    .map(|s| format_bytes_signed(s.db_size))
                    .unwrap_or_else(|| "-".to_string());

                let raft_idx = member
                    .status
                    .as_ref()
                    .map(|s| s.raft_index.to_string())
                    .unwrap_or_else(|| "-".to_string());

                // Share of the DB file in use; low means a defrag reclaims space
                let in_use = match &member.status {
                    Some(s) if s.db_size > 0 => {
                        let pct = s.db_size_in_use * 100 / s.db_size;
                        let color = if defrag_reclaims(s).is_some() {
                            Color::Yellow
                        } else {
                            Color::Reset
                        };
                        Cell::from(format!("{pct}%")).style(Style::default().fg(color))
                    }
                    _ => Cell::from("-"),
                };
                // Entries committed but not applied yet
                let lag = match &member.status {
                    Some(s) => {
                        let lag = s.raft_index.saturating_sub(s.raft_applied_index);
                        let color = if lag >= APPLY_LAG_ERROR {
                            Color::Red
                        } else if lag >= APPLY_LAG_WARN {
                            Color::Yellow
                        } else {
                            Color::Reset
                        };
                        Cell::from(lag.to_string()).style(Style::default().fg(color))
                    }
                    None => Cell::from("-"),
                };

                let errors = member
                    .status
                    .as_ref()
                    .map(|s| {
                        if s.errors.is_empty() {
                            "-".to_string()
                        } else {
                            s.errors.len().to_string()
                        }
                    })
                    .unwrap_or_else(|| "-".to_string());

                // Member list URLs; yellow when the config advertises
                // another address (mid-move)
                let url_cell = |urls: &[String]| {
                    let host = first_host(urls);
                    let stale = matches!(
                        (&host, &member.spec),
                        (Some(h), Some(spec)) if !spec.advertised.is_empty() && !spec.advertised.contains(h)
                    );
                    Cell::from(host.unwrap_or_else(|| "-".to_string())).style(
                        Style::default().fg(if stale { Color::Yellow } else { Color::Reset }),
                    )
                };
                let peer_url = url_cell(&member.info.peer_urls);
                let client_url = url_cell(&member.info.client_urls);

                let status_full = format!("{} {}", status_indicator, status_text);

                // The running process, else (dim) what the config asks for
                let (listen_peer, listen_client) = match (&member.running, &member.spec) {
                    (Some(r), _) => (
                        Cell::from(address_list(&r.listen_peer)),
                        Cell::from(address_list(&r.listen_client)),
                    ),
                    (None, Some(s)) => {
                        let dim = Style::default().add_modifier(Modifier::DIM);
                        (
                            Cell::from(address_list(&s.listen_peer)).style(dim),
                            Cell::from(address_list(&s.listen_client)).style(dim),
                        )
                    }
                    (None, None) => {
                        let unknown = || {
                            Cell::from("?").style(Style::default().add_modifier(Modifier::DIM))
                        };
                        (unknown(), unknown())
                    }
                };

                Row::new(vec![
                    Cell::from(member.info.hostname.clone()),
                    // What `talosctl etcd remove-member` takes
                    Cell::from(format!("{:x}", member.info.id))
                        .style(Style::default().add_modifier(Modifier::DIM)),
                    Cell::from(status_full).style(Style::default().fg(status_color)),
                    peer_url,
                    client_url,
                    listen_peer,
                    listen_client,
                    Cell::from(db_size),
                    in_use,
                    Cell::from(raft_idx),
                    lag,
                    Cell::from(errors),
                ])
            })
            .collect();

        let header = Row::new(vec![
            Cell::from("MEMBER"),
            Cell::from("ID"),
            Cell::from("STATUS"),
            Cell::from("PEER URL"),
            Cell::from("CLIENT URL"),
            Cell::from("LISTEN PEER"),
            Cell::from("LISTEN CLIENT"),
            Cell::from("DB SIZE"),
            Cell::from("IN USE"),
            Cell::from("RAFT IDX"),
            Cell::from("LAG"),
            Cell::from("ERRORS"),
        ])
        .style(Style::default().add_modifier(Modifier::DIM))
        .bottom_margin(1);

        // Use proportional column widths to fill available space
        // Addresses share what the fixed columns leave, listen lists
        // (often two addresses while moving) getting more
        let widths = [
            Constraint::Min(6),     // MEMBER
            Constraint::Length(17), // ID
            Constraint::Length(10), // STATUS
            Constraint::Fill(2),    // PEER URL
            Constraint::Fill(2),    // CLIENT URL
            Constraint::Fill(3),    // LISTEN PEER
            Constraint::Fill(3),    // LISTEN CLIENT
            Constraint::Length(10), // DB SIZE
            Constraint::Length(7),  // IN USE
            Constraint::Length(10), // RAFT IDX
            Constraint::Length(6),  // LAG
            Constraint::Length(6),  // ERRORS
        ];

        let table = Table::new(rows, widths)
            .header(header)
            .row_highlight_style(
                Style::default()
                    .bg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
            )
            .highlight_symbol("> ");

        frame.render_stateful_widget(table, area, &mut self.table_state);
    }

    /// Draw the selected member's live etcd log, newest at the bottom
    fn draw_log(&self, frame: &mut Frame, area: Rect) {
        let title = if self.log.member.is_empty() {
            " etcd log ".to_string()
        } else {
            format!(" etcd log · {} ", self.log.member)
        };
        let block = Block::default().title(title).borders(Borders::TOP);
        let height = block.inner(area).height as usize;

        let status = match &self.log.status {
            LogStatus::Waiting if self.log.member.is_empty() => None,
            LogStatus::Waiting => Some(Line::styled(
                "Waiting for log lines…",
                Style::default().fg(Color::DarkGray),
            )),
            LogStatus::Streaming => None,
            LogStatus::Failed(e) => Some(Line::styled(
                format!("Log stream failed: {e}"),
                Style::default().fg(Color::Red),
            )),
            LogStatus::Ended => Some(Line::styled(
                "Log stream ended",
                Style::default().fg(Color::DarkGray),
            )),
        };
        let room = height.saturating_sub(usize::from(status.is_some()));
        let skip = self.log.lines.len().saturating_sub(room);
        let mut lines: Vec<Line> = self
            .log
            .lines
            .iter()
            .skip(skip)
            .map(|l| log_line(l))
            .collect();
        lines.extend(status);

        frame.render_widget(Paragraph::new(lines).block(block), area);
    }

    /// Draw the alarms and member checks
    fn draw_alarms(&self, frame: &mut Frame, area: Rect) {
        let Some(data) = self.data() else {
            return;
        };

        let mut lines = Vec::new();
        if data.alarms.is_empty() {
            lines.push(Line::from(vec![
                Span::raw("Alarms: "),
                Span::styled("None", Style::default().fg(Color::Green)),
            ]));
        } else {
            let alarm_text: Vec<Span> = data
                .alarms
                .iter()
                .flat_map(|a| {
                    vec![
                        Span::styled("⚠ ", Style::default().fg(Color::Yellow)),
                        Span::styled(
                            format!("{}: {} ", a.alarm_type.as_str(), a.node),
                            Style::default().fg(Color::Yellow),
                        ),
                    ]
                })
                .collect();
            lines.push(Line::from(alarm_text));
        }
        let checks = member_checks(data.members.items());
        if checks.is_empty() {
            lines.push(Line::from(vec![
                Span::raw("Checks: "),
                Span::styled(
                    "OK (fragmentation, apply lag, quota)",
                    Style::default().fg(Color::Green),
                ),
            ]));
        }
        for errors in member_errors(data.members.items()) {
            lines.push(Line::from(vec![
                Span::styled("✗ ", Style::default().fg(Color::Red)),
                Span::styled(errors, Style::default().fg(Color::Red)),
            ]));
        }
        for check in checks {
            let (symbol, color) = if check.error {
                ("✗ ", Color::Red)
            } else {
                ("⚠ ", Color::Yellow)
            };
            lines.push(Line::from(vec![
                Span::styled(symbol, Style::default().fg(color)),
                Span::styled(check.text, Style::default().fg(color)),
            ]));
        }

        let para =
            Paragraph::new(lines).block(Block::default().borders(Borders::TOP | Borders::BOTTOM));
        frame.render_widget(para, area);
    }

    /// Draw the footer with keybindings
    fn draw_footer(&self, frame: &mut Frame, area: Rect) {
        let auto_status = if self.auto_refresh {
            Span::styled("ON ", Style::default().fg(Color::Green))
        } else {
            Span::styled("OFF", Style::default().fg(Color::DarkGray))
        };

        let line = Line::from(vec![
            Span::styled("[j/k]", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(" select  "),
            Span::styled("[r]", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(" refresh  "),
            Span::styled("[c]", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(" copy defrag  "),
            Span::styled("[a]", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(" auto:"),
            auto_status,
            Span::raw("  "),
            Span::styled("[q]", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(" back"),
        ]);

        let para = Paragraph::new(line);
        frame.render_widget(para, area);
    }
}

impl Component for EtcdComponent {
    fn handle_key_event(&mut self, key: KeyEvent) -> Result<Option<Action>> {
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => Ok(Some(Action::Back)),
            KeyCode::Char('j') | KeyCode::Down => {
                self.select_next();
                Ok(None)
            }
            KeyCode::Char('k') | KeyCode::Up => {
                self.select_prev();
                Ok(None)
            }
            KeyCode::Char('r') => Ok(Some(Action::Refresh)),
            KeyCode::Char('c') => {
                self.copy_defrag();
                Ok(None)
            }
            KeyCode::Char('a') => {
                // Toggle auto-refresh
                self.auto_refresh = !self.auto_refresh;
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    fn update(&mut self, action: Action) -> Result<Option<Action>> {
        if let Action::Tick = action {
            self.log.drain();
            // Check for auto-refresh using AsyncState helper
            let interval = Duration::from_secs(AUTO_REFRESH_INTERVAL_SECS);
            if self.state.should_auto_refresh(self.auto_refresh, interval) {
                return Ok(Some(Action::Refresh));
            }
        }
        Ok(None)
    }

    fn draw(&mut self, frame: &mut Frame, area: Rect) -> Result<()> {
        // Show loading state (only if no data yet)
        if self.state.is_loading() && !self.state.has_data() {
            let loading = Paragraph::new("Loading...").style(Style::default().fg(Color::DarkGray));
            frame.render_widget(loading, area);
            return Ok(());
        }

        // Show error state (only if no data to display)
        if let Some(err) = self.state.error()
            && !self.state.has_data()
        {
            let error =
                Paragraph::new(format!("Error: {}", err)).style(Style::default().fg(Color::Red));
            frame.render_widget(error, area);
            return Ok(());
        }

        // Get data for layout calculations; before the first refresh has
        // started there is none yet
        let Some(data) = self.data() else {
            let loading = Paragraph::new("Loading...").style(Style::default().fg(Color::DarkGray));
            frame.render_widget(loading, area);
            return Ok(());
        };

        let table_height = (data.members.len().max(1) + 2) as u16; // header + members + padding
        // Alarms line, member errors, then one line per check (or "Checks:
        // OK"), plus borders
        let alarm_height = (1
            + member_errors(data.members.items()).len()
            + member_checks(data.members.items()).len().max(1)
            + 2) as u16;

        let [status, table, alarms, log, footer] = Layout::vertical([
            Constraint::Length(2),
            Constraint::Length(table_height),
            Constraint::Length(alarm_height),
            Constraint::Min(3),
            Constraint::Length(1),
        ])
        .areas(area);

        self.draw_status_bar(frame, status);
        self.draw_member_table(frame, table);
        self.draw_alarms(frame, alarms);
        self.draw_log(frame, log);
        self.draw_footer(frame, footer);

        Ok(())
    }
}

impl crate::components::BackgroundRefresh for EtcdComponent {
    fn adopt_refresh(&mut self, refreshed: Self) {
        // Keep the member the user moved to while the refresh ran
        let selected = self.data().map(|d| d.members.selected_index());
        self.state = refreshed.state;
        if let (Some(index), Some(data)) = (selected, self.state.data_mut()) {
            data.members.select(index);
        }
        let index = self.data().map(|d| d.members.selected_index()).unwrap_or(0);
        self.table_state.select(Some(index));
        self.follow_selected_log();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: i64 = 1 << 20;

    fn member(
        name: &str,
        id: u64,
        leader: u64,
        db: i64,
        in_use: i64,
        raft: (u64, u64),
    ) -> EtcdMember {
        EtcdMember {
            info: EtcdMemberInfo {
                id,
                hostname: name.to_string(),
                peer_urls: vec![format!("https://10.0.0.{id}:2380")],
                client_urls: Vec::new(),
                is_learner: false,
            },
            status: Some(EtcdMemberStatus {
                node: name.to_string(),
                member_id: id,
                protocol_version: "3.5".to_string(),
                db_size: db,
                db_size_in_use: in_use,
                leader_id: leader,
                raft_index: raft.0,
                raft_term: 7,
                raft_applied_index: raft.1,
                errors: Vec::new(),
                is_learner: false,
            }),
            spec: None,
            running: None,
        }
    }

    fn hosts(a: &[&str]) -> Vec<String> {
        a.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn reads_addresses_from_the_etcd_command_line() {
        let flags = EtcdFlags::parse(
            "/usr/local/bin/etcd --advertise-client-urls=https://100.100.0.6:2379 \
             --listen-client-urls=https://127.0.0.1:2379,https://100.100.0.6:2379,https://116.202.55.89:2379 \
             --listen-client-http-urls=https://127.0.0.1:2383 \
             --listen-peer-urls=https://100.100.0.6:2380,https://[fd00::6]:2380 --name=kpc3",
        );
        assert_eq!(flags.advertise_client, ["100.100.0.6"]);
        assert_eq!(
            flags.listen_client,
            ["127.0.0.1", "100.100.0.6", "116.202.55.89"]
        );
        assert_eq!(flags.listen_peer, ["100.100.0.6", "fd00::6"]);
    }

    #[test]
    fn lists_addresses_without_loopback() {
        let client = hosts(&["127.0.0.1", "100.100.0.6", "116.202.55.89"]);
        assert_eq!(address_list(&client), "100.100.0.6, 116.202.55.89");
        assert_eq!(address_list(&hosts(&["127.0.0.1"])), "-");
        assert_eq!(
            first_host(&hosts(&["https://116.202.55.89:2380"])).as_deref(),
            Some("116.202.55.89")
        );
    }

    #[test]
    fn checks_addresses_against_the_config() {
        // kpc3: config moved to the public address, peer URL already
        // updated, etcd not restarted yet
        let mut m = member("kpc3", 6, 6, 0, 0, (1, 1));
        m.info.peer_urls = vec!["https://116.202.55.89:2380".to_string()];
        m.info.client_urls = vec!["https://10.0.0.6:2379".to_string()];
        let both = hosts(&["116.202.55.89", "10.0.0.6"]);
        m.spec = Some(EtcdSpecInfo {
            advertised: hosts(&["116.202.55.89"]),
            listen_peer: both.clone(),
            listen_client: [hosts(&["127.0.0.1"]), both.clone()].concat(),
        });
        // Not fetched yet: nothing to say
        assert_eq!(address_check(&member("kpc1", 1, 1, 0, 0, (1, 1))), None);

        m.running = Some(EtcdFlags {
            advertise_client: hosts(&["10.0.0.6"]),
            listen_peer: hosts(&["10.0.0.6", "116.202.55.89"]),
            listen_client: hosts(&["127.0.0.1", "10.0.0.6", "116.202.55.89"]),
        });
        assert_eq!(
            address_check(&m).as_deref(),
            Some(
                "kpc3: etcd advertises 10.0.0.6 (config: 116.202.55.89); the config \
                 applies when etcd restarts"
            )
        );

        // Restarted with the config, but the member list kept the old
        // client URL
        m.running = Some(EtcdFlags {
            advertise_client: hosts(&["116.202.55.89"]),
            listen_peer: both.clone(),
            listen_client: [hosts(&["127.0.0.1"]), both].concat(),
        });
        assert_eq!(
            address_check(&m).as_deref(),
            Some(
                "kpc3: config advertises 116.202.55.89 but the member list has client URL 10.0.0.6"
            )
        );
        assert!(member_checks(&[m.clone()]).iter().any(|c| !c.error));

        // Done
        m.info.client_urls = vec!["https://116.202.55.89:2379".to_string()];
        assert_eq!(address_check(&m), None);
    }

    #[test]
    fn formats_etcd_json_log_lines() {
        let line = log_line(
            r#"{"level":"warn","ts":"2026-10-20T14:53:47.097060Z","caller":"etcdserver/corrupt.go:278","msg":"slow apply","took":"1.2s"}"#,
        );
        let text: String = line.spans.iter().map(|s| s.content.as_ref()).collect();
        assert_eq!(text, "14:53:47 warn  slow apply  took=1.2s");
        assert_eq!(line.spans[2].style.fg, Some(Color::Yellow));
        // Not JSON: as it is
        let raw = log_line("plain text");
        assert_eq!(raw.spans[0].content, "plain text");
    }

    #[tokio::test]
    async fn log_keeps_the_newest_lines() {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut log = MemberLog::default();
        log.rx = Some(rx);
        assert_eq!(log.status, LogStatus::Waiting);
        for i in 0..LOG_KEEP + 5 {
            tx.send(LogEvent::Line(i.to_string())).unwrap();
        }
        log.drain();
        assert_eq!(log.status, LogStatus::Streaming);
        assert_eq!(log.lines.len(), LOG_KEEP);
        assert_eq!(log.lines.front().map(String::as_str), Some("5"));
        tx.send(LogEvent::Ended).unwrap();
        log.drain();
        assert_eq!(log.status, LogStatus::Ended);
    }

    #[test]
    fn lists_member_errors() {
        let mut members = vec![
            member("bc1", 1, 1, 100, 100, (5, 5)),
            member("bc2", 2, 1, 100, 100, (5, 5)),
        ];
        members[1].status.as_mut().unwrap().errors = vec!["NOSPACE".to_string()];
        assert_eq!(member_errors(&members), ["bc2: NOSPACE"]);
    }

    /// `TALOS_PILOT_LIVE_CONTEXT=bc cargo test --lib live_etcd_checks -- --ignored --nocapture`
    #[tokio::test]
    #[ignore]
    async fn live_etcd_checks() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let context =
            std::env::var("TALOS_PILOT_LIVE_CONTEXT").expect("set TALOS_PILOT_LIVE_CONTEXT");
        let mut etcd = EtcdComponent::new();
        etcd.set_client(TalosClient::from_named_context(&context).await.unwrap());
        etcd.set_context(Some(context.clone()));
        etcd.refresh().await.unwrap();
        let data = etcd.data().expect("no etcd data");
        for m in data.members.items() {
            match &m.status {
                Some(s) => println!(
                    "{}: db {} in use {} raft {} applied {} leader {}",
                    m.info.hostname,
                    format_bytes_signed(s.db_size),
                    format_bytes_signed(s.db_size_in_use),
                    s.raft_index,
                    s.raft_applied_index,
                    s.is_leader()
                ),
                None => println!("{}: unreachable", m.info.hostname),
            }
        }
        for c in member_checks(data.members.items()) {
            println!("check (error={}): {}", c.error, c.text);
        }
        for cmd in defrag_commands(data.members.items(), Some(&context)) {
            println!("cmd: {cmd}");
        }
    }

    #[test]
    fn healthy_members_have_no_checks() {
        let members = [
            member("cp1", 1, 1, 120 * MIB, 100 * MIB, (5_000, 4_998)),
            member("cp2", 2, 1, 120 * MIB, 100 * MIB, (4_990, 4_990)),
            // Mostly free, but too small to be worth a defrag
            member("cp3", 3, 1, 60 * MIB, 10 * MIB, (5_000, 5_000)),
        ];
        assert_eq!(member_checks(&members), Vec::new());
        assert!(defrag_commands(&members, Some("bc")).is_empty());
    }

    #[test]
    fn flags_fragmentation_lag_and_quota_and_orders_defrag() {
        let members = [
            member("cp1", 1, 1, 900 * MIB, 300 * MIB, (100_000, 100_000)),
            member("cp2", 2, 1, 900 * MIB, 400 * MIB, (100_000, 93_000)),
            member("cp3", 3, 1, 1700 * MIB, 1600 * MIB, (85_000, 85_000)),
        ];
        let texts: Vec<(bool, String)> = member_checks(&members)
            .into_iter()
            .map(|c| (c.error, c.text))
            .collect();
        assert_eq!(
            texts[0],
            (
                true,
                "cp2: 7000 entries committed but not applied (slow disk?)".to_string()
            )
        );
        assert!(
            texts
                .iter()
                .any(|(e, t)| !e && t.starts_with("cp1: defrag would reclaim"))
        );
        assert!(
            texts
                .iter()
                .any(|(_, t)| t == "cp3: 15000 entries behind the leader")
        );
        assert!(
            texts
                .iter()
                .any(|(_, t)| t.starts_with("cp3: DB") && t.contains("83%"))
        );
        assert!(!texts.iter().any(|(_, t)| t.starts_with("cp3: defrag")));

        // Followers first, the leader last
        assert_eq!(
            defrag_commands(&members, Some("bc")),
            [
                "talosctl --context bc -n 10.0.0.2 etcd defrag",
                "talosctl --context bc -n 10.0.0.1 etcd defrag",
            ]
        );
    }
}

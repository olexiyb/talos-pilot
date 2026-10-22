# talos-pilot

A terminal UI (TUI) for managing and monitoring [Talos Linux](https://www.talos.dev/) Kubernetes clusters.

**talos-pilot** provides real-time cluster visibility, diagnostics, log streaming, network analysis, and production-ready node operations - all from your terminal.

[![CI](https://github.com/Handfish/talos-pilot/actions/workflows/ci.yml/badge.svg)](https://github.com/Handfish/talos-pilot/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/Handfish/talos-pilot)](https://github.com/Handfish/talos-pilot/releases)
![Rust](https://img.shields.io/badge/rust-2024%20edition-orange)
![License](https://img.shields.io/badge/license-MIT-blue)

https://github.com/user-attachments/assets/4c946c32-1f7e-4ab8-9d88-9937516015d1

*Live cluster overview, interleaved log streaming, diagnostics, and safe node operations, all from the terminal.*

## Contents

- [Why talos-pilot?](#why-talos-pilot) · [Relationship to k9s](#relationship-to-k9s)
- [Features](#features)
- [Design Philosophy](#design-philosophy)
- [Engineering Highlights](#engineering-highlights)
- [Installation](#installation) · [Usage](#usage) · [Keyboard Navigation](#keyboard-navigation)
- [Architecture](#architecture) · [Development](#development)

## Why talos-pilot?

Talos Linux removes SSH access for security, replacing it with an API-driven management model. While `talosctl` is powerful, it requires memorizing many subcommands. **talos-pilot** provides:

- **Interactive cluster overview** - See all nodes, services, and health at a glance
- **Real-time monitoring** - CPU, memory, network stats with auto-refresh
- **Unified log viewer** - Stream logs from multiple services simultaneously (Stern-style), on one node or across a cluster's nodes, with the kernel log (dmesg)
- **Production operations** - Drain, reboot, rolling upgrades with safety checks
- **Diagnostics** - Automated health checks with actionable fix suggestions

### Relationship to k9s

**talos-pilot is complementary to k9s, not a replacement.** They operate at different layers:

| Tool | Layer | API Port | Use Case |
|------|-------|----------|----------|
| **k9s** | Kubernetes | `:6443` | Pods, deployments, services, workload debugging |
| **talos-pilot** | Operating System | `:50000` | Talos services, etcd, kubelet, node health, OS config |

Use **k9s** for "why won't my pod start?"
Use **talos-pilot** for "why won't my node join the cluster?"

## Features

### Cluster Management

| Feature | Description |
|---------|-------------|
| **Cluster Overview** | Multi-cluster monitoring, node list with health indicators |
| **Node Details** | CPU, memory, load averages, Talos/K8s versions |
| **Service Status** | All Talos services with health indicators |

### Monitoring

| Feature | Description |
|---------|-------------|
| **Service Logs** | Scrollable, searchable (`/`), color-coded by level |
| **Multi-Service Logs** | Stern-style interleaved logs from multiple services |
| **Cluster Logs** | `l` on a cluster, control plane or workers row: the same services from every node, merged by time with a node column; `o` filters nodes and shows streams that failed. The `kernel` service streams dmesg |
| **Processes View** | htop-like process list with tree view, CPU/MEM sorting |
| **Network Stats** | Interface traffic, connections, KubeSpan peers, packet capture |
| **Storage/Disks** | Disk list with size, transport, serial, system disk indicators |
| **etcd Status** | Quorum health, member list, alarms, leader tracking, peer/client and listen addresses (running vs config), live etcd log of the selected member |
| **Workload Health** | K8s deployments, statefulsets, pod issues by namespace |
| **Lifecycle View** | Version status, config drift detection, cluster alerts |

### Diagnostics & Security

| Feature | Description |
|---------|-------------|
| **System Diagnostics** | Automated health checks with actionable fixes |
| **CNI Detection** | Flannel, Cilium, Calico with provider-specific checks |
| **Addon Detection** | cert-manager, ArgoCD, Flux, and more |
| **Security Audit** | PKI certificate expiry, encryption status |

### Operations

| Feature | Description |
|---------|-------------|
| **Node Drain** | PDB-aware with configurable timeouts |
| **Node Reboot** | Post-reboot verification, auto-uncordon |
| **Rolling Operations** | Sequential multi-node with progress tracking |
| **Audit Logging** | All operations logged to `~/.talos-pilot/audit.log` |

## Design Philosophy

talos-pilot favors reliability in how it reports cluster health:

- State over logs: health is read from system state (procfs files, Talos and Kubernetes API responses), not from log lines. A stale error in an old log does not trigger a false alarm.
- Reliability hierarchy: checks prefer file and procfs state first, then API responses, then log parsing. Where a file like `/run/flannel/subnet.env` exists, it is read directly.
- No false positives: when a data source is unavailable, a check reports `unknown` rather than guessing or crashing.
- Separation of concerns: three crates. `talos-rs` is the gRPC client, `talos-pilot-core` holds the business logic and unit tests, and `talos-pilot-tui` is the ratatui UI. The logic can be tested without a terminal or a live cluster.

## Engineering Highlights

Some implementation notes:

- The Talos API is addressed at node endpoints directly, not through the cluster VIP. The VIP depends on a healthy control plane, so using it would break diagnostics when the control plane is down.
- The COSI resource API is not reachable on the public `:50000` port; a direct gRPC client gets `PermissionDenied`. talos-pilot shells out to `talosctl get` for that data instead.
- When the Talos discovery service is disabled, there is no membership list to read. talos-pilot enumerates and targets nodes through the Kubernetes API, so multi-node views and rolling operations still work.
- Packet capture streams pcap data back over the same `:50000` connection it runs on, so an unfiltered capture records its own traffic and loops. talos-pilot applies a BPF filter that drops traffic on the API port. It is precompiled with `tcpdump -dd` and embedded as bytecode (parameterized by port, with a variant per link type for IPv4/IPv6, TCP/UDP/SCTP, and fragmented packets), so no filter compiler is needed on the node.
- Business logic in `talos-pilot-core` is unit-tested without a terminal or a live node (47 tests). The workspace has 124 tests across the three crates and builds clean under `clippy -D warnings`.

## Installation

### From Releases (Recommended)

Download the latest release for your platform from the [Releases](https://github.com/Handfish/talos-pilot/releases) page.

Install prebuilt binaries via shell script:
```bash
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/Handfish/talos-pilot/releases/download/<version>/talos-pilot-installer.sh | sh
```
Install prebuilt binaries via powershell script:
```bash
powershell -ExecutionPolicy Bypass -c "irm https://github.com/Handfish/talos-pilot/releases/download/<version>/talos-pilot-installer.ps1 | iex"
```

Install prebuilt binaries via Homebrew
```bash
brew install Handfish/tap/talos-pilot
```

### From Source

```bash
git clone https://github.com/Handfish/talos-pilot
cd talos-pilot
cargo build --release
./target/release/talos-pilot
```

### NixOS

Talos pilot is available as a Nix flake but can also be run without installing.

#### Run talos-pilot without installing

You can test the app directly by using a nix shell

```bash
nix shell github:Handfish/talos-pilot
```

Or run it directly

```bash
nix run github:Handfish/talos-pilot
```

#### Usage in flakes

```nix
# flake.nix
{
  inputs = {
    # ...
    talos-pilot.url = "github:Handfish/talos-pilot";
  };
  outputs =
    {
      self,
      nixpkgs,
      talos-pilot,
      # ...
    }:
    {
      nixosConfigurations.mymachine = nixpkgs.lib.nixosSystem {
        system = "x86_64-linux";
        modules = [
          {
            # provides `pkgs.talos-pilot`
            nixpkgs.overlays = [ talos-pilot.overlays.default ];
          }
          (
            { pkgs, ... }:
            {
              # install talos-pilot
              environment.systemPackages = [ pkgs.talos-pilot ];
            }
          )
        ];
      };
    };
}
```

### Requirements

- Valid `~/.talos/config` (talosconfig)
- Network access to Talos nodes on port 50000
- (Building from source) Rust 2024 edition (1.85+)

## Usage

```bash
# Use default context from talosconfig
talos-pilot

# Use specific context
talos-pilot --context homelab

# Set log tail limit
talos-pilot --tail 1000

# Enable debug logging
talos-pilot --debug --log-file ~/talos-pilot.log

# Without --log-file each instance logs to <temp dir>/talos-pilot-<pid>.log
# ($TMPDIR on macOS); logs older than a week are removed at startup
```

### Bootstrap Wizard (Insecure Mode)

For bootstrapping new clusters on bare metal or VMs in maintenance mode, talos-pilot provides an interactive wizard:

https://github.com/user-attachments/assets/0955b5a9-e35d-4fc6-a53d-db16e4558fc9

```bash
# Connect to a node in maintenance mode
talos-pilot --insecure --endpoint <node-ip>
```

The wizard guides you through:
1. **Generate Config** - Creates talosconfig, controlplane.yaml, and worker.yaml
2. **Apply Config** - Applies configuration to the node, triggering installation
3. **Bootstrap** - Initializes etcd and starts the Kubernetes cluster

Once complete, you can manage the cluster using standard talos-pilot commands.

### Keyboard Navigation

| Key | Action |
|-----|--------|
| `?` | Help |
| `q` / `Ctrl+C` | Quit |
| `Esc` | Back / Close |
| `j/k` or `↑/↓` | Navigate |
| `Enter` | Select / Expand |
| `Tab` | Next panel |
| `r` | Refresh |
| `a` | Toggle auto-refresh |
| `/` | Search (in logs) |
| `n/N` | Next/prev search match |

### View Shortcuts

| Key | View | Description |
|-----|------|-------------|
| `c` | Security | PKI and encryption audit |
| `s` | Storage | Disk list with system disk indicators |
| `l` | Logs | Single service logs |
| `L` | Multi-Logs | Interleaved multi-service logs |
| `p` | Processes | Process tree view |
| `n` | Network | Interface stats, connections |
| `e` | etcd | Cluster health, members |
| `w` | Workloads | K8s deployment health |
| `y` | Lifecycle | Version status, alerts |
| `d` | Diagnostics | System health checks |
| `o` | Operations | Single node operations |
| `O` | Rolling | Multi-node rolling operations |

## Architecture

A three-crate workspace (~40k lines of Rust, 110 tests) split so the logic is testable without a terminal or a live cluster:

```
crates/
├── talos-rs/           # Talos gRPC client library   (~10k LOC, 41 tests)
├── talos-pilot-core/   # Shared business logic        (~2.6k LOC, 47 tests)
└── talos-pilot-tui/    # Terminal UI (ratatui)        (~27k LOC, 22 tests)
```

### Core Modules

| Module | Purpose |
|--------|---------|
| `indicators` | HealthIndicator, QuorumState, SafetyStatus |
| `formatting` | format_bytes, format_duration, pluralize |
| `selection` | SelectableList<T>, MultiSelectList<T> |
| `async_state` | Loading/error/refresh state management |
| `diagnostics` | CheckStatus, CniType, PodHealthInfo |
| `constants` | Thresholds, CRD lists, refresh intervals |
| `network` | Port-to-service mapping, classification |
| `errors` | User-friendly error formatting |

### Key Technologies

- **Rust 2024 edition** with async/await
- **tokio** - Async runtime
- **ratatui + crossterm** - TUI framework
- **tonic + prost** - gRPC client
- **kube-rs** - Kubernetes client
- **color-eyre** - Error handling

## Development

```bash
# Run all tests
cargo test --all

# Run with debug output
RUST_LOG=debug cargo run

# Watch logs in another terminal
tail -f "$(ls -t ${TMPDIR:-/tmp}/talos-pilot-*.log | head -1)"

# Check for warnings
cargo clippy --all --all-targets -- -D warnings
```

### Local Testing with Docker

See [docs/local-talos-setup.md](docs/local-talos-setup.md) for setting up a local Talos cluster.

## Contributing

Contributions are welcome. Please keep changes aligned with the [Design Philosophy](#design-philosophy) above: check real system state, degrade gracefully, and never report failure when "unknown" is the honest answer.

## License

MIT License - see [LICENSE](./LICENSE) for details.

## Acknowledgments

- [Talos Linux](https://www.talos.dev/) by Sidero Labs
- [k9s](https://k9scli.io/) for TUI inspiration
- [ratatui](https://ratatui.rs/) for the TUI framework

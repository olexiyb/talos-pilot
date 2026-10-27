//! talos-pilot-tui: Terminal UI for talos-pilot
//!
//! This crate provides a Ratatui-based TUI using the Component pattern
//! with crossterm for terminal input and output.

pub mod action;
pub mod app;
pub mod audit;
pub mod clipboard;
pub mod components;
pub mod kube_list;
pub mod linstor;
pub mod linstor_volumes;
pub mod orphans;
pub mod reachability;
pub mod task;
pub mod tui;
pub mod ui_ext;
pub mod zfs;

pub use app::App;
pub use ui_ext::*;

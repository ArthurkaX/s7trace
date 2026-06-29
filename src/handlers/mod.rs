//! CLI command handlers, split out of `main.rs` by domain.
//!
//! `main.rs` keeps the thin entry points (`main`, `run_main`, `dispatch`) and
//! the shared helpers (`server_base`, `run_op`); each `handle_*` lives here in
//! its domain module and is reached as `handlers::<domain>::<fn>`.

pub mod collection;
pub mod commands;
pub mod events;
pub mod history;
pub mod import;
pub mod reads;
pub mod sets;
pub mod sql;
pub mod status;

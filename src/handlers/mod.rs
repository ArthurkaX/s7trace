//! CLI command handlers, split out of `main.rs` by domain.
//!
//! `main.rs` keeps the thin entry points (`main`, `run_main`, `dispatch`) and
//! the shared helpers (`server_base`, `run_op`); each `handle_*` lives here in
//! its domain module and is reached as `handlers::<domain>::<fn>`.

pub mod import;
pub mod status;

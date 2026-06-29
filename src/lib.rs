#![allow(dead_code)]
// TODO(reboot): remove these once Block 1/2 replace the multi-arg handlers
// (typed `Op` structs) and simplify `CommandKind`.
#![allow(clippy::too_many_arguments)]
#![allow(clippy::enum_variant_names)]

pub mod api;
pub mod cli;
pub mod db;
pub mod engine;
pub mod platform;
pub mod plc;
pub mod types;
pub mod version;

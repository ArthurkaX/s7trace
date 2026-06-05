//! Platform-specific server UI components.
//!
//! On Windows, exposes a small control window and tray icon
//! (see `windows.rs` and `docs/server-control-window.md`).
//! On other platforms this module is empty.

#[cfg(windows)]
pub mod windows;

#[cfg(windows)]
pub use windows::run_control_window;

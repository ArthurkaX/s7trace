pub mod actor;
pub mod client;
pub mod snap7;
pub mod turbos7_backend;

pub use actor::{spawn_actor, PlcHandle, PRIORITY_INTERACTIVE};
pub use turbos7_backend::create_plc_client;

/// Create a PLC connection backed by a per-PLC actor thread (see
/// [`actor`]). This is the server-side entry point: the returned
/// [`PlcHandle`] is cloned per consumer so concurrent poll groups don't block
/// each other on a shared mutex.
pub fn create_plc_handle(plc_name: &str, host: &str, rack: u16, slot: u16) -> PlcHandle {
    let client = create_plc_client(plc_name, host, rack, slot);
    spawn_actor(plc_name.to_string(), client)
}

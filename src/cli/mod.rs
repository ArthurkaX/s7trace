pub mod args;
pub mod format;
pub mod import;
pub mod output;
pub mod queue;
pub mod remote;

/// Resolve the server base URL from CLI args.
/// Returns None when no server address is configured.
pub fn remote_base(cli: &args::Cli) -> Option<String> {
    cli.remote_server
        .as_ref()
        .map(|s| remote::resolve_url(s, cli.server_port))
}

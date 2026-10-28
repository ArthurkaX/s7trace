use crate::cli::format::{ExportFormat, Format, RawFormat, ReadFormat};
use crate::types::DATA_TYPE_HELP;
/// CLI argument definitions using clap derive.
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "s7trace",
    version,
    about = "Read-only data collection service for Siemens S7 PLCs",
    long_about = "s7trace polls and stores data from Siemens S7 PLCs.\n\n\
        Client commands require a server: `-r HOST` or `S7TRACE_SERVER`.\n\
        Local operations: `--local-db sql` (read) and `admin-sql` (maintenance).\n\
        Action commands fail without a server address.",
    after_help = "QUICKSTART  (local maintenance -> live collection):\n  \
        s7trace --local-db sql \"SELECT * FROM plc\"\n  \
        SERVER: Port 7707 by default, override with S7TRACE_PORT.\n  \
        \n\
        REMOTE SERVER:\n  \
        Prefix commands with `-r HOST` or set env S7TRACE_SERVER.\n  \
        The port is 7707 by default (S7TRACE_PORT to override).\n  \
        Full URL also accepted (e.g. `-r http://host:7707`).\n\
        \n\
        COMMAND GROUPS:\n  \
        config    plc-add  poll-group-add  tag-add  tag-import  tag-update  tag-disable\n  \
        sets      set-create  set-add-tag  set-remove-tag  set-enable  set-disable  set-list  set-show\n  \
        collect   collection-start  collection-stop\n  \
        inspect   status  read  read-raw  sql  command-list  command-result  command-wait\n  \
        history   history-clean  history-policy  history-delete-dataset  history-vacuum\n  \
        admin     server  admin-sql  tag-revalidate\n\
        \n\
        MACHINE OUTPUT:\n  \
        `--json` (or env S7TRACE_JSON=1): one JSON envelope per command with\n  \
        error.code, error.retryable and command_id. After a timeout, follow the\n  \
        command with `command-wait <id>` instead of resubmitting it.\n\
        \n\
        TARGETS:\n  \
        Server: `-r IP` or `-r http://IP:PORT` (or env S7TRACE_SERVER).\n  \
        Local DB: `--local-db` for sql/admin-sql only (read-only SQL or maintenance).\n  \
        All other commands require a server address and fail with a clear error.\n\
        \n\
        Run `s7trace <command> --help` for the options of a single command."
)]
pub struct Cli {
    /// Path to SQLite database.
    #[arg(
        short = 'd',
        long = "db",
        default_value = "s7trace.db",
        env = "S7TRACE_DB"
    )]
    pub db: String,

    /// Route operations through a s7trace server at this address.
    /// Accepts a bare IP/host (e.g. 192.168.0.50) or a full URL
    /// (e.g. http://192.168.0.50:7707). Default port is 7707;
    /// override with env S7TRACE_PORT.
    #[arg(
        short = 'r',
        long(None::<&str>),
        env = "S7TRACE_SERVER",
        global = true
    )]
    pub remote_server: Option<String>,

    /// Server listening port for the s7trace HTTP transport.
    /// Only used when -r / S7TRACE_SERVER is a bare host (no port).
    /// Default: 7707.
    #[arg(
        long = "server-port",
        default_value_t = 7707,
        env = "S7TRACE_PORT",
        global = true
    )]
    pub server_port: u16,

    /// Explicitly use the local SQLite database selected by --db.
    /// Only valid with `sql` (read-only) and `admin-sql` (maintenance).
    /// Action commands (tag-add, plc-add, etc.) reject --local-db.
    #[arg(long = "local-db", global = true, conflicts_with = "remote_server")]
    pub local_db: bool,

    /// Maximum time to wait for a remote command to complete, in ms.
    #[arg(long = "remote-timeout-ms", default_value_t = 30_000, global = true)]
    pub remote_timeout_ms: u64,

    /// Machine-readable output: print exactly one JSON line on stdout,
    /// `{"ok":true,"result":...}` or
    /// `{"ok":false,"error":{"code":...,"message":...,"retryable":...}}`,
    /// with `command_id` for queued commands. Takes precedence over
    /// `--format`. Logs stay on stderr.
    #[arg(
        long = "json",
        global = true,
        env = "S7TRACE_JSON",
        action = clap::ArgAction::SetTrue,
        value_parser = clap::builder::FalseyValueParser::new()
    )]
    pub json: bool,

    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Subcommand)]
pub enum Command {
    /// Start the s7trace server (daemon mode).
    Server {
        /// Run once, check configuration, then exit.
        #[arg(long)]
        dry_run: bool,
        /// Bind the internal HTTP transport for s7trace CLI clients
        /// (e.g. 127.0.0.1:8080). Default: 0.0.0.0:7707.
        /// Operators must use `s7trace -r`, not curl.
        #[arg(long)]
        http_bind: Option<String>,
        /// Run the server without the Windows control window.
        #[arg(long = "noui")]
        no_ui: bool,
        /// Use strict overrun mode: don't skip missed poll ticks (useful for commissioning).
        #[arg(long)]
        strict_overrun: bool,
        /// Skip the startup VACUUM. By default the server compacts the
        /// database before collection starts when at least a quarter of the
        /// file is free space left by history retention.
        #[arg(long)]
        no_startup_vacuum: bool,
    },
    /// Execute a read-only SQL query through s7trace.
    Sql {
        /// SQL query string. Use this command even for remote databases:
        /// `s7trace -r http://HOST:8080 sql "SELECT ..."`.
        /// Direct HTTP SQL, curl, and browser calls are forbidden.
        query: String,

        /// Output format. `json` carries `truncated` and `limit`; `text` and
        /// `jsonl` report truncation on stderr.
        #[arg(long = "format", value_enum, default_value_t = Format::Text)]
        format: Format,

        /// Maximum rows to return.
        #[arg(long = "limit", default_value_t = 200)]
        limit: u64,

        /// Query timeout in milliseconds. The server caps this at 30000; a
        /// query that runs past the deadline is interrupted, not queued.
        #[arg(long = "timeout-ms", default_value_t = 5000)]
        timeout_ms: u64,
    },

    /// Export the samples of one or more tags over a time range, as JSON Lines
    /// or CSV. Unlike `sql` it has no row cap: it reads in pages and streams,
    /// merging the tags in timestamp order.
    SampleExport {
        /// Tag name or id. Repeat for several tags.
        #[arg(long = "tag", required = true)]
        tags: Vec<String>,

        /// PLC name, when a tag name exists on several PLCs.
        #[arg(long = "plc")]
        plc: Option<String>,

        /// Start of the range, inclusive: RFC 3339, `YYYY-MM-DD[ HH:MM[:SS]]`
        /// (UTC), or Unix microseconds.
        #[arg(long = "since")]
        since: String,

        /// End of the range, exclusive. Same forms as `--since`.
        #[arg(long = "until")]
        until: String,

        /// Row encoding.
        #[arg(long = "format", value_enum, default_value_t = ExportFormat::Jsonl)]
        format: ExportFormat,

        /// Write the rows to this file instead of stdout. Required with
        /// `--json`, which prints a one-line summary envelope instead.
        #[arg(long = "out")]
        out: Option<std::path::PathBuf>,

        /// Rows per request (at most 10000).
        #[arg(long = "page-rows", default_value_t = 10000)]
        page_rows: i64,
    },

    /// Execute write-capable SQL against the local database (maintenance only).
    AdminSql {
        /// SQL statement. This command is local-only: `-r`
        /// is rejected. For a server database, run this on the server host
        /// with the server's --db path. Do not use curl or direct HTTP SQL.
        query: String,
    },

    /// List queued commands.
    CommandList {
        /// Filter by state: pending, running, done, bad, cancelled.
        #[arg(long = "state")]
        state: Option<String>,

        /// Output format.
        #[arg(long = "format", value_enum, default_value_t = Format::Text)]
        format: Format,
    },

    /// Wait for a queued command to complete.
    CommandWait {
        /// Command ID to wait for.
        command_id: i64,

        /// Poll interval in ms.
        #[arg(long = "poll-ms", default_value_t = 200)]
        poll_ms: u64,

        /// Maximum wait time in ms.
        #[arg(long = "timeout-ms", default_value_t = 60000)]
        timeout_ms: u64,
    },

    /// Cancel a pending command.
    CommandCancel {
        /// Command ID to cancel.
        command_id: i64,
    },

    /// Print a stored command result.
    CommandResult {
        /// Command ID.
        command_id: i64,

        /// Output format.
        #[arg(long = "format", value_enum, default_value_t = Format::Text)]
        format: Format,
    },

    /// Validate and add a new tag.
    TagAdd {
        /// PLC name.
        #[arg(long = "plc")]
        plc: String,

        /// Tag name.
        #[arg(long = "name")]
        name: String,

        /// S7 address (e.g. DB5999.DBW120).
        #[arg(long = "address")]
        address: String,

        /// Data type: bool, i16, u16, i32, u32, f32.
        #[arg(long = "type")]
        data_type: String,

        /// Poll group name.
        #[arg(long = "poll-group", default_value = "slow")]
        poll_group: String,

        /// Store mode.
        #[arg(long = "store-mode", default_value = "on_change")]
        store_mode: String,

        /// Storage hook.
        #[arg(long = "storage-hook", default_value = "on_change")]
        storage_hook: String,

        /// Value role.
        #[arg(long = "value-role", default_value = "state")]
        value_role: String,

        /// Metric set to add the tag to.
        #[arg(long = "set")]
        metric_set: Option<String>,
    },

    /// Revalidate and update an existing tag.
    TagUpdate {
        /// Tag ID.
        #[arg(long = "id")]
        tag_id: Option<i64>,

        /// PLC name (alternative to --id).
        #[arg(long = "plc")]
        plc: Option<String>,

        /// Tag name (alternative to --id, requires --plc).
        #[arg(long = "name")]
        name: Option<String>,

        /// New address (if changing).
        #[arg(long = "address")]
        address: Option<String>,

        /// New data type.
        #[arg(long = "type")]
        data_type: Option<String>,

        /// New poll group.
        #[arg(long = "poll-group")]
        poll_group: Option<String>,

        /// New store mode.
        #[arg(long = "store-mode")]
        store_mode: Option<String>,

        /// New storage hook.
        #[arg(long = "storage-hook")]
        storage_hook: Option<String>,
    },

    /// Disable a tag (remove from plans without deleting history).
    TagDisable {
        /// Tag ID to disable.
        #[arg(long = "id")]
        tag_id: Option<i64>,

        /// PLC name (alternative to --id).
        #[arg(long = "plc")]
        plc: Option<String>,

        /// Tag name (alternative to --id, requires --plc).
        #[arg(long = "name")]
        name: Option<String>,
    },

    /// Delete a tag entirely (removes the row and its samples/rollups/events;
    /// metric-set membership cascades). Use for tags that are wrong for this
    /// PLC (e.g. a DB that does not exist). Routed to the server with `-r`.
    TagDelete {
        /// Tag ID to delete.
        #[arg(long = "id")]
        tag_id: Option<i64>,

        /// PLC name (alternative to --id).
        #[arg(long = "plc")]
        plc: Option<String>,

        /// Tag name (alternative to --id, requires --plc).
        #[arg(long = "name")]
        name: Option<String>,
    },

    /// Enable a disabled tag (re-add to plans).
    TagEnable {
        /// Tag ID to enable.
        #[arg(long = "id")]
        tag_id: Option<i64>,

        /// PLC name (alternative to --id).
        #[arg(long = "plc")]
        plc: Option<String>,

        /// Tag name (alternative to --id, requires --plc).
        #[arg(long = "name")]
        name: Option<String>,
    },

    /// Revalidate a bad tag.
    TagRevalidate {
        /// Tag ID.
        #[arg(long = "id")]
        tag_id: Option<i64>,

        /// PLC name (alternative to --id).
        #[arg(long = "plc")]
        plc: Option<String>,

        /// Tag name (alternative to --id, requires --plc).
        #[arg(long = "name")]
        name: Option<String>,
    },

    /// One-off typed PLC read, not added to polling.
    Read {
        /// PLC name.
        #[arg(long = "plc")]
        plc: String,

        /// S7 address (e.g. DB5999.DBW120).
        #[arg(long = "address")]
        address: String,

        /// Data type.
        #[arg(long = "type", help = DATA_TYPE_HELP)]
        data_type: String,

        /// Output format (always JSON).
        #[arg(long = "format", value_enum, default_value_t = ReadFormat::Json)]
        format: ReadFormat,
    },

    /// One-off raw DB byte read for offset diagnostics.
    ReadRaw {
        /// PLC name.
        #[arg(long = "plc")]
        plc: String,

        /// DB number.
        #[arg(long = "db")]
        db: u16,

        /// Start offset.
        #[arg(long = "start")]
        start: usize,

        /// Number of bytes.
        #[arg(long = "len", default_value_t = 32)]
        len: usize,

        /// Byte encoding in the JSON result.
        #[arg(long = "format", value_enum, default_value_t = RawFormat::Hex)]
        format: RawFormat,

        /// Save as a diagnostic probe.
        #[arg(long = "save-probe")]
        save_probe: bool,
    },

    /// Runtime health, bad tags, current plans.
    Status {
        /// Output format.
        #[arg(long = "format", value_enum, default_value_t = Format::Text)]
        format: Format,
    },

    /// Create a new metric set.
    SetCreate {
        /// PLC name.
        #[arg(long = "plc")]
        plc: String,

        /// Set name.
        #[arg(long = "name")]
        name: String,

        /// Description.
        #[arg(long = "description")]
        description: Option<String>,
    },

    /// Add tags to a metric set. Accepts a single tag (name or ID) or
    /// a comma-separated list (`--tag tag1,tag2,tag3`). For bulk import
    /// use `--from-sql` (returns tag IDs) or `--from-file` (one tag name
    /// per line, `#`-comments allowed).
    SetAddTag {
        /// Metric set name.
        #[arg(long = "set")]
        set_name: String,

        /// Tag name, ID, or comma-separated list.
        #[arg(long = "tag")]
        tag: Option<String>,

        /// Add tags from SQL result (returns tag IDs).
        #[arg(long = "from-sql")]
        from_sql: Option<String>,

        /// Add tags from a text file (one name per line).
        #[arg(long = "from-file")]
        from_file: Option<String>,
    },

    /// Remove a tag from a metric set.
    SetRemoveTag {
        /// Metric set name.
        #[arg(long = "set")]
        set_name: String,

        /// Tag name or ID.
        #[arg(long = "tag")]
        tag: Option<String>,

        /// Remove tags matching SQL.
        #[arg(long = "from-sql")]
        from_sql: Option<String>,
    },

    /// Enable a metric set.
    SetEnable {
        /// Metric set name.
        set_name: String,
    },

    /// Disable a metric set.
    SetDisable {
        /// Metric set name.
        set_name: String,
    },

    /// List metric sets for a PLC.
    SetList {
        /// PLC name.
        #[arg(long = "plc")]
        plc: Option<String>,
    },

    /// Show metric set details.
    SetShow {
        /// PLC name.
        #[arg(long = "plc")]
        plc: Option<String>,

        /// Metric set name (positional).
        set_name: Option<String>,

        /// Metric set name.
        #[arg(long = "set")]
        set_flag: Option<String>,
    },

    /// Delete a metric set and its membership rows (cascades to metric_set_tag).
    SetDelete {
        /// Metric set name.
        set_name: String,
    },

    /// Start collecting data into a named dataset.
    CollectionStart {
        /// PLC name.
        #[arg(long = "plc")]
        plc: String,

        /// Dataset name.
        #[arg(long = "dataset")]
        dataset: String,

        /// Metric set(s) to collect.
        #[arg(long = "set")]
        sets: Vec<String>,
    },

    /// Stop data collection for a PLC.
    CollectionStop {
        /// PLC name.
        #[arg(long = "plc")]
        plc: String,
    },

    /// Clean history by cutoff timestamp.
    HistoryClean {
        /// PLC name.
        #[arg(long = "plc")]
        plc: String,

        /// Delete history (samples, rollups, events, PLC time samples, raw
        /// probes and the poll cycles nothing references) before this
        /// timestamp, in epoch microseconds.
        #[arg(long = "before-ts")]
        before_ts: Option<i64>,

        /// Same cutoff as --before-ts, given as an ISO 8601 datetime.
        #[arg(long = "before")]
        before: Option<String>,

        /// Batch size for deletes.
        #[arg(long = "batch-size", default_value_t = 10000)]
        batch_size: u64,
    },

    /// Show or change the retention policy.
    HistoryPolicy {
        /// Days of poll_cycle history to keep; 0 keeps it forever.
        #[arg(long = "cycle-days")]
        cycle_days: Option<i64>,

        /// Days of sample history to keep; 0 keeps it forever.
        #[arg(long = "sample-days")]
        sample_days: Option<i64>,

        /// Days of failed poll_cycle history to keep; 0 keeps failed cycles forever.
        #[arg(long = "failed-cycle-days")]
        failed_cycle_days: Option<i64>,

        /// Apply the separate failed-cycle cutoff instead of cycle-days.
        #[arg(long = "keep-failed")]
        keep_failed: Option<bool>,

        /// Rows deleted per statement by the collector's retention sweep.
        #[arg(long = "batch-rows")]
        batch_rows: Option<i64>,
    },

    /// Delete a stopped dataset.
    HistoryDeleteDataset {
        /// Dataset name.
        #[arg(long = "dataset")]
        dataset: String,
    },

    /// Run VACUUM on the database.
    HistoryVacuum {
        /// Skip confirmation prompt.
        #[arg(long = "force")]
        force: bool,
    },

    /// Create a new PLC.
    PlcAdd {
        /// PLC name.
        #[arg(long)]
        name: String,

        /// PLC host (IP or hostname).
        #[arg(long)]
        host: String,

        /// Rack number.
        #[arg(long, default_value_t = 0)]
        rack: i64,

        /// Slot number.
        #[arg(long, default_value_t = 2)]
        slot: i64,

        /// Disable the PLC (instead of default enabled).
        #[arg(long)]
        disabled: bool,
    },

    /// Create a new poll group.
    PollGroupAdd {
        /// Poll group name.
        #[arg(long)]
        name: String,

        /// Poll period in milliseconds.
        #[arg(long)]
        period_ms: i64,
    },

    /// Update a poll group's polling period.
    PollGroupSetPeriod {
        /// Poll group ID.
        #[arg(long = "id")]
        poll_group_id: Option<i64>,

        /// Poll group name (alternative to --id).
        #[arg(long = "name")]
        poll_group_name: Option<String>,

        /// New polling period in milliseconds (minimum 50).
        #[arg(long = "period-ms")]
        period_ms: i64,
    },

    /// Import tags from a YAML-shaped signal file.
    TagImport {
        /// PLC name.
        #[arg(long)]
        plc: String,

        /// Path to the signal file.
        #[arg(long)]
        file: String,

        /// Metric set name (optional; defaults to 'default').
        #[arg(long)]
        set: Option<String>,

        /// Poll group name.
        #[arg(long = "poll-group", default_value = "slow")]
        poll_group: String,

        /// Store mode.
        #[arg(long = "store-mode", default_value = "on_change")]
        store_mode: String,

        /// Storage hook.
        #[arg(long = "storage-hook", default_value = "on_change")]
        storage_hook: String,

        /// Value role.
        #[arg(long = "value-role", default_value = "state")]
        value_role: String,

        /// Validate each imported tag by reading from the PLC.
        #[arg(long)]
        validate: bool,
    },

    /// Add an event marker.
    Event {
        #[command(subcommand)]
        action: EventAction,
    },

    /// Quick note (shorthand for `event add --kind note`).
    Note {
        /// Note text (stored in details_json).
        text: String,

        /// PLC name (auto-detected if omitted).
        #[arg(long = "plc")]
        plc: Option<String>,

        /// Dataset name.
        #[arg(long = "dataset")]
        dataset: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn test_command_cancel_parse() {
        let cli = Cli::parse_from(["s7trace", "command-cancel", "42"]);
        match cli.command.unwrap() {
            Command::CommandCancel { command_id } => assert_eq!(command_id, 42),
            _ => panic!("expected CommandCancel"),
        }
    }
}

#[derive(Subcommand)]
pub enum EventAction {
    /// Create a new event marker.
    Add {
        /// Event text (stored in details_json).
        text: String,

        /// Event kind (default: note).
        #[arg(long = "kind", default_value = "note")]
        kind: String,

        /// Severity: info, warning, error (default: info).
        #[arg(long = "severity", default_value = "info")]
        severity: String,

        /// PLC name (auto-detected if omitted).
        #[arg(long = "plc")]
        plc: Option<String>,

        /// Dataset name.
        #[arg(long = "dataset")]
        dataset: Option<String>,

        /// Poll cycle ID.
        #[arg(long = "cycle-id")]
        cycle_id: Option<i64>,
    },
}

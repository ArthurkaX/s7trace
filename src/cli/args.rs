/// CLI argument definitions using clap derive.

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "s7trace", about = "Read-only S7 data collection service for Siemens PLCs")]
pub struct Cli {
    /// Path to SQLite database.
    #[arg(short = 'd', long = "db", default_value = "s7trace.db", env = "S7TRACE_DB")]
    pub db: String,

    /// If set, all PLC- and command-related operations go through the
    /// remote server at this URL (e.g. http://10.0.0.1:8080). The local
    /// --db is still used for read-only SQL queries that can be answered
    /// from a replica. Use this when the server runs on-site and the CLI
    /// has no direct PLC access.
    #[arg(long = "remote-server", env = "S7TRACE_REMOTE_SERVER", global = true)]
    pub remote_server: Option<String>,

    /// Maximum time to wait for a remote command to complete, in ms.
    #[arg(long = "remote-timeout-ms", default_value_t = 30_000, global = true)]
    pub remote_timeout_ms: u64,

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
        /// Bind the HTTP control API to this address (e.g. 127.0.0.1:8080).
        #[arg(long)]
        http_bind: Option<String>,
        /// Open the Windows control window (Windows only; ignored elsewhere).
        #[arg(long)]
        control_window: bool,
    },
    /// Execute a read-only SQL query.
    Sql {
        /// SQL query string.
        query: String,

        /// Output format: table, json, jsonl.
        #[arg(long = "format", default_value = "table")]
        format: String,

        /// Maximum rows to return.
        #[arg(long = "limit", default_value_t = 200)]
        limit: u64,

        /// Query timeout in milliseconds.
        #[arg(long = "timeout-ms", default_value_t = 5000)]
        timeout_ms: u64,
    },

    /// Execute a write-capable SQL query (maintenance only).
    AdminSql {
        /// SQL statement.
        query: String,
    },

    /// List queued commands.
    CommandList {
        /// Filter by state.
        #[arg(long = "state")]
        state: Option<String>,
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

    /// Print a stored command result.
    CommandResult {
        /// Command ID.
        command_id: i64,

        /// Output format: text, json, jsonl.
        #[arg(long = "format", default_value = "text")]
        format: String,
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

        /// Submit asynchronously.
        #[arg(long = "async")]
        async_mode: bool,
    },

    /// Revalidate and update an existing tag.
    TagUpdate {
        /// Tag ID.
        #[arg(long = "id")]
        tag_id: i64,

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
        tag_id: i64,
    },

    /// Revalidate a bad tag.
    TagRevalidate {
        /// Tag ID.
        #[arg(long = "id")]
        tag_id: i64,
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
        #[arg(long = "type")]
        data_type: String,

        /// Output format.
        #[arg(long = "format", default_value = "text")]
        format: String,
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

        /// Output format: hex, json.
        #[arg(long = "format", default_value = "hex")]
        format: String,

        /// Save as a diagnostic probe.
        #[arg(long = "save-probe")]
        save_probe: bool,
    },

    /// Runtime health, bad tags, current plans.
    Status {
        /// Output format: text, json, jsonl.
        #[arg(long = "format", default_value = "text")]
        format: String,
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

        /// Submit asynchronously.
        #[arg(long = "async")]
        async_mode: bool,
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

        /// Delete samples before this timestamp (epoch microseconds).
        #[arg(long = "before-ts")]
        before_ts: Option<i64>,

        /// Delete samples before this datetime (ISO 8601).
        #[arg(long = "before")]
        before: Option<String>,

        /// Batch size for deletes.
        #[arg(long = "batch-size", default_value_t = 10000)]
        batch_size: u64,
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
}

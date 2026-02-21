use std::{collections::HashSet, sync::Arc, time::Duration};

use miette::{Context, Result, miette};
use redis_protocol::resp2::types::BytesFrame;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Semaphore,
    task::JoinSet,
    time::timeout,
};

mod app_state;
mod cluster;
mod compression;
mod config;
mod http_server;
mod metrics;
mod migration;
mod redis;
mod server;
mod shard_manager;

use app_state::AppState;
use gumdrop::Options;

const DEFAULT_NODE_CONCURRENCY: usize = 1;
const DEFAULT_SHARD_CONCURRENCY: usize = 1;
const DEFAULT_NODE_TIMEOUT_SEC: u64 = 30;
const MAX_ADMIN_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Options, Debug)]
struct Args {
    #[options(help = "Print help message")]
    help: bool,

    #[options(
        help = "Path to the configuration file",
        short = "c",
        long = "config",
        default = "config.toml"
    )]
    config: String,

    #[options(command)]
    command: Option<Command>,
}

#[derive(Options, Debug)]
enum Command {
    #[options(help = "Run the server (default)")]
    Serve(ServeOptions),

    #[options(help = "Shard migration and maintenance commands")]
    Shard(ShardCommand),
}

#[derive(Options, Debug)]
struct ServeOptions {}

#[derive(Options, Debug)]
enum ShardCommand {
    #[options(help = "Migrate data from old shard configuration to new")]
    Migrate(MigrateOptions),

    #[options(help = "Orchestrate vacuum across one or more nodes")]
    Vacuum(VacuumOptions),
}

#[derive(Options, Debug)]
struct MigrateOptions {
    #[options(help = "Old number of shards", free)]
    old_shard_count: usize,

    #[options(help = "New number of shards", free)]
    new_shard_count: usize,

    #[options(
        help = "Data directory path",
        short = "d",
        long = "data-dir",
        meta = "DIR"
    )]
    data_dir: Option<String>,

    #[options(help = "Verify migration after completion")]
    verify: bool,
}

#[derive(Options, Debug)]
struct VacuumOptions {
    #[options(help = "Target all local shards on each node", long = "all-shards")]
    all_shards: bool,

    #[options(
        help = "Vacuum mode: incremental|full",
        long = "mode",
        default = "incremental"
    )]
    mode: String,

    #[options(help = "Per-shard budget in MB", long = "budget-mb", default = "128")]
    budget_mb: u64,

    #[options(
        help = "Comma-separated node addresses (host:port)",
        long = "nodes",
        meta = "ADDR1,ADDR2,..."
    )]
    nodes: Option<String>,

    #[options(help = "Only calculate/report vacuum effects", long = "dry-run")]
    dry_run: bool,

    #[options(
        help = "Max parallel node calls",
        long = "node-concurrency",
        default = "1"
    )]
    node_concurrency: usize,

    #[options(
        help = "Shard concurrency per node (currently serial-only)",
        long = "shard-concurrency",
        default = "1"
    )]
    shard_concurrency: usize,

    #[options(help = "Per-node timeout in seconds", long = "timeout-sec")]
    timeout_sec: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
enum CliVacuumMode {
    Incremental,
    Full,
}

impl CliVacuumMode {
    fn as_str(&self) -> &'static str {
        match self {
            CliVacuumMode::Incremental => "incremental",
            CliVacuumMode::Full => "full",
        }
    }
}

#[derive(Debug, Clone)]
struct VacuumInvocation {
    mode: CliVacuumMode,
    budget_mb: u64,
    dry_run: bool,
    timeout: Duration,
}

#[derive(Debug)]
struct NodeVacuumReport {
    node: String,
    elapsed: Duration,
    result: std::result::Result<VacuumResponsePayload, String>,
}

#[derive(Debug)]
struct VacuumResponsePayload {
    mode: String,
    budget_mb: u64,
    dry_run: bool,
    shard_results: Vec<ShardVacuumResult>,
}

#[derive(Debug)]
struct ShardVacuumResult {
    shard_id: usize,
    status: String,
    error: Option<String>,
    duration_ms: Option<u64>,
    estimated_reclaimed_pages: Option<u64>,
    incremental_pages_requested: Option<u64>,
    execution_errors: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // initialize tracing
    tracing_subscriber::fmt::init();

    let args = Args::parse_args_default_or_exit();

    // Handle different commands
    match args.command {
        Some(Command::Shard(shard_cmd)) => handle_shard_command(shard_cmd, &args.config).await,
        Some(Command::Serve(_)) | None => {
            // Default to serving if no command specified
            run_server(&args.config).await
        }
    }
}

async fn handle_shard_command(shard_cmd: ShardCommand, config_path: &str) -> Result<()> {
    match shard_cmd {
        ShardCommand::Migrate(migrate_opts) => {
            let data_dir = if let Some(dir) = migrate_opts.data_dir {
                dir
            } else {
                // Load config to get data_dir
                let cfg = config::Cfg::load(config_path).wrap_err("loading config")?;
                cfg.data_dir
            };

            let migration_manager = migration::MigrationManager::new(
                migrate_opts.old_shard_count,
                migrate_opts.new_shard_count,
                data_dir,
            )?;

            migration_manager.run_migration().await?;

            if migrate_opts.verify {
                migration_manager.verify_migration().await?;
            }

            Ok(())
        }
        ShardCommand::Vacuum(vacuum_opts) => {
            handle_shard_vacuum_command(vacuum_opts, config_path).await
        }
    }
}

async fn handle_shard_vacuum_command(vacuum_opts: VacuumOptions, config_path: &str) -> Result<()> {
    if !vacuum_opts.all_shards {
        return Err(miette!(
            "Phase-4 orchestrator currently supports all local shards only; pass --all-shards"
        ));
    }

    if vacuum_opts.budget_mb == 0 {
        return Err(miette!("--budget-mb must be greater than 0"));
    }

    let mode = parse_vacuum_mode(&vacuum_opts.mode)?;
    let node_concurrency = vacuum_opts.node_concurrency.max(DEFAULT_NODE_CONCURRENCY);
    let shard_concurrency = vacuum_opts.shard_concurrency.max(DEFAULT_SHARD_CONCURRENCY);
    let timeout_sec = vacuum_opts
        .timeout_sec
        .unwrap_or(DEFAULT_NODE_TIMEOUT_SEC)
        .max(1);

    if shard_concurrency != 1 {
        eprintln!(
            "Note: --shard-concurrency={} is not supported yet in v1; each node still runs shards serially.",
            shard_concurrency
        );
    }

    let cfg = if vacuum_opts.nodes.is_some() {
        None
    } else {
        match config::Cfg::load(config_path) {
            Ok(cfg) => Some(cfg),
            Err(err) => {
                eprintln!(
                    "Warning: failed to load config '{}': {}. Falling back to 127.0.0.1:6379.",
                    config_path, err
                );
                None
            }
        }
    };

    let nodes = resolve_target_nodes(vacuum_opts.nodes.as_deref(), cfg.as_ref())?;
    let invocation = VacuumInvocation {
        mode,
        budget_mb: vacuum_opts.budget_mb,
        dry_run: vacuum_opts.dry_run,
        timeout: Duration::from_secs(timeout_sec),
    };

    println!("Vacuum orchestration started");
    println!("  mode: {}", invocation.mode.as_str());
    println!("  budget_mb: {}", invocation.budget_mb);
    println!("  dry_run: {}", invocation.dry_run);
    println!("  node_concurrency: {}", node_concurrency);
    println!("  shard_concurrency: {}", shard_concurrency);
    println!("  timeout_sec: {}", timeout_sec);
    println!("  nodes: {}", nodes.join(", "));

    let reports = run_vacuum_on_nodes(nodes, invocation.clone(), node_concurrency).await;
    let has_failures = print_vacuum_report(&reports);

    if has_failures {
        return Err(miette!("vacuum orchestration completed with failures"));
    }

    Ok(())
}

fn parse_vacuum_mode(raw: &str) -> Result<CliVacuumMode> {
    if raw.eq_ignore_ascii_case("incremental") {
        return Ok(CliVacuumMode::Incremental);
    }

    if raw.eq_ignore_ascii_case("full") {
        return Ok(CliVacuumMode::Full);
    }

    Err(miette!(
        "invalid --mode '{}': expected incremental or full",
        raw
    ))
}

fn resolve_target_nodes(
    explicit_nodes: Option<&str>,
    cfg: Option<&config::Cfg>,
) -> Result<Vec<String>> {
    if let Some(nodes_raw) = explicit_nodes {
        let nodes = parse_node_list(nodes_raw);
        if nodes.is_empty() {
            return Err(miette!("--nodes must contain at least one node address"));
        }
        return Ok(nodes);
    }

    let mut nodes = Vec::new();

    if let Some(cfg) = cfg {
        if let Some(addr) = cfg.addr.as_deref() {
            if let Some(normalized) = normalize_node_address(addr) {
                nodes.push(normalized);
            }
        }

        if let Some(cluster_cfg) = cfg.cluster.as_ref() {
            if let Some(advertise_addr) = cluster_cfg.advertise_addr.as_deref() {
                if let Some(normalized) = normalize_node_address(advertise_addr) {
                    nodes.push(normalized);
                }
            }
        }
    }

    if nodes.is_empty() {
        nodes.push("127.0.0.1:6379".to_string());
    }

    Ok(dedupe_nodes(nodes))
}

fn parse_node_list(raw: &str) -> Vec<String> {
    let nodes = raw
        .split(',')
        .filter_map(normalize_node_address)
        .collect::<Vec<_>>();

    dedupe_nodes(nodes)
}

fn normalize_node_address(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Some(port) = trimmed.strip_prefix("0.0.0.0:") {
        return Some(format!("127.0.0.1:{}", port));
    }

    if let Some(port) = trimmed.strip_prefix("[::]:") {
        return Some(format!("[::1]:{}", port));
    }

    Some(trimmed.to_string())
}

fn dedupe_nodes(nodes: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::with_capacity(nodes.len());

    for node in nodes {
        if seen.insert(node.clone()) {
            deduped.push(node);
        }
    }

    deduped
}

async fn run_vacuum_on_nodes(
    nodes: Vec<String>,
    invocation: VacuumInvocation,
    node_concurrency: usize,
) -> Vec<NodeVacuumReport> {
    let concurrency = node_concurrency.max(1);
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let mut join_set = JoinSet::new();

    for node in nodes {
        let semaphore = semaphore.clone();
        let invocation = invocation.clone();

        join_set.spawn(async move {
            let started_at = std::time::Instant::now();
            let permit = semaphore.acquire_owned().await;

            let result = match permit {
                Ok(_permit_guard) => call_node_vacuum(&node, &invocation).await,
                Err(_) => Err("node concurrency semaphore closed".to_string()),
            };

            NodeVacuumReport {
                node,
                elapsed: started_at.elapsed(),
                result,
            }
        });
    }

    let mut reports = Vec::new();

    while let Some(next) = join_set.join_next().await {
        match next {
            Ok(report) => reports.push(report),
            Err(err) => reports.push(NodeVacuumReport {
                node: "<join_error>".to_string(),
                elapsed: Duration::from_secs(0),
                result: Err(format!("vacuum task join error: {}", err)),
            }),
        }
    }

    reports.sort_by(|a, b| a.node.cmp(&b.node));
    reports
}

async fn call_node_vacuum(
    node: &str,
    invocation: &VacuumInvocation,
) -> std::result::Result<VacuumResponsePayload, String> {
    let request = build_vacuum_command(invocation);
    let response_frame = send_redis_command_with_timeout(node, request, invocation.timeout).await?;

    parse_vacuum_response_frame(response_frame)
}

fn build_vacuum_command(invocation: &VacuumInvocation) -> BytesFrame {
    let mut parts = vec![
        "BLOBASAUR.VACUUM".to_string(),
        "SHARD".to_string(),
        "ALL".to_string(),
        "MODE".to_string(),
        invocation.mode.as_str().to_string(),
        "BUDGET_MB".to_string(),
        invocation.budget_mb.to_string(),
    ];

    if invocation.dry_run {
        parts.push("DRYRUN".to_string());
    }

    BytesFrame::Array(
        parts
            .into_iter()
            .map(|part| BytesFrame::BulkString(part.into_bytes().into()))
            .collect(),
    )
}

async fn send_redis_command_with_timeout(
    node: &str,
    request_frame: BytesFrame,
    timeout_duration: Duration,
) -> std::result::Result<BytesFrame, String> {
    let request_bytes = redis::serialize_frame(&request_frame);

    timeout(timeout_duration, async {
        let mut stream = TcpStream::connect(node)
            .await
            .map_err(|e| format!("failed to connect to {}: {}", node, e))?;

        stream
            .write_all(request_bytes.as_ref())
            .await
            .map_err(|e| format!("failed to send command to {}: {}", node, e))?;

        stream
            .flush()
            .await
            .map_err(|e| format!("failed to flush command to {}: {}", node, e))?;

        read_single_resp_frame(&mut stream).await
    })
    .await
    .map_err(|_| {
        format!(
            "timed out waiting for node {} after {}s",
            node,
            timeout_duration.as_secs()
        )
    })?
}

async fn read_single_resp_frame(stream: &mut TcpStream) -> std::result::Result<BytesFrame, String> {
    let mut buffer = Vec::with_capacity(4096);
    let mut chunk = [0_u8; 4096];

    loop {
        let read = stream
            .read(&mut chunk)
            .await
            .map_err(|e| format!("failed reading node response: {}", e))?;

        if read == 0 {
            return Err("connection closed before a full RESP response was received".to_string());
        }

        buffer.extend_from_slice(&chunk[..read]);

        if buffer.len() > MAX_ADMIN_RESPONSE_BYTES {
            return Err(format!(
                "node response exceeded max size ({} bytes)",
                MAX_ADMIN_RESPONSE_BYTES
            ));
        }

        match redis::parse_resp_with_remaining(&buffer) {
            Ok((frame, _)) => return Ok(frame),
            Err(redis::ParseError::Incomplete) => continue,
            Err(err) => return Err(format!("failed to parse RESP response: {}", err)),
        }
    }
}

fn parse_vacuum_response_frame(
    frame: BytesFrame,
) -> std::result::Result<VacuumResponsePayload, String> {
    match frame {
        BytesFrame::Error(err) => Err(format!("node returned error: {}", err)),
        BytesFrame::Array(fields) => {
            let mode = required_string_field(&fields, "mode")?;
            let budget_mb = required_u64_field(&fields, "budget_mb")?;
            let dry_run_raw = required_u64_field(&fields, "dry_run")?;
            let dry_run = match dry_run_raw {
                0 => false,
                1 => true,
                other => {
                    return Err(format!("field 'dry_run' must be 0 or 1, got {}", other));
                }
            };

            let results_frame = array_field(&fields, "results")
                .ok_or_else(|| "missing 'results' field in vacuum response".to_string())?;

            let result_frames = match results_frame {
                BytesFrame::Array(items) => items,
                other => {
                    return Err(format!("field 'results' must be an array, got {:?}", other));
                }
            };

            let mut shard_results = Vec::with_capacity(result_frames.len());
            for shard_frame in result_frames {
                shard_results.push(parse_shard_result(shard_frame)?);
            }

            Ok(VacuumResponsePayload {
                mode,
                budget_mb,
                dry_run,
                shard_results,
            })
        }
        other => Err(format!("unexpected vacuum response frame: {:?}", other)),
    }
}

fn parse_shard_result(frame: &BytesFrame) -> std::result::Result<ShardVacuumResult, String> {
    let fields = match frame {
        BytesFrame::Array(items) => items,
        other => {
            return Err(format!(
                "each shard result must be an array of fields, got {:?}",
                other
            ));
        }
    };

    let shard_id = required_u64_field(fields, "shard")? as usize;
    let status = required_string_field(fields, "status")?;
    let error = optional_string_field(fields, "error")?;
    let duration_ms = optional_u64_field(fields, "duration_ms")?;
    let estimated_reclaimed_pages = optional_u64_field(fields, "estimated_reclaimed_pages")?;
    let incremental_pages_requested = optional_u64_field(fields, "incremental_pages_requested")?;

    let mut execution_errors = Vec::new();
    if let Some(execution_errors_frame) = array_field(fields, "execution_errors") {
        match execution_errors_frame {
            BytesFrame::Array(items) => {
                for item in items {
                    let msg = frame_to_string(item).ok_or_else(|| {
                        "execution_errors must contain only string values".to_string()
                    })?;
                    execution_errors.push(msg);
                }
            }
            BytesFrame::Null => {}
            other => {
                return Err(format!(
                    "field 'execution_errors' must be an array, got {:?}",
                    other
                ));
            }
        }
    }

    Ok(ShardVacuumResult {
        shard_id,
        status,
        error,
        duration_ms,
        estimated_reclaimed_pages,
        incremental_pages_requested,
        execution_errors,
    })
}

fn frame_to_string(frame: &BytesFrame) -> Option<String> {
    match frame {
        BytesFrame::BulkString(bytes) => Some(String::from_utf8_lossy(bytes.as_ref()).to_string()),
        BytesFrame::SimpleString(text) => Some(String::from_utf8_lossy(text.as_ref()).to_string()),
        _ => None,
    }
}

fn frame_to_u64(frame: &BytesFrame) -> Option<u64> {
    match frame {
        BytesFrame::Integer(value) if *value >= 0 => Some(*value as u64),
        BytesFrame::BulkString(bytes) => {
            String::from_utf8_lossy(bytes.as_ref()).parse::<u64>().ok()
        }
        BytesFrame::SimpleString(text) => {
            String::from_utf8_lossy(text.as_ref()).parse::<u64>().ok()
        }
        _ => None,
    }
}

fn array_field<'a>(items: &'a [BytesFrame], field_name: &str) -> Option<&'a BytesFrame> {
    let mut idx = 0;

    while idx + 1 < items.len() {
        if let Some(name) = frame_to_string(&items[idx]) {
            if name == field_name {
                return Some(&items[idx + 1]);
            }
        }

        idx += 2;
    }

    None
}

fn required_string_field(
    items: &[BytesFrame],
    field_name: &str,
) -> std::result::Result<String, String> {
    let frame =
        array_field(items, field_name).ok_or_else(|| format!("missing '{}' field", field_name))?;

    frame_to_string(frame).ok_or_else(|| format!("field '{}' must be a string", field_name))
}

fn optional_string_field(
    items: &[BytesFrame],
    field_name: &str,
) -> std::result::Result<Option<String>, String> {
    let Some(frame) = array_field(items, field_name) else {
        return Ok(None);
    };

    match frame {
        BytesFrame::Null => Ok(None),
        _ => frame_to_string(frame)
            .map(Some)
            .ok_or_else(|| format!("field '{}' must be a string or null", field_name)),
    }
}

fn required_u64_field(items: &[BytesFrame], field_name: &str) -> std::result::Result<u64, String> {
    let frame =
        array_field(items, field_name).ok_or_else(|| format!("missing '{}' field", field_name))?;

    frame_to_u64(frame)
        .ok_or_else(|| format!("field '{}' must be a non-negative integer", field_name))
}

fn optional_u64_field(
    items: &[BytesFrame],
    field_name: &str,
) -> std::result::Result<Option<u64>, String> {
    let Some(frame) = array_field(items, field_name) else {
        return Ok(None);
    };

    match frame {
        BytesFrame::Null => Ok(None),
        _ => frame_to_u64(frame)
            .map(Some)
            .ok_or_else(|| format!("field '{}' must be an integer or null", field_name)),
    }
}

fn print_vacuum_report(reports: &[NodeVacuumReport]) -> bool {
    let mut node_failures = 0_u64;
    let mut shard_successes = 0_u64;
    let mut shard_failures = 0_u64;

    println!("\nVacuum orchestration report:");

    for report in reports {
        match &report.result {
            Ok(payload) => {
                let mut node_has_failure = false;
                println!(
                    "- node {}: connected ({} ms), mode={}, budget_mb={}, dry_run={}",
                    report.node,
                    report.elapsed.as_millis(),
                    payload.mode,
                    payload.budget_mb,
                    payload.dry_run
                );

                for shard in &payload.shard_results {
                    let shard_ok = shard.status.eq_ignore_ascii_case("ok");
                    if shard_ok {
                        shard_successes += 1;
                    } else {
                        shard_failures += 1;
                        node_has_failure = true;
                    }

                    let mut details = Vec::new();
                    details.push(format!("status={}", shard.status));
                    if let Some(duration_ms) = shard.duration_ms {
                        details.push(format!("duration_ms={}", duration_ms));
                    }
                    if let Some(pages) = shard.incremental_pages_requested {
                        details.push(format!("incremental_pages_requested={}", pages));
                    }
                    if let Some(pages) = shard.estimated_reclaimed_pages {
                        details.push(format!("estimated_reclaimed_pages={}", pages));
                    }
                    if let Some(err) = &shard.error {
                        details.push(format!("error={}", err));
                    }

                    println!("    shard {} -> {}", shard.shard_id, details.join(", "));

                    for err in &shard.execution_errors {
                        println!("      execution_error: {}", err);
                    }
                }

                if node_has_failure {
                    node_failures += 1;
                }
            }
            Err(err) => {
                node_failures += 1;
                println!(
                    "- node {}: failed after {} ms ({})",
                    report.node,
                    report.elapsed.as_millis(),
                    err
                );
            }
        }
    }

    println!(
        "Summary: node_failures={}, shard_successes={}, shard_failures={}",
        node_failures, shard_successes, shard_failures
    );

    node_failures > 0 || shard_failures > 0
}

async fn run_server(config_path: &str) -> Result<()> {
    let cfg = config::Cfg::load(config_path).wrap_err("loading config")?;

    // Initialize metrics if enabled
    let prometheus_handle = if cfg.metrics.as_ref().map_or(false, |m| m.enabled) {
        Some(metrics::init_metrics_exporter().wrap_err("initializing metrics exporter")?)
    } else {
        None
    };

    // This vector will be populated by AppState::new
    let mut shard_receivers = Vec::with_capacity(cfg.num_shards);

    // Initialize AppState, AppState::new will populate shard_receivers
    let shared_state = Arc::new(
        AppState::new(cfg.clone(), &mut shard_receivers)
            .await
            .wrap_err("initializing AppState")?,
    );

    // Initialize metrics
    shared_state.metrics.record_server_startup();

    // Spawn shard writer tasks using the receivers populated by AppState::new
    for (i, receiver) in shard_receivers.into_iter().enumerate() {
        let pool = shared_state.db_pools[i].clone();
        let batch_size = cfg.batch_size.unwrap_or(1);
        let batch_timeout_ms = cfg.batch_timeout_ms.unwrap_or(0);
        let inflight_cache = shared_state.inflight_cache.clone();
        let inflight_hcache = shared_state.inflight_hcache.clone();
        let metrics = shared_state.metrics.clone();
        // Pass the receiver to the spawned task
        tokio::spawn(shard_manager::shard_writer_task(
            i,
            pool,
            receiver,
            batch_size,
            batch_timeout_ms,
            inflight_cache,
            inflight_hcache,
            metrics,
        ));
    }

    // Spawn cleanup tasks for each shard
    let cleanup_interval_secs = 60; // Clean up expired keys every 60 seconds
    for i in 0..cfg.num_shards {
        let pool = shared_state.db_pools[i].clone();
        tokio::spawn(shard_manager::shard_cleanup_task(
            i,
            pool,
            cleanup_interval_secs,
        ));
    }

    // Start HTTP metrics server if enabled
    if let Some(handle) = prometheus_handle {
        let metrics_addr = cfg
            .metrics
            .as_ref()
            .and_then(|m| m.addr.as_ref())
            .map(|s| s.as_str())
            .unwrap_or("0.0.0.0:9090");

        let metrics_addr = metrics_addr.to_string();
        let metrics_handle = handle.clone();

        tokio::spawn(async move {
            if let Err(e) = http_server::run_metrics_server(metrics_handle, &metrics_addr).await {
                tracing::error!("Failed to run metrics server: {}", e);
            }
        });
    }

    // Run Redis server
    if let Err(e) = server::run_redis_server(
        shared_state,
        &cfg.addr.unwrap_or("0.0.0.0:6379".to_string()),
    )
    .await
    {
        return Err(miette::miette!("Failed to run Redis server: {}", e));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk(text: &str) -> BytesFrame {
        BytesFrame::BulkString(text.as_bytes().to_vec().into())
    }

    #[test]
    fn parse_node_list_trims_dedupes_and_normalizes() {
        let nodes = parse_node_list("127.0.0.1:6379, 127.0.0.1:6379 ,0.0.0.0:6380, [::]:6381, ");

        assert_eq!(
            nodes,
            vec![
                "127.0.0.1:6379".to_string(),
                "127.0.0.1:6380".to_string(),
                "[::1]:6381".to_string(),
            ]
        );
    }

    #[test]
    fn resolve_target_nodes_prefers_explicit_nodes() {
        let cfg = config::Cfg {
            data_dir: "./data".to_string(),
            num_shards: 4,
            storage_compression: None,
            async_write: None,
            batch_size: None,
            batch_timeout_ms: None,
            addr: Some("10.0.0.9:6379".to_string()),
            cluster: None,
            metrics: None,
            sqlite: None,
        };

        let nodes = resolve_target_nodes(Some("10.0.0.1:6379,10.0.0.2:6379"), Some(&cfg)).unwrap();

        assert_eq!(
            nodes,
            vec!["10.0.0.1:6379".to_string(), "10.0.0.2:6379".to_string()]
        );
    }

    #[test]
    fn resolve_target_nodes_uses_config_and_default_fallback() {
        let cfg = config::Cfg {
            data_dir: "./data".to_string(),
            num_shards: 4,
            storage_compression: None,
            async_write: None,
            batch_size: None,
            batch_timeout_ms: None,
            addr: Some("0.0.0.0:6379".to_string()),
            cluster: None,
            metrics: None,
            sqlite: None,
        };

        let from_cfg = resolve_target_nodes(None, Some(&cfg)).unwrap();
        assert_eq!(from_cfg, vec!["127.0.0.1:6379".to_string()]);

        let fallback = resolve_target_nodes(None, None).unwrap();
        assert_eq!(fallback, vec!["127.0.0.1:6379".to_string()]);
    }

    #[test]
    fn parse_vacuum_mode_accepts_expected_values() {
        assert!(matches!(
            parse_vacuum_mode("incremental").unwrap(),
            CliVacuumMode::Incremental
        ));
        assert!(matches!(
            parse_vacuum_mode("FULL").unwrap(),
            CliVacuumMode::Full
        ));
        assert!(parse_vacuum_mode("fast").is_err());
    }

    #[test]
    fn parse_vacuum_response_frame_extracts_payload_and_shards() {
        let response = BytesFrame::Array(vec![
            bulk("mode"),
            bulk("incremental"),
            bulk("budget_mb"),
            BytesFrame::Integer(128),
            bulk("dry_run"),
            BytesFrame::Integer(1),
            bulk("results"),
            BytesFrame::Array(vec![BytesFrame::Array(vec![
                bulk("shard"),
                BytesFrame::Integer(3),
                bulk("status"),
                bulk("ok"),
                bulk("error"),
                BytesFrame::Null,
                bulk("duration_ms"),
                BytesFrame::Integer(42),
                bulk("before"),
                BytesFrame::Null,
                bulk("after"),
                BytesFrame::Null,
                bulk("incremental_pages_requested"),
                BytesFrame::Integer(10),
                bulk("estimated_reclaimed_pages"),
                BytesFrame::Integer(7),
                bulk("execution_errors"),
                BytesFrame::Array(vec![]),
            ])]),
        ]);

        let parsed = parse_vacuum_response_frame(response).unwrap();

        assert_eq!(parsed.mode, "incremental");
        assert_eq!(parsed.budget_mb, 128);
        assert!(parsed.dry_run);
        assert_eq!(parsed.shard_results.len(), 1);
        assert_eq!(parsed.shard_results[0].shard_id, 3);
        assert_eq!(parsed.shard_results[0].status, "ok");
        assert_eq!(parsed.shard_results[0].duration_ms, Some(42));
        assert_eq!(
            parsed.shard_results[0].incremental_pages_requested,
            Some(10)
        );
    }

    #[test]
    fn parse_vacuum_response_frame_surfaces_error_reply() {
        let err = parse_vacuum_response_frame(BytesFrame::Error("ERR bad request".into()))
            .expect_err("error frame should fail parsing");

        assert!(err.contains("node returned error"));
    }
}

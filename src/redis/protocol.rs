use crate::cluster::REDIS_CLUSTER_SLOTS;
use bytes::{Bytes, BytesMut};
use redis_protocol::resp2::{decode::decode_bytes_mut, encode::extend_encode, types::BytesFrame};

/// Longest relative TTL accepted, as in Redis (the TTL in ms must fit in i64).
/// Also keeps `now + ttl` from overflowing.
const MAX_TTL_SECS: u64 = (i64::MAX / 1000) as u64;

/// Max arguments in one request, same as Redis' multibulk limit.
const MAX_REQUEST_ARGS: usize = 1024 * 1024;
/// Longest possible `*N` / `$len` header line, including its CRLF.
const MAX_HEADER_LEN: usize = 32;

/// Redis protocol data types (re-export from redis-protocol crate)
pub type RespValue = BytesFrame;

/// Expiration options for commands
#[derive(Debug, Clone, PartialEq)]
pub enum ExpireOption {
    Ex(u64),   // seconds
    Px(u64),   // milliseconds
    ExAt(i64), // unix timestamp in seconds
    PxAt(i64), // unix timestamp in milliseconds
    KeepTtl,
}

/// Condition options for HEXPIRE command
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HExpireCondition {
    /// Set expiration only when the field has no expiration
    Nx,
    /// Set expiration only when the field has an existing expiration
    Xx,
    /// Set expiration only when the new expiration is greater than current one
    Gt,
    /// Set expiration only when the new expiration is less than current one
    Lt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VacuumShardTarget {
    Shard(usize),
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VacuumCommandMode {
    Incremental,
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterFailoverMode {
    Default,
    Force,
    Takeover,
}

/// Commands supported by Blobasaur
#[derive(Debug, Clone, PartialEq)]
pub enum RedisCommand {
    Get {
        key: String,
    },
    Set {
        key: String,
        value: Bytes,
        ttl_seconds: Option<u64>,
    },
    Del {
        keys: Vec<String>,
    },
    Exists {
        key: String,
    },
    HGet {
        namespace: String,
        key: String,
    },
    HSet {
        namespace: String,
        key: String,
        value: Bytes,
    },
    HSetEx {
        key: String,
        fnx: bool,
        fxx: bool,
        expire_option: Option<ExpireOption>,
        fields: Vec<(String, Bytes)>,
    },
    HDel {
        namespace: String,
        key: String,
    },
    HExists {
        namespace: String,
        key: String,
    },
    HExpire {
        key: String,
        seconds: i64,
        condition: Option<HExpireCondition>,
        fields: Vec<String>,
    },
    HExpireAt {
        key: String,
        unix_time_seconds: i64,
        condition: Option<HExpireCondition>,
        fields: Vec<String>,
    },
    Ping {
        message: Option<String>,
    },
    Info {
        section: Option<String>,
    },
    Command {
        args: Vec<String>,
    },
    // Cluster commands
    ClusterMyId,
    ClusterNodes,
    ClusterInfo,
    ClusterSlots,
    ClusterShards,
    ClusterAddSlots {
        slots: Vec<u16>,
    },
    ClusterDelSlots {
        slots: Vec<u16>,
    },
    ClusterKeySlot {
        key: String,
    },
    ClusterFailover {
        mode: ClusterFailoverMode,
    },
    ClusterReplicate {
        node_id: String,
    },
    Ttl {
        key: String,
    },
    Expire {
        key: String,
        seconds: u64,
    },
    BlobasaurVacuum {
        target: VacuumShardTarget,
        mode: VacuumCommandMode,
        budget_mb: u64,
        dry_run: bool,
    },
    /// Per shard `[shard, replid, lsn]`; see `replication::shard_watermark`.
    BlobasaurWatermark,
    /// A replica opening the replication stream of one shard; see
    /// `replication::serve_sync`. `replid` is empty if the replica has none.
    BlobasaurSync {
        shard: usize,
        replid: String,
        lsn: u64,
        num_shards: usize,
        compression: String,
    },
    /// Replica election vote request sent to slot-owning primaries.
    BlobasaurFailoverAuth(crate::cluster::FailoverAuthRequest),
    /// Coordinated manual failover start sent by a replica to its primary.
    BlobasaurMfStart {
        replica_id: String,
    },
    Hello {
        protover: Option<u64>,
    },
    Client {
        subcommand: String,
    },
    ClientGetName,
    ClientSetName(Bytes),
    Quit,
    /// Lets a replica serve this connection's reads.
    ReadOnly,
    ReadWrite,
    Unknown(String),
}

impl RedisCommand {
    /// Get the command name for metrics
    pub fn name(&self) -> String {
        match self {
            RedisCommand::Get { .. } => "GET".to_string(),
            RedisCommand::Set { .. } => "SET".to_string(),
            RedisCommand::Del { .. } => "DEL".to_string(),
            RedisCommand::Exists { .. } => "EXISTS".to_string(),
            RedisCommand::HGet { .. } => "HGET".to_string(),
            RedisCommand::HSet { .. } => "HSET".to_string(),
            RedisCommand::HSetEx { .. } => "HSETEX".to_string(),
            RedisCommand::HDel { .. } => "HDEL".to_string(),
            RedisCommand::HExists { .. } => "HEXISTS".to_string(),
            RedisCommand::HExpire { .. } => "HEXPIRE".to_string(),
            RedisCommand::HExpireAt { .. } => "HEXPIREAT".to_string(),
            RedisCommand::Ping { .. } => "PING".to_string(),
            RedisCommand::Info { .. } => "INFO".to_string(),
            RedisCommand::Command { .. } => "COMMAND".to_string(),
            RedisCommand::ClusterMyId => "CLUSTER MYID".to_string(),
            RedisCommand::ClusterNodes => "CLUSTER NODES".to_string(),
            RedisCommand::ClusterInfo => "CLUSTER INFO".to_string(),
            RedisCommand::ClusterSlots => "CLUSTER SLOTS".to_string(),
            RedisCommand::ClusterShards => "CLUSTER SHARDS".to_string(),
            RedisCommand::ClusterAddSlots { .. } => "CLUSTER ADDSLOTS".to_string(),
            RedisCommand::ClusterDelSlots { .. } => "CLUSTER DELSLOTS".to_string(),
            RedisCommand::ClusterKeySlot { .. } => "CLUSTER KEYSLOT".to_string(),
            RedisCommand::ClusterFailover { .. } => "CLUSTER FAILOVER".to_string(),
            RedisCommand::ClusterReplicate { .. } => "CLUSTER REPLICATE".to_string(),
            RedisCommand::Ttl { .. } => "TTL".to_string(),
            RedisCommand::Expire { .. } => "EXPIRE".to_string(),
            RedisCommand::BlobasaurVacuum { .. } => "BLOBASAUR.VACUUM".to_string(),
            RedisCommand::BlobasaurWatermark => "BLOBASAUR.WATERMARK".to_string(),
            RedisCommand::BlobasaurSync { .. } => "BLOBASAUR.SYNC".to_string(),
            RedisCommand::BlobasaurFailoverAuth(_) => "BLOBASAUR.FAILOVER-AUTH".to_string(),
            RedisCommand::BlobasaurMfStart { .. } => "BLOBASAUR.MFSTART".to_string(),
            RedisCommand::Hello { .. } => "HELLO".to_string(),
            RedisCommand::Client { .. }
            | RedisCommand::ClientGetName
            | RedisCommand::ClientSetName(_) => "CLIENT".to_string(),
            RedisCommand::Quit => "QUIT".to_string(),
            RedisCommand::ReadOnly => "READONLY".to_string(),
            RedisCommand::ReadWrite => "READWRITE".to_string(),
            RedisCommand::Unknown(cmd) => cmd.clone(),
        }
    }
}

/// Parse error types
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("Incomplete data")]
    Incomplete,
    #[error("Invalid protocol: {0}")]
    Invalid(String),
}

/// Decodes one client request from the front of `buf`.
///
/// Clients only send flat arrays of bulk strings (`*N\r\n` then N x
/// `$len\r\n<data>\r\n`), so anything else is a protocol error: nesting,
/// other frame types, a missing CRLF, or a request larger than `max_size`.
/// Nothing recurses and payloads are never copied or rescanned: on success the
/// request is split off `buf` and each argument is a slice of it.
///
/// Returns `Ok(None)` until the whole request has arrived.
pub fn decode_request(
    buf: &mut BytesMut,
    max_size: usize,
) -> Result<Option<BytesFrame>, ParseError> {
    let mut pos = 0;
    let Some(argc) = read_header(buf, &mut pos, b'*')? else {
        return Ok(None);
    };
    if argc > MAX_REQUEST_ARGS {
        return Err(ParseError::Invalid("invalid multibulk length".to_string()));
    }

    let mut args = Vec::with_capacity(argc.min(64));
    for _ in 0..argc {
        let Some(len) = read_header(buf, &mut pos, b'$')? else {
            return Ok(None);
        };
        let end = pos.saturating_add(len).saturating_add(2);
        if end > max_size {
            return Err(ParseError::Invalid(format!(
                "request exceeds max_request_size_mb ({} bytes)",
                max_size
            )));
        }
        if buf.len() < end {
            return Ok(None);
        }
        if &buf[end - 2..end] != b"\r\n" {
            return Err(ParseError::Invalid(
                "expected CRLF after bulk string".to_string(),
            ));
        }
        args.push(pos..end - 2);
        pos = end;
    }

    let request = buf.split_to(pos).freeze();
    Ok(Some(BytesFrame::Array(
        args.into_iter()
            .map(|range| BytesFrame::BulkString(request.slice(range)))
            .collect(),
    )))
}

/// Reads a `<prefix><decimal>\r\n` header at `pos` and advances past it.
fn read_header(buf: &[u8], pos: &mut usize, prefix: u8) -> Result<Option<usize>, ParseError> {
    let rest = &buf[*pos..];
    let Some(&first) = rest.first() else {
        return Ok(None);
    };
    if first != prefix {
        return Err(ParseError::Invalid(format!(
            "expected '{}', got '{}'",
            prefix as char,
            first.escape_ascii()
        )));
    }
    let Some(cr) = rest.iter().take(MAX_HEADER_LEN).position(|&b| b == b'\r') else {
        return if rest.len() >= MAX_HEADER_LEN {
            Err(ParseError::Invalid("header line too long".to_string()))
        } else {
            Ok(None)
        };
    };
    if rest.len() < cr + 2 {
        return Ok(None);
    }
    let digits = &rest[1..cr];
    if rest[cr + 1] != b'\n' || digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return Err(ParseError::Invalid(format!(
            "invalid {} length",
            if prefix == b'*' { "multibulk" } else { "bulk" }
        )));
    }
    // At most MAX_HEADER_LEN digits, so this only fails on overflow.
    let value = std::str::from_utf8(digits)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| ParseError::Invalid("length out of range".to_string()))?;
    *pos += cr + 2;
    Ok(Some(value))
}

/// Parse a single RESP message and return both the parsed value and remaining bytes.
/// Generic decoder for replies from our own nodes (vacuum CLI); client requests
/// go through [`decode_request`].
pub fn parse_resp_with_remaining(input: &[u8]) -> Result<(RespValue, &[u8]), ParseError> {
    let mut bytes_mut = bytes::BytesMut::from(input);

    match decode_bytes_mut(&mut bytes_mut) {
        Ok(Some((frame, consumed, _))) => {
            let remaining = &input[consumed..];
            Ok((frame, remaining))
        }
        Ok(None) => Err(ParseError::Incomplete),
        Err(e) => Err(ParseError::Invalid(format!("Parse error: {:?}", e))),
    }
}

/// Parse a Redis command from RESP value
pub fn parse_command(resp: RespValue) -> Result<RedisCommand, ParseError> {
    match resp {
        BytesFrame::Array(elements) if !elements.is_empty() => parse_command_array(elements),
        BytesFrame::Array(_) => Err(ParseError::Invalid("Empty command array".to_string())),
        _ => Err(ParseError::Invalid("Commands must be arrays".to_string())),
    }
}

/// Parse command from array of RESP values
fn parse_command_array(elements: Vec<BytesFrame>) -> Result<RedisCommand, ParseError> {
    let command_name = match &elements[0] {
        BytesFrame::BulkString(data) => String::from_utf8_lossy(data).to_uppercase(),
        BytesFrame::SimpleString(s) => String::from_utf8_lossy(s).to_uppercase(),
        _ => {
            return Err(ParseError::Invalid(
                "Command name must be a string".to_string(),
            ));
        }
    };

    match command_name.as_str() {
        "GET" => {
            if elements.len() != 2 {
                return Err(ParseError::Invalid(
                    "GET requires exactly 1 argument".to_string(),
                ));
            }
            let key = extract_string(&elements[1])?;
            Ok(RedisCommand::Get { key })
        }
        "SET" => {
            if elements.len() < 3 || elements.len() > 5 {
                return Err(ParseError::Invalid(
                    "SET requires 2-4 arguments".to_string(),
                ));
            }
            let key = extract_string(&elements[1])?;
            let value = extract_bytes(&elements[2])?;

            let mut ttl_seconds = None;

            // Parse optional TTL arguments (EX seconds or PX milliseconds)
            let mut i = 3;
            while i < elements.len() {
                let option = extract_string(&elements[i])?.to_uppercase();
                match option.as_str() {
                    "EX" | "PX" => {
                        if i + 1 >= elements.len() {
                            return Err(ParseError::Invalid(format!("{option} requires a value")));
                        }
                        let ttl = redis_integer(&elements[i + 1])?;
                        if ttl <= 0 {
                            return Err(invalid_expire_time("set"));
                        }
                        ttl_seconds = Some(ttl_secs(ttl as u64, option == "PX", "set")?);
                        i += 2;
                    }
                    _ => {
                        return Err(ParseError::Invalid(format!(
                            "Unknown SET option: {}",
                            option
                        )));
                    }
                }
            }

            Ok(RedisCommand::Set {
                key,
                value,
                ttl_seconds,
            })
        }
        "DEL" => {
            if elements.len() < 2 {
                return Err(ParseError::Invalid(
                    "DEL requires at least 1 argument".to_string(),
                ));
            }
            let mut keys = Vec::new();
            for key_element in &elements[1..] {
                keys.push(extract_string(key_element)?);
            }
            Ok(RedisCommand::Del { keys })
        }
        "EXISTS" => {
            if elements.len() != 2 {
                return Err(ParseError::Invalid(
                    "EXISTS requires exactly 1 argument".to_string(),
                ));
            }
            let key = extract_string(&elements[1])?;
            Ok(RedisCommand::Exists { key })
        }
        "PING" => {
            if elements.len() > 2 {
                return Err(ParseError::Invalid(
                    "wrong number of arguments for 'ping' command".into(),
                ));
            }
            let message = if elements.len() > 1 {
                Some(extract_string(&elements[1])?)
            } else {
                None
            };
            Ok(RedisCommand::Ping { message })
        }
        "INFO" => {
            let section = if elements.len() > 1 {
                Some(extract_string(&elements[1])?)
            } else {
                None
            };
            Ok(RedisCommand::Info { section })
        }
        "COMMAND" => {
            let args = elements[1..]
                .iter()
                .map(extract_string)
                .collect::<Result<_, _>>()?;
            Ok(RedisCommand::Command { args })
        }
        "HGET" => {
            if elements.len() != 3 {
                return Err(ParseError::Invalid(
                    "HGET requires exactly 2 arguments".to_string(),
                ));
            }
            let namespace = extract_string(&elements[1])?;
            let key = extract_string(&elements[2])?;
            Ok(RedisCommand::HGet { namespace, key })
        }
        "HSET" => {
            if elements.len() != 4 {
                return Err(ParseError::Invalid(
                    "HSET requires exactly 3 arguments".to_string(),
                ));
            }
            let namespace = extract_string(&elements[1])?;
            let key = extract_string(&elements[2])?;
            let value = extract_bytes(&elements[3])?;
            Ok(RedisCommand::HSet {
                namespace,
                key,
                value,
            })
        }
        "HSETEX" => {
            if elements.len() < 5 {
                return Err(ParseError::Invalid(
                    "HSETEX requires at least key, FIELDS, numfields, and one field-value pair"
                        .to_string(),
                ));
            }

            let key = extract_string(&elements[1])?;
            let mut idx = 2;
            let mut fnx = false;
            let mut fxx = false;
            let mut expire_option = None;
            let mut fields = None;

            // Parse options
            while idx < elements.len() {
                let arg = extract_string(&elements[idx])?;
                let opt = arg.to_uppercase();
                // As in Redis, a condition or expiry given twice is an error
                // rather than the last one silently winning.
                match opt.as_str() {
                    "FNX" | "FXX" if fnx || fxx => {
                        return Err(ParseError::Invalid(
                            "Only one of FXX or FNX arguments can be specified".to_string(),
                        ));
                    }
                    "EX" | "PX" | "EXAT" | "PXAT" | "KEEPTTL" if expire_option.is_some() => {
                        return Err(ParseError::Invalid(
                            "Only one of EX, PX, EXAT, PXAT or KEEPTTL arguments can be specified"
                                .to_string(),
                        ));
                    }
                    _ => {}
                }
                match opt.as_str() {
                    "FNX" => {
                        fnx = true;
                        idx += 1;
                    }
                    "FXX" => {
                        fxx = true;
                        idx += 1;
                    }
                    "EX" | "PX" | "EXAT" | "PXAT" => {
                        let value = elements.get(idx + 1).ok_or_else(|| {
                            ParseError::Invalid(format!("{opt} requires a value"))
                        })?;
                        let millis = opt.starts_with("PX");
                        let relative = !opt.ends_with("AT");
                        let time = hash_expiry_time(value, millis, relative, "hsetex")?;
                        expire_option = Some(match (millis, relative) {
                            (false, true) => ExpireOption::Ex(time as u64),
                            (true, true) => ExpireOption::Px(time as u64),
                            (false, false) => ExpireOption::ExAt(time),
                            (true, false) => ExpireOption::PxAt(time),
                        });
                        idx += 2;
                    }
                    "KEEPTTL" => {
                        expire_option = Some(ExpireOption::KeepTtl);
                        idx += 1;
                    }
                    // Like Redis, options may also follow the fields, so parsing
                    // goes on after them.
                    "FIELDS" => {
                        if fields.is_some() {
                            return Err(fields_given_twice());
                        }
                        idx += 1;

                        // Get number of fields
                        if idx >= elements.len() {
                            return Err(ParseError::Invalid(
                                "HSETEX requires field count after FIELDS".to_string(),
                            ));
                        }
                        let num_fields =
                            field_count(&elements[idx], "invalid number of fields")? as usize;
                        idx += 1;

                        // Validate against remaining args before allocating.
                        if num_fields > (elements.len() - idx) / 2 {
                            return Err(ParseError::Invalid(
                                "Not enough field-value pairs".to_string(),
                            ));
                        }

                        // Parse field-value pairs
                        let mut pairs = Vec::with_capacity(num_fields);
                        for _ in 0..num_fields {
                            let field = extract_string(&elements[idx])?;
                            let value = extract_bytes(&elements[idx + 1])?;
                            pairs.push((field, value));
                            idx += 2;
                        }
                        fields = Some(pairs);
                    }
                    _ => return Err(unknown_argument(&arg)),
                }
            }

            let Some(fields) = fields else {
                return Err(ParseError::Invalid(
                    "HSETEX requires FIELDS keyword".to_string(),
                ));
            };

            Ok(RedisCommand::HSetEx {
                key,
                fnx,
                fxx,
                expire_option,
                fields,
            })
        }
        "HDEL" => {
            if elements.len() != 3 {
                return Err(ParseError::Invalid(
                    "HDEL requires exactly 2 arguments".to_string(),
                ));
            }
            let namespace = extract_string(&elements[1])?;
            let key = extract_string(&elements[2])?;
            Ok(RedisCommand::HDel { namespace, key })
        }
        "HEXISTS" => {
            if elements.len() != 3 {
                return Err(ParseError::Invalid(
                    "HEXISTS requires exactly 2 arguments".to_string(),
                ));
            }
            let namespace = extract_string(&elements[1])?;
            let key = extract_string(&elements[2])?;
            Ok(RedisCommand::HExists { namespace, key })
        }
        "HEXPIRE" => {
            // HEXPIRE key seconds [NX | XX | GT | LT] FIELDS numfields field [field ...]
            // Minimum: HEXPIRE key seconds FIELDS 1 field = 6 elements
            if elements.len() < 6 {
                return Err(ParseError::Invalid(
                    "HEXPIRE requires at least key, seconds, FIELDS, numfields, and one field"
                        .to_string(),
                ));
            }

            let key = extract_string(&elements[1])?;
            let seconds = hash_expiry_time(&elements[2], false, true, "hexpire")?;
            let (condition, fields) = parse_hexpire_args(&elements, "HEXPIRE")?;

            Ok(RedisCommand::HExpire {
                key,
                seconds,
                condition,
                fields,
            })
        }
        "HEXPIREAT" => {
            // HEXPIREAT key unix-time-seconds [NX | XX | GT | LT] FIELDS numfields field [field ...]
            // Minimum: HEXPIREAT key timestamp FIELDS 1 field = 6 elements
            if elements.len() < 6 {
                return Err(ParseError::Invalid(
                    "HEXPIREAT requires at least key, unix-time-seconds, FIELDS, numfields, and one field"
                        .to_string(),
                ));
            }

            let key = extract_string(&elements[1])?;
            let unix_time_seconds = hash_expiry_time(&elements[2], false, false, "hexpireat")?;
            let (condition, fields) = parse_hexpire_args(&elements, "HEXPIREAT")?;

            Ok(RedisCommand::HExpireAt {
                key,
                unix_time_seconds,
                condition,
                fields,
            })
        }
        "CLUSTER" => {
            if elements.len() < 2 {
                return Err(ParseError::Invalid(
                    "CLUSTER requires at least 1 argument".to_string(),
                ));
            }
            let subcommand = extract_string(&elements[1])?.to_uppercase();
            match subcommand.as_str() {
                "MYID" => {
                    if elements.len() != 2 {
                        return Err(ParseError::Invalid(
                            "CLUSTER MYID takes no arguments".to_string(),
                        ));
                    }
                    Ok(RedisCommand::ClusterMyId)
                }
                "NODES" => Ok(RedisCommand::ClusterNodes),
                "INFO" => Ok(RedisCommand::ClusterInfo),
                "SLOTS" => Ok(RedisCommand::ClusterSlots),
                "SHARDS" => {
                    if elements.len() != 2 {
                        return Err(ParseError::Invalid(
                            "CLUSTER SHARDS takes no arguments".to_string(),
                        ));
                    }
                    Ok(RedisCommand::ClusterShards)
                }
                "ADDSLOTS" => {
                    if elements.len() < 3 {
                        return Err(ParseError::Invalid(
                            "CLUSTER ADDSLOTS requires at least 1 slot".to_string(),
                        ));
                    }
                    let mut slots = Vec::new();
                    for slot_arg in &elements[2..] {
                        let slot_str = extract_string(slot_arg)?;
                        let slot = slot_str
                            .parse::<u16>()
                            .ok()
                            .filter(|slot| *slot < REDIS_CLUSTER_SLOTS)
                            .ok_or_else(|| {
                                ParseError::Invalid(format!("Invalid slot number: {}", slot_str))
                            })?;
                        slots.push(slot);
                    }
                    Ok(RedisCommand::ClusterAddSlots { slots })
                }
                "DELSLOTS" => {
                    if elements.len() < 3 {
                        return Err(ParseError::Invalid(
                            "CLUSTER DELSLOTS requires at least 1 slot".to_string(),
                        ));
                    }
                    let mut slots = Vec::new();
                    for slot_arg in &elements[2..] {
                        let slot_str = extract_string(slot_arg)?;
                        let slot = slot_str
                            .parse::<u16>()
                            .ok()
                            .filter(|slot| *slot < REDIS_CLUSTER_SLOTS)
                            .ok_or_else(|| {
                                ParseError::Invalid(format!("Invalid slot number: {}", slot_str))
                            })?;
                        slots.push(slot);
                    }
                    Ok(RedisCommand::ClusterDelSlots { slots })
                }
                "KEYSLOT" => {
                    if elements.len() != 3 {
                        return Err(ParseError::Invalid(
                            "CLUSTER KEYSLOT requires exactly 1 argument".to_string(),
                        ));
                    }
                    let key = extract_string(&elements[2])?;
                    Ok(RedisCommand::ClusterKeySlot { key })
                }
                "FAILOVER" => match elements.len() {
                    2 => Ok(RedisCommand::ClusterFailover {
                        mode: ClusterFailoverMode::Default,
                    }),
                    3 => {
                        let option = extract_string(&elements[2])?.to_uppercase();
                        let mode = match option.as_str() {
                            "FORCE" => ClusterFailoverMode::Force,
                            "TAKEOVER" => ClusterFailoverMode::Takeover,
                            _ => {
                                return Err(ParseError::Invalid(
                                    "CLUSTER FAILOVER option must be FORCE or TAKEOVER".to_string(),
                                ));
                            }
                        };
                        Ok(RedisCommand::ClusterFailover { mode })
                    }
                    _ => Err(ParseError::Invalid(
                        "wrong number of arguments for 'cluster|failover' command".to_string(),
                    )),
                },
                "REPLICATE" => {
                    if elements.len() != 3 {
                        return Err(ParseError::Invalid(
                            "CLUSTER REPLICATE requires exactly 1 argument".to_string(),
                        ));
                    }
                    let node_id = extract_string(&elements[2])?;
                    if node_id.is_empty() {
                        return Err(ParseError::Invalid(
                            "CLUSTER REPLICATE node id cannot be empty".to_string(),
                        ));
                    }
                    Ok(RedisCommand::ClusterReplicate { node_id })
                }
                _ => Ok(RedisCommand::Unknown(format!("CLUSTER {}", subcommand))),
            }
        }
        "TTL" => {
            if elements.len() != 2 {
                return Err(ParseError::Invalid(
                    "TTL requires exactly 1 argument".to_string(),
                ));
            }
            let key = extract_string(&elements[1])?;
            Ok(RedisCommand::Ttl { key })
        }
        "EXPIRE" => {
            if elements.len() != 3 {
                return Err(ParseError::Invalid(
                    "EXPIRE requires exactly 2 arguments".to_string(),
                ));
            }
            let key = extract_string(&elements[1])?;
            let seconds = redis_integer(&elements[2])?;
            if seconds.unsigned_abs() > MAX_TTL_SECS {
                return Err(invalid_expire_time("expire"));
            }
            // Like Redis, 0 or less expires the key immediately.
            let seconds = seconds.max(0) as u64;
            Ok(RedisCommand::Expire { key, seconds })
        }
        "BLOBASAUR.VACUUM" => parse_blobasaur_vacuum_command(&elements),
        "BLOBASAUR.SYNC" => parse_blobasaur_sync_command(&elements),
        "BLOBASAUR.FAILOVER-AUTH" => parse_blobasaur_failover_auth_command(&elements),
        "BLOBASAUR.MFSTART" => parse_blobasaur_mfstart_command(&elements),
        "BLOBASAUR.WATERMARK" => {
            if elements.len() != 1 {
                return Err(ParseError::Invalid(
                    "BLOBASAUR.WATERMARK takes no arguments".to_string(),
                ));
            }
            Ok(RedisCommand::BlobasaurWatermark)
        }
        "HELLO" => {
            let protover = if elements.len() > 1 {
                let ver_str = extract_string(&elements[1])?;
                let ver = ver_str.parse::<u64>().map_err(|_| {
                    ParseError::Invalid(format!("Invalid protocol version: {}", ver_str))
                })?;
                Some(ver)
            } else {
                None
            };
            Ok(RedisCommand::Hello { protover })
        }
        "CLIENT" => {
            let subcommand = if elements.len() > 1 {
                extract_string(&elements[1])?.to_uppercase()
            } else {
                String::new()
            };
            match subcommand.as_str() {
                "GETNAME" if elements.len() == 2 => Ok(RedisCommand::ClientGetName),
                "SETNAME" if elements.len() == 3 => {
                    let name = extract_bytes(&elements[2])?;
                    if !name.iter().all(u8::is_ascii_graphic) {
                        return Err(ParseError::Invalid(
                            "Client names cannot contain spaces, newlines or special characters."
                                .into(),
                        ));
                    }
                    Ok(RedisCommand::ClientSetName(name))
                }
                "SETNAME" | "GETNAME" => Err(ParseError::Invalid(format!(
                    "wrong number of arguments for 'client|{}' command",
                    subcommand.to_ascii_lowercase()
                ))),
                _ => Ok(RedisCommand::Client { subcommand }),
            }
        }
        "QUIT" => Ok(RedisCommand::Quit),
        "READONLY" => Ok(RedisCommand::ReadOnly),
        "READWRITE" => Ok(RedisCommand::ReadWrite),
        _ => Ok(RedisCommand::Unknown(command_name)),
    }
}

fn parse_blobasaur_sync_command(elements: &[BytesFrame]) -> Result<RedisCommand, ParseError> {
    if elements.len() != 6 {
        return Err(ParseError::Invalid(
            "BLOBASAUR.SYNC syntax: BLOBASAUR.SYNC <shard> <replid> <lsn> <num_shards> <compression>"
                .to_string(),
        ));
    }
    fn number<T: std::str::FromStr>(value: &BytesFrame, what: &str) -> Result<T, ParseError> {
        let text = extract_string(value)?;
        text.parse()
            .map_err(|_| ParseError::Invalid(format!("Invalid {what} '{text}'")))
    }
    Ok(RedisCommand::BlobasaurSync {
        shard: number(&elements[1], "shard")?,
        replid: extract_string(&elements[2])?,
        lsn: number(&elements[3], "lsn")?,
        num_shards: number(&elements[4], "num_shards")?,
        compression: extract_string(&elements[5])?,
    })
}

fn parse_blobasaur_failover_auth_command(
    elements: &[BytesFrame],
) -> Result<RedisCommand, ParseError> {
    if elements.len() != 6 && elements.len() != 7 {
        return Err(ParseError::Invalid(
            "BLOBASAUR.FAILOVER-AUTH syntax: BLOBASAUR.FAILOVER-AUTH <candidate> <epoch> <failed-primary> <primary-config-epoch> <slot-ranges> [MANUAL]"
                .to_string(),
        ));
    }
    let candidate = extract_string(&elements[1])?;
    if candidate.is_empty() {
        return Err(ParseError::Invalid("candidate cannot be empty".to_string()));
    }
    let epoch_str = extract_string(&elements[2])?;
    let epoch: u64 = epoch_str
        .parse()
        .map_err(|_| ParseError::Invalid(format!("Invalid epoch '{epoch_str}'")))?;
    let failed_primary = extract_string(&elements[3])?;
    if failed_primary.is_empty() {
        return Err(ParseError::Invalid(
            "failed-primary cannot be empty".to_string(),
        ));
    }
    let p_epoch_str = extract_string(&elements[4])?;
    let primary_config_epoch: u64 = p_epoch_str.parse().map_err(|_| {
        ParseError::Invalid(format!("Invalid primary-config-epoch '{p_epoch_str}'"))
    })?;
    let slot_ranges = extract_string(&elements[5])?;
    let slots = crate::cluster::parse_slot_ranges(&slot_ranges).map_err(ParseError::Invalid)?;
    let manual = if elements.len() == 7 {
        let flag = extract_string(&elements[6])?;
        if !flag.eq_ignore_ascii_case("MANUAL") {
            return Err(ParseError::Invalid(format!(
                "Invalid BLOBASAUR.FAILOVER-AUTH flag '{flag}', expected MANUAL"
            )));
        }
        true
    } else {
        false
    };
    Ok(RedisCommand::BlobasaurFailoverAuth(
        crate::cluster::FailoverAuthRequest {
            candidate,
            epoch,
            failed_primary,
            primary_config_epoch,
            slots,
            manual,
        },
    ))
}

fn parse_blobasaur_mfstart_command(elements: &[BytesFrame]) -> Result<RedisCommand, ParseError> {
    if elements.len() != 2 {
        return Err(ParseError::Invalid(
            "BLOBASAUR.MFSTART syntax: BLOBASAUR.MFSTART <replica-id>".to_string(),
        ));
    }
    let replica_id = extract_string(&elements[1])?;
    if replica_id.is_empty() {
        return Err(ParseError::Invalid(
            "replica-id cannot be empty".to_string(),
        ));
    }
    Ok(RedisCommand::BlobasaurMfStart { replica_id })
}

fn parse_blobasaur_vacuum_command(elements: &[BytesFrame]) -> Result<RedisCommand, ParseError> {
    if elements.len() != 7 && elements.len() != 8 {
        return Err(ParseError::Invalid(
            "BLOBASAUR.VACUUM syntax: BLOBASAUR.VACUUM SHARD <id|ALL> MODE <incremental|full> BUDGET_MB <n> [DRYRUN]"
                .to_string(),
        ));
    }

    let shard_keyword = extract_string(&elements[1])?;
    if !shard_keyword.eq_ignore_ascii_case("SHARD") {
        return Err(ParseError::Invalid(
            "BLOBASAUR.VACUUM requires SHARD keyword".to_string(),
        ));
    }

    let shard_token = extract_string(&elements[2])?;
    let target = if shard_token.eq_ignore_ascii_case("ALL") {
        VacuumShardTarget::All
    } else {
        let shard_id = shard_token.parse::<usize>().map_err(|_| {
            ParseError::Invalid(format!(
                "Invalid shard id '{}': expected non-negative integer or ALL",
                shard_token
            ))
        })?;
        VacuumShardTarget::Shard(shard_id)
    };

    let mode_keyword = extract_string(&elements[3])?;
    if !mode_keyword.eq_ignore_ascii_case("MODE") {
        return Err(ParseError::Invalid(
            "BLOBASAUR.VACUUM requires MODE keyword".to_string(),
        ));
    }

    let mode_token = extract_string(&elements[4])?;
    let mode = if mode_token.eq_ignore_ascii_case("incremental") {
        VacuumCommandMode::Incremental
    } else if mode_token.eq_ignore_ascii_case("full") {
        VacuumCommandMode::Full
    } else {
        return Err(ParseError::Invalid(format!(
            "Invalid MODE '{}': expected incremental or full",
            mode_token
        )));
    };

    let budget_keyword = extract_string(&elements[5])?;
    if !budget_keyword.eq_ignore_ascii_case("BUDGET_MB") {
        return Err(ParseError::Invalid(
            "BLOBASAUR.VACUUM requires BUDGET_MB keyword".to_string(),
        ));
    }

    let budget_token = extract_string(&elements[6])?;
    let budget_mb = budget_token.parse::<u64>().map_err(|_| {
        ParseError::Invalid(format!(
            "Invalid BUDGET_MB '{}': expected positive integer",
            budget_token
        ))
    })?;

    if budget_mb == 0 {
        return Err(ParseError::Invalid(
            "BUDGET_MB must be greater than 0".to_string(),
        ));
    }

    let dry_run = if elements.len() == 8 {
        let dryrun_token = extract_string(&elements[7])?;
        if !dryrun_token.eq_ignore_ascii_case("DRYRUN") {
            return Err(ParseError::Invalid(format!(
                "Unexpected trailing token '{}': expected DRYRUN",
                dryrun_token
            )));
        }
        true
    } else {
        false
    };

    Ok(RedisCommand::BlobasaurVacuum {
        target,
        mode,
        budget_mb,
        dry_run,
    })
}

/// Extract string from RESP value
/// Keys are stored as TEXT, so non-UTF-8 input is rejected rather than lossily
/// converted: lossy conversion maps different binary keys to the same key.
fn extract_string(value: &BytesFrame) -> Result<String, ParseError> {
    match value {
        BytesFrame::BulkString(data) | BytesFrame::SimpleString(data) => {
            std::str::from_utf8(data).map(str::to_owned).map_err(|_| {
                ParseError::Invalid("keys and string arguments must be valid UTF-8".to_string())
            })
        }
        BytesFrame::Null => Err(ParseError::Invalid(
            "Cannot use null as string argument".to_string(),
        )),
        _ => Err(ParseError::Invalid("Expected string argument".to_string())),
    }
}

fn redis_integer(value: &BytesFrame) -> Result<i64, ParseError> {
    let text = extract_string(value)?;
    let invalid = || ParseError::Invalid("value is not an integer or out of range".into());
    if text.starts_with('+') || text.starts_with("-0") || (text.starts_with('0') && text.len() > 1)
    {
        return Err(invalid());
    }
    text.parse().map_err(|_| invalid())
}

fn field_count(value: &BytesFrame, error: &str) -> Result<i64, ParseError> {
    redis_integer(value)
        .ok()
        .filter(|count| (1..=i64::from(i32::MAX)).contains(count))
        .ok_or_else(|| ParseError::Invalid(error.into()))
}

fn hash_expiry_time(
    value: &BytesFrame,
    millis: bool,
    relative: bool,
    command: &str,
) -> Result<i64, ParseError> {
    // Redis reserves the top two bits of its 48-bit hash expiry timestamp.
    const MAX_HASH_EXPIRY_MS: i64 = (1 << 46) - 1;
    let time = redis_integer(value)?;
    if time < 0 {
        return Err(ParseError::Invalid(
            "invalid expire time, must be >= 0".into(),
        ));
    }
    let base = if relative {
        chrono::Utc::now().timestamp_millis()
    } else {
        0
    };
    let remaining_ms = MAX_HASH_EXPIRY_MS - base;
    let limit = if millis {
        remaining_ms
    } else {
        remaining_ms / 1000
    };
    if time > limit {
        return Err(invalid_expire_time(command));
    }
    Ok(time)
}

/// Checks a relative TTL for SET: must be positive and in range.
/// Expiry is stored in whole seconds, so millisecond TTLs round up and a key
/// never expires before the time asked for.
fn ttl_secs(ttl: u64, millis: bool, command: &str) -> Result<u64, ParseError> {
    let secs = if millis { ttl.div_ceil(1000) } else { ttl };
    if secs == 0 || secs > MAX_TTL_SECS {
        return Err(invalid_expire_time(command));
    }
    Ok(secs)
}

fn invalid_expire_time(command: &str) -> ParseError {
    ParseError::Invalid(format!("invalid expire time in '{}' command", command))
}

fn unknown_argument(arg: &str) -> ParseError {
    ParseError::Invalid(format!("unknown argument: {}", arg))
}

fn fields_given_twice() -> ParseError {
    ParseError::Invalid("FIELDS keyword specified multiple times".to_string())
}

/// Parses what follows the time in HEXPIRE/HEXPIREAT: an optional condition
/// and `FIELDS numfields field [field ...]`. Like Redis, the condition may
/// come before or after the fields.
fn parse_hexpire_args(
    elements: &[BytesFrame],
    command: &str,
) -> Result<(Option<HExpireCondition>, Vec<String>), ParseError> {
    let mut idx = 3;
    let mut condition = None;
    let mut fields = None;

    while idx < elements.len() {
        let arg = extract_string(&elements[idx])?;
        let parsed = match arg.to_uppercase().as_str() {
            "NX" => HExpireCondition::Nx,
            "XX" => HExpireCondition::Xx,
            "GT" => HExpireCondition::Gt,
            "LT" => HExpireCondition::Lt,
            "FIELDS" => {
                if fields.is_some() {
                    return Err(fields_given_twice());
                }
                idx += 1;

                // Parse numfields
                if idx >= elements.len() {
                    return Err(ParseError::Invalid(format!(
                        "{} requires field count after FIELDS",
                        command
                    )));
                }
                let num_fields = field_count(
                    &elements[idx],
                    "Parameter `numFields` should be greater than 0",
                )? as usize;
                idx += 1;

                // Validate against remaining args before allocating.
                if num_fields > elements.len() - idx {
                    return Err(ParseError::Invalid(
                        "Not enough fields provided".to_string(),
                    ));
                }

                // Parse field names
                let mut names = Vec::with_capacity(num_fields);
                for _ in 0..num_fields {
                    names.push(extract_string(&elements[idx])?);
                    idx += 1;
                }
                fields = Some(names);
                continue;
            }
            _ => return Err(unknown_argument(&arg)),
        };

        if condition.is_some_and(|previous| previous != parsed) {
            return Err(ParseError::Invalid(
                "Multiple condition flags specified".to_string(),
            ));
        }
        condition = Some(parsed);
        idx += 1;
    }

    let fields = fields
        .ok_or_else(|| ParseError::Invalid(format!("{} requires FIELDS keyword", command)))?;
    Ok((condition, fields))
}

/// Extract bytes from RESP value
fn extract_bytes(value: &BytesFrame) -> Result<Bytes, ParseError> {
    match value {
        BytesFrame::BulkString(data) => Ok(data.clone()),
        BytesFrame::SimpleString(data) => Ok(data.clone()),
        BytesFrame::Null => Err(ParseError::Invalid(
            "Cannot use null as byte argument".to_string(),
        )),
        _ => Err(ParseError::Invalid(
            "Expected string/bytes argument".to_string(),
        )),
    }
}

/// Serialize RESP value to bytes using redis-protocol crate
pub fn serialize_frame(frame: &BytesFrame) -> Bytes {
    // Error replies end at the first CRLF, and some echo client input (e.g. an
    // unknown command name). Replace CR/LF with spaces, as Redis does, so a
    // client can't split one reply into several.
    let sanitized;
    let frame = match frame {
        BytesFrame::Error(msg) if msg.contains(['\r', '\n']) => {
            sanitized = BytesFrame::Error(msg.replace(['\r', '\n'], " ").into());
            &sanitized
        }
        _ => frame,
    };
    let mut buf = bytes::BytesMut::new();
    extend_encode(&mut buf, frame, false).expect("Failed to encode frame");
    buf.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX: usize = 1024 * 1024;

    fn request(parts: &[&[u8]]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", parts.len()).into_bytes();
        for part in parts {
            out.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
            out.extend_from_slice(part);
            out.extend_from_slice(b"\r\n");
        }
        out
    }

    fn decode(input: &[u8], max: usize) -> (Result<Option<BytesFrame>, ParseError>, BytesMut) {
        let mut buf = BytesMut::from(input);
        let result = decode_request(&mut buf, max);
        (result, buf)
    }

    fn assert_protocol_error(input: &[u8], max: usize, expected: &str) {
        match decode(input, max).0 {
            Err(ParseError::Invalid(msg)) => {
                assert!(
                    msg.contains(expected),
                    "{msg:?} should contain {expected:?}"
                )
            }
            other => panic!("expected protocol error {expected:?}, got {other:?}"),
        }
    }

    #[test]
    fn decode_request_splits_one_request_off_the_buffer() {
        let mut input = request(&[b"SET", b"key", b"bin\r\n\x00value"]);
        input.extend_from_slice(&request(&[b"GET", b"key"]));

        let (result, rest) = decode(&input, MAX);
        let expected = command_frame(&["SET", "key", "bin\r\n\x00value"]);
        assert_eq!(result.unwrap(), Some(expected));
        assert_eq!(&rest[..], &request(&[b"GET", b"key"])[..]);
    }

    #[test]
    fn decode_request_waits_for_every_partial_prefix() {
        let input = request(&[b"SET", b"key", b"value"]);
        for cut in 0..input.len() {
            let (result, rest) = decode(&input[..cut], MAX);
            assert!(
                matches!(result, Ok(None)),
                "prefix of {cut} bytes: {result:?}"
            );
            assert_eq!(rest.len(), cut, "incomplete input must stay buffered");
        }
        assert!(decode(&input, MAX).0.unwrap().is_some());
    }

    #[test]
    fn decode_request_handles_empty_args_and_empty_array() {
        let (result, _) = decode(&request(&[b"PING", b""]), MAX);
        assert_eq!(result.unwrap(), Some(command_frame(&["PING", ""])));
        let (result, rest) = decode(b"*0\r\n", MAX);
        assert_eq!(result.unwrap(), Some(BytesFrame::Array(vec![])));
        assert!(rest.is_empty());
    }

    #[test]
    fn decode_request_rejects_deeply_nested_arrays_without_recursing() {
        // Used to overflow the stack and abort the whole process.
        let mut input = b"*1\r\n".repeat(100_000);
        input.extend_from_slice(b"$4\r\nPING\r\n");
        assert_protocol_error(&input, MAX, "expected '$'");
    }

    #[test]
    fn decode_request_rejects_bad_framing() {
        // Declared length is short, so the bytes after the value aren't CRLF.
        // The old decoder accepted this and ran the trailing bytes as a command.
        let mut short = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\nXYab".to_vec();
        short.extend_from_slice(&request(&[b"DEL", b"victim"]));
        assert_protocol_error(&short, MAX, "expected CRLF");

        assert_protocol_error(b"PING\r\n", MAX, "expected '*'");
        assert_protocol_error(b"*1\r\n+PING\r\n", MAX, "expected '$'");
        assert_protocol_error(b"*1\r\n$-1\r\n", MAX, "invalid bulk length");
        assert_protocol_error(b"*x\r\n", MAX, "invalid multibulk length");
        assert_protocol_error(b"*1\n$4\r\nPING\r\n", MAX, "invalid multibulk length");
        assert_protocol_error(&[b"*".as_slice(), &[b'1'; 40]].concat(), MAX, "too long");
        assert_protocol_error(b"*99999999999999999999\r\n", MAX, "out of range");
        assert_protocol_error(b"*2000000\r\n", MAX, "invalid multibulk length");
    }

    #[test]
    fn decode_request_enforces_max_size_before_buffering_the_value() {
        let input = request(&[b"SET", b"k", &[b'x'; 100]]);
        assert!(decode(&input, input.len()).0.unwrap().is_some());
        assert_protocol_error(&input, input.len() - 1, "max_request_size_mb");
        // Rejected from the header alone, without waiting for the payload.
        assert_protocol_error(
            b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$999999999\r\n",
            MAX,
            "max_request_size_mb",
        );
    }

    #[test]
    fn serialize_frame_strips_crlf_from_errors() {
        let frame = BytesFrame::Error("ERR unknown command 'FOO\r\n+OK'".into());
        assert_eq!(
            &serialize_frame(&frame)[..],
            b"-ERR unknown command 'FOO  +OK'\r\n"
        );
    }

    fn parse(parts: &[&str]) -> Result<RedisCommand, ParseError> {
        parse_command(command_frame(parts))
    }

    #[test]
    fn cluster_slot_commands_reject_slots_outside_the_keyspace() {
        for sub in ["ADDSLOTS", "DELSLOTS"] {
            assert!(parse(&["CLUSTER", sub, "0", "16383"]).is_ok());
            assert_parse_error(&["CLUSTER", sub, "16384"], "Invalid slot number");
        }
    }

    fn assert_parse_error(parts: &[&str], expected: &str) {
        match parse(parts) {
            Err(ParseError::Invalid(msg)) => {
                assert!(
                    msg.contains(expected),
                    "{msg:?} should contain {expected:?}"
                )
            }
            other => panic!("{parts:?}: expected error {expected:?}, got {other:?}"),
        }
    }

    #[test]
    fn non_utf8_keys_are_rejected_instead_of_colliding() {
        let frame = BytesFrame::Array(vec![
            BytesFrame::BulkString(Bytes::from_static(b"GET")),
            BytesFrame::BulkString(Bytes::from_static(b"\xff")),
        ]);
        match parse_command(frame) {
            Err(ParseError::Invalid(msg)) => assert!(msg.contains("UTF-8"), "{msg}"),
            other => panic!("expected UTF-8 error, got {other:?}"),
        }
        // Values stay binary.
        let frame = BytesFrame::Array(vec![
            BytesFrame::BulkString(Bytes::from_static(b"SET")),
            BytesFrame::BulkString(Bytes::from_static(b"k")),
            BytesFrame::BulkString(Bytes::from_static(b"\xff\x00")),
        ]);
        assert!(parse_command(frame).is_ok());
    }

    #[test]
    fn set_ttls_round_up_and_reject_zero_or_out_of_range() {
        let ttl = |parts: &[&str]| match parse(parts).unwrap() {
            RedisCommand::Set { ttl_seconds, .. } => ttl_seconds,
            other => panic!("unexpected {other:?}"),
        };
        assert_eq!(ttl(&["SET", "k", "v", "EX", "10"]), Some(10));
        assert_eq!(ttl(&["SET", "k", "v", "PX", "500"]), Some(1));
        assert_eq!(ttl(&["SET", "k", "v", "PX", "1500"]), Some(2));
        assert_eq!(ttl(&["SET", "k", "v", "PX", "2000"]), Some(2));

        let too_big = (MAX_TTL_SECS + 1).to_string();
        for (option, value) in [("EX", "0"), ("PX", "0"), ("EX", too_big.as_str())] {
            assert_parse_error(&["SET", "k", "v", option, value], "invalid expire time");
        }
        assert!(parse(&["HSETEX", "ns", "EX", "0", "FIELDS", "1", "f", "v"]).is_ok());
    }

    #[test]
    fn hash_expiries_respect_redis_deadline_limits() {
        let max_ms = ((1_i64 << 46) - 1).to_string();
        let over_ms = (1_i64 << 46).to_string();
        assert!(parse(&["HSETEX", "h", "PXAT", &max_ms, "FIELDS", "1", "f", "v"]).is_ok());
        for (option, value) in [("PXAT", &over_ms), ("PX", &max_ms)] {
            assert_parse_error(
                &["HSETEX", "h", option, value, "FIELDS", "1", "f", "v"],
                "invalid expire time",
            );
        }
        for cmd in ["HEXPIRE", "HEXPIREAT"] {
            for value in ["-1", "70368744178"] {
                assert_parse_error(
                    &[cmd, "h", value, "FIELDS", "1", "f"],
                    "invalid expire time",
                );
            }
        }
    }

    #[test]
    fn redis_integers_reject_noncanonical_expiries_and_counts() {
        for value in ["00", "-0", "+0", "01"] {
            assert_parse_error(&["EXPIRE", "k", value], "not an integer");
            assert_parse_error(&["SET", "k", "v", "EX", value], "not an integer");
            assert_parse_error(
                &["HSETEX", "h", "EXAT", value, "FIELDS", "1", "f", "v"],
                "not an integer",
            );
            assert_parse_error(
                &["HSETEX", "h", "FIELDS", value, "f", "v"],
                "invalid number of fields",
            );
        }
    }

    #[test]
    fn expire_and_hexpire_reject_out_of_range_ttls() {
        assert!(matches!(
            parse(&["EXPIRE", "k", "0"]),
            Ok(RedisCommand::Expire { seconds: 0, .. })
        ));
        // A negative TTL expires the key at once, like 0, as in Redis.
        assert!(matches!(
            parse(&["EXPIRE", "k", "-5"]),
            Ok(RedisCommand::Expire { seconds: 0, .. })
        ));
        let too_big = (MAX_TTL_SECS + 1).to_string();
        for seconds in [too_big.as_str(), "-9223372036854775808"] {
            assert_parse_error(&["EXPIRE", "k", seconds], "invalid expire time");
        }
        for seconds in [too_big.as_str(), "-9223372036854775808"] {
            assert_parse_error(
                &["HEXPIRE", "ns", seconds, "FIELDS", "1", "f"],
                "invalid expire time",
            );
        }
    }

    fn command_frame(parts: &[&str]) -> BytesFrame {
        BytesFrame::Array(
            parts
                .iter()
                .map(|part| BytesFrame::BulkString(part.as_bytes().to_vec().into()))
                .collect(),
        )
    }

    #[test]
    fn test_parse_get_command() {
        let input = b"*2\r\n$3\r\nGET\r\n$7\r\nmykey42\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(
            command,
            RedisCommand::Get {
                key: "mykey42".to_string()
            }
        );
    }

    #[test]
    fn test_parse_set_command() {
        let input = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$11\r\nhello world\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(
            command,
            RedisCommand::Set {
                key: "mykey".to_string(),
                value: Bytes::from_static(b"hello world"),
                ttl_seconds: None
            }
        );
    }

    #[test]
    fn test_parse_set_command_with_ex() {
        let input =
            b"*5\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$11\r\nhello world\r\n$2\r\nEX\r\n$2\r\n60\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(
            command,
            RedisCommand::Set {
                key: "mykey".to_string(),
                value: Bytes::from_static(b"hello world"),
                ttl_seconds: Some(60)
            }
        );
    }

    #[test]
    fn test_parse_set_command_with_px() {
        let input =
            b"*5\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$11\r\nhello world\r\n$2\r\nPX\r\n$5\r\n60000\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(
            command,
            RedisCommand::Set {
                key: "mykey".to_string(),
                value: Bytes::from_static(b"hello world"),
                ttl_seconds: Some(60)
            }
        );
    }

    #[test]
    fn test_parse_ttl_command() {
        let input = b"*2\r\n$3\r\nTTL\r\n$5\r\nmykey\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(
            command,
            RedisCommand::Ttl {
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn test_parse_expire_command() {
        let input = b"*3\r\n$6\r\nEXPIRE\r\n$5\r\nmykey\r\n$2\r\n60\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(
            command,
            RedisCommand::Expire {
                key: "mykey".to_string(),
                seconds: 60
            }
        );
    }

    #[test]
    fn test_parse_set_command_invalid_ttl_options() {
        // Test SET with invalid EX value
        let input = b"*5\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nvalue\r\n$2\r\nEX\r\n$3\r\nabc\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let result = parse_command(resp);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not an integer"));

        // Test SET with invalid PX value
        let input = b"*5\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$3\r\n-10\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let result = parse_command(resp);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid expire time")
        );
    }

    #[test]
    fn test_parse_set_command_missing_ttl_value() {
        // Test SET with EX but no value
        let input = b"*4\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nvalue\r\n$2\r\nEX\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let result = parse_command(resp);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("EX requires a value")
        );

        // Test SET with PX but no value
        let input = b"*4\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let result = parse_command(resp);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("PX requires a value")
        );
    }

    #[test]
    fn test_parse_set_command_unknown_option() {
        let input = b"*5\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nvalue\r\n$2\r\nXX\r\n$2\r\n60\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let result = parse_command(resp);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unknown SET option: XX")
        );
    }

    #[test]
    fn test_parse_expire_command_invalid_seconds() {
        let input = b"*3\r\n$6\r\nEXPIRE\r\n$5\r\nmykey\r\n$3\r\nabc\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let result = parse_command(resp);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not an integer"));
    }

    #[test]
    fn test_parse_ttl_command_wrong_args() {
        // Too many arguments
        let input = b"*3\r\n$3\r\nTTL\r\n$5\r\nmykey\r\n$5\r\nextra\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let result = parse_command(resp);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("TTL requires exactly 1 argument")
        );

        // Too few arguments
        let input = b"*1\r\n$3\r\nTTL\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let result = parse_command(resp);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("TTL requires exactly 1 argument")
        );
    }

    #[test]
    fn test_parse_expire_command_wrong_args() {
        // Too many arguments
        let input = b"*4\r\n$6\r\nEXPIRE\r\n$5\r\nmykey\r\n$2\r\n60\r\n$5\r\nextra\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let result = parse_command(resp);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("EXPIRE requires exactly 2 arguments")
        );

        // Too few arguments
        let input = b"*2\r\n$6\r\nEXPIRE\r\n$5\r\nmykey\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let result = parse_command(resp);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("EXPIRE requires exactly 2 arguments")
        );
    }

    #[test]
    fn test_parse_blobasaur_vacuum_command_shard_incremental_dryrun() {
        let command = parse_command(command_frame(&[
            "BLOBASAUR.VACUUM",
            "SHARD",
            "3",
            "MODE",
            "incremental",
            "BUDGET_MB",
            "128",
            "DRYRUN",
        ]))
        .unwrap();

        assert_eq!(
            command,
            RedisCommand::BlobasaurVacuum {
                target: VacuumShardTarget::Shard(3),
                mode: VacuumCommandMode::Incremental,
                budget_mb: 128,
                dry_run: true,
            }
        );
    }

    #[test]
    fn test_parse_blobasaur_watermark_command() {
        assert_eq!(
            parse_command(command_frame(&["blobasaur.watermark"])).unwrap(),
            RedisCommand::BlobasaurWatermark
        );
        assert!(parse_command(command_frame(&["BLOBASAUR.WATERMARK", "0"])).is_err());
    }

    #[test]
    fn test_parse_blobasaur_mfstart_command() {
        let cmd = parse_command(command_frame(&["blobasaur.mfstart", "r1"])).unwrap();
        assert_eq!(cmd.name(), "BLOBASAUR.MFSTART");
        assert_eq!(
            cmd,
            RedisCommand::BlobasaurMfStart {
                replica_id: "r1".to_string(),
            }
        );
        for bad in [
            &["BLOBASAUR.MFSTART"][..],
            &["BLOBASAUR.MFSTART", ""][..],
            &["BLOBASAUR.MFSTART", "r1", "extra"][..],
        ] {
            assert!(parse_command(command_frame(bad)).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn test_parse_blobasaur_sync_command() {
        let command = parse_command(command_frame(&[
            "blobasaur.sync",
            "2",
            "",
            "17",
            "4",
            "zstd",
        ]))
        .unwrap();
        assert_eq!(
            command,
            RedisCommand::BlobasaurSync {
                shard: 2,
                replid: String::new(),
                lsn: 17,
                num_shards: 4,
                compression: "zstd".to_string(),
            }
        );
        for bad in [
            &["BLOBASAUR.SYNC", "0", "id", "0", "1"][..],
            &["BLOBASAUR.SYNC", "x", "id", "0", "1", "none"][..],
            &["BLOBASAUR.SYNC", "0", "id", "-1", "1", "none"][..],
        ] {
            assert!(parse_command(command_frame(bad)).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn test_parse_blobasaur_failover_auth() {
        let cmd = parse_command(command_frame(&[
            "BLOBASAUR.FAILOVER-AUTH",
            "r1",
            "5",
            "p1",
            "2",
            "0-3,10",
        ]))
        .unwrap();
        assert_eq!(cmd.name(), "BLOBASAUR.FAILOVER-AUTH");
        match cmd {
            RedisCommand::BlobasaurFailoverAuth(req) => {
                assert_eq!(req.candidate, "r1");
                assert_eq!(req.epoch, 5);
                assert_eq!(req.failed_primary, "p1");
                assert_eq!(req.primary_config_epoch, 2);
                assert_eq!(req.slots, [0, 1, 2, 3, 10].into_iter().collect());
                assert!(!req.manual);
            }
            other => panic!("expected BlobasaurFailoverAuth, got {other:?}"),
        }

        let manual_cmd = parse_command(command_frame(&[
            "BLOBASAUR.FAILOVER-AUTH",
            "r1",
            "6",
            "p1",
            "2",
            "0-5",
            "manual",
        ]))
        .unwrap();
        match manual_cmd {
            RedisCommand::BlobasaurFailoverAuth(req) => assert!(req.manual),
            other => panic!("expected BlobasaurFailoverAuth, got {other:?}"),
        }

        for bad in [
            &["BLOBASAUR.FAILOVER-AUTH", "r1", "5", "p1", "2"][..],
            &["BLOBASAUR.FAILOVER-AUTH", "", "5", "p1", "2", "0-5"][..],
            &[
                "BLOBASAUR.FAILOVER-AUTH",
                "r1",
                "notanint",
                "p1",
                "2",
                "0-5",
            ][..],
            &["BLOBASAUR.FAILOVER-AUTH", "r1", "5", "", "2", "0-5"][..],
            &["BLOBASAUR.FAILOVER-AUTH", "r1", "5", "p1", "bad", "0-5"][..],
            &["BLOBASAUR.FAILOVER-AUTH", "r1", "5", "p1", "2", ""][..],
            &["BLOBASAUR.FAILOVER-AUTH", "r1", "5", "p1", "2", "10-5"][..],
            &["BLOBASAUR.FAILOVER-AUTH", "r1", "5", "p1", "2", "16384"][..],
            &[
                "BLOBASAUR.FAILOVER-AUTH",
                "r1",
                "5",
                "p1",
                "2",
                "0-5",
                "EXTRA",
            ][..],
            &[
                "BLOBASAUR.FAILOVER-AUTH",
                "r1",
                "5",
                "p1",
                "2",
                "0-5",
                "MANUAL",
                "X",
            ][..],
        ] {
            assert!(parse_command(command_frame(bad)).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn test_parse_blobasaur_vacuum_command_all_full() {
        let command = parse_command(command_frame(&[
            "blobasaur.vacuum",
            "shard",
            "all",
            "mode",
            "FULL",
            "budget_mb",
            "64",
        ]))
        .unwrap();

        assert_eq!(
            command,
            RedisCommand::BlobasaurVacuum {
                target: VacuumShardTarget::All,
                mode: VacuumCommandMode::Full,
                budget_mb: 64,
                dry_run: false,
            }
        );
    }

    #[test]
    fn test_parse_blobasaur_vacuum_command_invalid_forms() {
        let invalid_cases = vec![
            (
                command_frame(&["BLOBASAUR.VACUUM", "SHARD", "0", "MODE", "incremental"]),
                "BLOBASAUR.VACUUM syntax",
            ),
            (
                command_frame(&[
                    "BLOBASAUR.VACUUM",
                    "SHARDS",
                    "0",
                    "MODE",
                    "incremental",
                    "BUDGET_MB",
                    "64",
                ]),
                "requires SHARD keyword",
            ),
            (
                command_frame(&[
                    "BLOBASAUR.VACUUM",
                    "SHARD",
                    "-1",
                    "MODE",
                    "incremental",
                    "BUDGET_MB",
                    "64",
                ]),
                "Invalid shard id",
            ),
            (
                command_frame(&[
                    "BLOBASAUR.VACUUM",
                    "SHARD",
                    "0",
                    "MODE",
                    "fast",
                    "BUDGET_MB",
                    "64",
                ]),
                "Invalid MODE",
            ),
            (
                command_frame(&[
                    "BLOBASAUR.VACUUM",
                    "SHARD",
                    "0",
                    "MODE",
                    "incremental",
                    "BUDGET_MB",
                    "0",
                ]),
                "BUDGET_MB must be greater than 0",
            ),
            (
                command_frame(&[
                    "BLOBASAUR.VACUUM",
                    "SHARD",
                    "0",
                    "MODE",
                    "incremental",
                    "BUDGET_MB",
                    "abc",
                ]),
                "Invalid BUDGET_MB",
            ),
            (
                command_frame(&[
                    "BLOBASAUR.VACUUM",
                    "SHARD",
                    "0",
                    "MODE",
                    "incremental",
                    "BUDGET_MB",
                    "64",
                    "NOW",
                ]),
                "expected DRYRUN",
            ),
        ];

        for (frame, expected_msg) in invalid_cases {
            let err = parse_command(frame).unwrap_err();
            assert!(
                err.to_string().contains(expected_msg),
                "expected '{}' in error, got '{}'",
                expected_msg,
                err
            );
        }
    }

    #[test]
    fn test_parse_del_command() {
        let input = b"*2\r\n$3\r\nDEL\r\n$5\r\nmykey\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(
            command,
            RedisCommand::Del {
                keys: vec!["mykey".to_string()]
            }
        );
    }

    #[test]
    fn test_parse_exists_command() {
        let input = b"*2\r\n$6\r\nEXISTS\r\n$5\r\nmykey\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(
            command,
            RedisCommand::Exists {
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn test_parse_ping_command() {
        // PING without message
        let input = b"*1\r\n$4\r\nPING\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(command, RedisCommand::Ping { message: None });

        // PING with message
        let input = b"*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(
            command,
            RedisCommand::Ping {
                message: Some("hello".to_string())
            }
        );
    }

    #[test]
    fn test_parse_info_command() {
        // INFO without section
        let input = b"*1\r\n$4\r\nINFO\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(command, RedisCommand::Info { section: None });

        // INFO with section
        let input = b"*2\r\n$4\r\nINFO\r\n$6\r\nserver\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(
            command,
            RedisCommand::Info {
                section: Some("server".to_string())
            }
        );
    }

    #[test]
    fn test_parse_quit_command() {
        let input = b"*1\r\n$4\r\nQUIT\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(command, RedisCommand::Quit);
    }

    #[test]
    fn test_parse_readonly_readwrite() {
        assert_eq!(parse(&["READONLY"]).unwrap(), RedisCommand::ReadOnly);
        assert_eq!(parse(&["readwrite"]).unwrap(), RedisCommand::ReadWrite);
    }

    #[test]
    fn test_parse_command_command() {
        let input = b"*1\r\n$7\r\nCOMMAND\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(command, RedisCommand::Command { args: vec![] });

        assert_eq!(
            parse(&["COMMAND", "INFO", "get"]).unwrap(),
            RedisCommand::Command {
                args: vec!["INFO".to_string(), "get".to_string()]
            }
        );
    }

    #[test]
    fn test_parse_hget_command() {
        let input = b"*3\r\n$4\r\nHGET\r\n$9\r\nnamespace\r\n$5\r\nmykey\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(
            command,
            RedisCommand::HGet {
                namespace: "namespace".to_string(),
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn test_parse_hset_command() {
        let input = b"*4\r\n$4\r\nHSET\r\n$9\r\nnamespace\r\n$5\r\nmykey\r\n$11\r\nhello world\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(
            command,
            RedisCommand::HSet {
                namespace: "namespace".to_string(),
                key: "mykey".to_string(),
                value: Bytes::from_static(b"hello world")
            }
        );
    }

    #[test]
    fn test_parse_hdel_command() {
        let input = b"*3\r\n$4\r\nHDEL\r\n$9\r\nnamespace\r\n$5\r\nmykey\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(
            command,
            RedisCommand::HDel {
                namespace: "namespace".to_string(),
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn test_parse_hexists_command() {
        let input = b"*3\r\n$7\r\nHEXISTS\r\n$9\r\nnamespace\r\n$5\r\nmykey\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(
            command,
            RedisCommand::HExists {
                namespace: "namespace".to_string(),
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn test_parse_cluster_nodes_command() {
        let input = b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nNODES\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(command, RedisCommand::ClusterNodes);
    }

    #[test]
    fn test_parse_cluster_myid_and_shards_commands() {
        let myid = parse_command(command_frame(&["CLUSTER", "MYID"])).unwrap();
        assert_eq!(myid, RedisCommand::ClusterMyId);
        assert!(parse_command(command_frame(&["CLUSTER", "MYID", "extra"])).is_err());

        let shards = parse_command(command_frame(&["CLUSTER", "SHARDS"])).unwrap();
        assert_eq!(shards, RedisCommand::ClusterShards);
        assert!(parse_command(command_frame(&["CLUSTER", "SHARDS", "extra"])).is_err());
    }

    #[test]
    fn test_parse_cluster_info_command() {
        let input = b"*2\r\n$7\r\nCLUSTER\r\n$4\r\nINFO\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(command, RedisCommand::ClusterInfo);
    }

    #[test]
    fn test_parse_cluster_addslots_command() {
        let input = b"*4\r\n$7\r\nCLUSTER\r\n$8\r\nADDSLOTS\r\n$1\r\n0\r\n$1\r\n1\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(command, RedisCommand::ClusterAddSlots { slots: vec![0, 1] });
    }

    #[test]
    fn test_parse_cluster_keyslot_command() {
        let input = b"*3\r\n$7\r\nCLUSTER\r\n$7\r\nKEYSLOT\r\n$5\r\nmykey\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(
            command,
            RedisCommand::ClusterKeySlot {
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn test_parse_unknown_command() {
        let input = b"*2\r\n$7\r\nUNKNOWN\r\n$3\r\narg\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(command, RedisCommand::Unknown("UNKNOWN".to_string()));
    }

    #[test]
    fn test_parse_hexpire_command_basic() {
        // HEXPIRE myhash 10 FIELDS 2 field1 field2
        let command = parse_command(command_frame(&[
            "HEXPIRE", "myhash", "10", "FIELDS", "2", "field1", "field2",
        ]))
        .unwrap();
        assert_eq!(
            command,
            RedisCommand::HExpire {
                key: "myhash".to_string(),
                seconds: 10,
                condition: None,
                fields: vec!["field1".to_string(), "field2".to_string()],
            }
        );
    }

    #[test]
    fn test_parse_hexpire_command_with_nx() {
        let command = parse_command(command_frame(&[
            "HEXPIRE", "myhash", "60", "NX", "FIELDS", "1", "field1",
        ]))
        .unwrap();
        assert_eq!(
            command,
            RedisCommand::HExpire {
                key: "myhash".to_string(),
                seconds: 60,
                condition: Some(HExpireCondition::Nx),
                fields: vec!["field1".to_string()],
            }
        );
    }

    #[test]
    fn test_parse_hexpire_command_with_xx() {
        let command = parse_command(command_frame(&[
            "HEXPIRE", "myhash", "60", "XX", "FIELDS", "1", "field1",
        ]))
        .unwrap();
        assert_eq!(
            command,
            RedisCommand::HExpire {
                key: "myhash".to_string(),
                seconds: 60,
                condition: Some(HExpireCondition::Xx),
                fields: vec!["field1".to_string()],
            }
        );
    }

    #[test]
    fn test_parse_hexpire_command_with_gt() {
        let command = parse_command(command_frame(&[
            "HEXPIRE", "myhash", "60", "GT", "FIELDS", "1", "field1",
        ]))
        .unwrap();
        assert_eq!(
            command,
            RedisCommand::HExpire {
                key: "myhash".to_string(),
                seconds: 60,
                condition: Some(HExpireCondition::Gt),
                fields: vec!["field1".to_string()],
            }
        );
    }

    #[test]
    fn test_parse_hexpire_command_with_lt() {
        let command = parse_command(command_frame(&[
            "HEXPIRE", "myhash", "60", "LT", "FIELDS", "1", "field1",
        ]))
        .unwrap();
        assert_eq!(
            command,
            RedisCommand::HExpire {
                key: "myhash".to_string(),
                seconds: 60,
                condition: Some(HExpireCondition::Lt),
                fields: vec!["field1".to_string()],
            }
        );
    }

    #[test]
    fn test_parse_hexpire_command_too_few_args() {
        // Missing fields
        let result = parse_command(command_frame(&["HEXPIRE", "myhash", "10"]));
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_hexpire_command_invalid_seconds() {
        let result = parse_command(command_frame(&[
            "HEXPIRE", "myhash", "abc", "FIELDS", "1", "field1",
        ]));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not an integer"));
    }

    #[test]
    fn test_parse_hexpire_command_zero_fields() {
        // With a condition token to bring element count to 6 (the minimum)
        let result = parse_command(command_frame(&[
            "HEXPIRE", "myhash", "10", "NX", "FIELDS", "0",
        ]));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("greater than 0"));
    }

    #[test]
    fn test_parse_hexpireat_command_basic() {
        let command = parse_command(command_frame(&[
            "HEXPIREAT",
            "myhash",
            "1715704971",
            "FIELDS",
            "2",
            "field1",
            "field2",
        ]))
        .unwrap();
        assert_eq!(
            command,
            RedisCommand::HExpireAt {
                key: "myhash".to_string(),
                unix_time_seconds: 1715704971,
                condition: None,
                fields: vec!["field1".to_string(), "field2".to_string()],
            }
        );
    }

    #[test]
    fn test_parse_hexpireat_command_with_gt() {
        let command = parse_command(command_frame(&[
            "HEXPIREAT",
            "myhash",
            "1715704971",
            "GT",
            "FIELDS",
            "1",
            "field1",
        ]))
        .unwrap();
        assert_eq!(
            command,
            RedisCommand::HExpireAt {
                key: "myhash".to_string(),
                unix_time_seconds: 1715704971,
                condition: Some(HExpireCondition::Gt),
                fields: vec!["field1".to_string()],
            }
        );
    }

    #[test]
    fn test_parse_hexpireat_command_too_few_args() {
        let result = parse_command(command_frame(&["HEXPIREAT", "myhash", "123"]));
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_hexpireat_command_invalid_timestamp() {
        let result = parse_command(command_frame(&[
            "HEXPIREAT",
            "myhash",
            "notanumber",
            "FIELDS",
            "1",
            "field1",
        ]));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not an integer"));
    }

    #[test]
    fn hsetex_rejects_conflicting_options() {
        for flags in [["FNX", "FXX"], ["FXX", "FNX"], ["FNX", "FNX"]] {
            let mut parts = vec!["HSETEX", "ns"];
            parts.extend(flags);
            parts.extend(["FIELDS", "1", "f", "v"]);
            assert_parse_error(&parts, "Only one of FXX or FNX");
        }

        for expiry in [
            &["EX", "10", "PX", "100"][..],
            &["EX", "10", "KEEPTTL"],
            &["KEEPTTL", "EXAT", "4102444800"],
            &["PXAT", "4102444800000", "EX", "10"],
        ] {
            let mut parts = vec!["HSETEX", "ns", "FXX"];
            parts.extend(expiry);
            parts.extend(["FIELDS", "1", "f", "v"]);
            assert_parse_error(&parts, "Only one of EX, PX, EXAT, PXAT or KEEPTTL");
        }

        assert!(matches!(
            parse(&["HSETEX", "ns", "FXX", "EX", "10", "FIELDS", "1", "f", "v"]),
            Ok(RedisCommand::HSetEx {
                fxx: true,
                fnx: false,
                expire_option: Some(ExpireOption::Ex(10)),
                ..
            })
        ));
    }

    #[test]
    fn hash_field_commands_reject_args_past_numfields() {
        assert_parse_error(
            &["HSETEX", "ns", "FIELDS", "1", "a", "b", "c"],
            "unknown argument: c",
        );
        assert_parse_error(
            &["HSETEX", "ns", "FIELDS", "1", "a", "b", "c", "d"],
            "unknown argument: c",
        );
        assert_parse_error(
            &["HEXPIRE", "ns", "10", "FIELDS", "1", "f", "extra"],
            "unknown argument: extra",
        );
        assert_parse_error(
            &[
                "HEXPIREAT",
                "ns",
                "4102444800",
                "NX",
                "FIELDS",
                "1",
                "f",
                "g",
            ],
            "unknown argument: g",
        );

        // Exactly numfields still parses.
        assert!(parse(&["HSETEX", "ns", "FIELDS", "2", "a", "1", "b", "2"]).is_ok());
        assert!(parse(&["HEXPIRE", "ns", "10", "FIELDS", "2", "f", "g"]).is_ok());
        assert!(parse(&["HEXPIREAT", "ns", "4102444800", "FIELDS", "1", "f"]).is_ok());

        assert_parse_error(
            &[
                "HSETEX", "ns", "FIELDS", "1", "a", "b", "FIELDS", "1", "c", "d",
            ],
            "FIELDS keyword specified multiple times",
        );
        assert_parse_error(
            &[
                "HEXPIRE", "ns", "10", "FIELDS", "1", "f", "FIELDS", "1", "g",
            ],
            "FIELDS keyword specified multiple times",
        );
        assert_parse_error(&["HSETEX", "ns", "FNX", "EX", "10"], "requires FIELDS");
    }

    #[test]
    fn hash_field_commands_take_options_after_fields() {
        // Redis accepts options on either side of the fields.
        assert!(matches!(
            parse(&["HSETEX", "ns", "FIELDS", "1", "f", "v", "EX", "60"]),
            Ok(RedisCommand::HSetEx {
                fnx: false,
                fxx: false,
                expire_option: Some(ExpireOption::Ex(60)),
                ..
            })
        ));
        assert!(matches!(
            parse(&["HSETEX", "ns", "PX", "5000", "FIELDS", "1", "f", "v", "FNX"]),
            Ok(RedisCommand::HSetEx {
                fnx: true,
                fxx: false,
                expire_option: Some(ExpireOption::Px(5000)),
                ..
            })
        ));
        assert!(matches!(
            parse(&["HEXPIRE", "ns", "10", "FIELDS", "1", "f", "NX"]),
            Ok(RedisCommand::HExpire {
                condition: Some(HExpireCondition::Nx),
                ..
            })
        ));
        assert!(matches!(
            parse(&["HEXPIREAT", "ns", "4102444800", "FIELDS", "1", "f", "XX"]),
            Ok(RedisCommand::HExpireAt {
                condition: Some(HExpireCondition::Xx),
                ..
            })
        ));

        // Fields named like options are still fields.
        match parse(&["HEXPIRE", "ns", "10", "FIELDS", "2", "NX", "FIELDS"]) {
            Ok(RedisCommand::HExpire {
                condition: None,
                fields,
                ..
            }) => assert_eq!(fields, ["NX", "FIELDS"]),
            other => panic!("unexpected parse: {other:?}"),
        }

        // Conflicts are caught across both sides of the fields.
        assert_parse_error(
            &[
                "HSETEX", "ns", "EX", "10", "FIELDS", "1", "a", "b", "PX", "100",
            ],
            "Only one of EX, PX, EXAT, PXAT or KEEPTTL",
        );
        assert_parse_error(
            &["HSETEX", "ns", "FNX", "FIELDS", "1", "a", "b", "FXX"],
            "Only one of FXX or FNX",
        );
        assert_parse_error(
            &["HEXPIRE", "ns", "10", "NX", "FIELDS", "1", "f", "XX"],
            "Multiple condition flags specified",
        );
        assert_parse_error(
            &["HEXPIRE", "ns", "10", "FIELDS", "1", "f", "NX", "extra"],
            "unknown argument: extra",
        );
    }

    #[test]
    fn hash_field_commands_reject_oversized_field_counts_without_allocating() {
        for huge in ["1000000000000000", "2147483648"] {
            assert_parse_error(
                &["HEXPIRE", "k", "10", "FIELDS", huge, "f"],
                "greater than 0",
            );
            assert_parse_error(
                &["HEXPIREAT", "k", "4102444800", "FIELDS", huge, "f"],
                "greater than 0",
            );
            assert_parse_error(
                &["HSETEX", "k", "FIELDS", huge, "f", "v"],
                "invalid number of fields",
            );
        }

        // Within i32::MAX, but larger than the remaining arguments.
        let max_i32 = i32::MAX.to_string();
        assert_parse_error(
            &["HEXPIRE", "k", "10", "FIELDS", &max_i32, "f"],
            "Not enough fields provided",
        );
        assert_parse_error(
            &["HEXPIREAT", "k", "4102444800", "FIELDS", &max_i32, "f"],
            "Not enough fields provided",
        );
        assert_parse_error(
            &["HSETEX", "k", "FIELDS", &max_i32, "f", "v"],
            "Not enough field-value pairs",
        );
    }

    #[test]
    fn test_parse_case_insensitive_commands() {
        let input = b"*2\r\n$3\r\nget\r\n$5\r\nmykey\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(
            command,
            RedisCommand::Get {
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn test_serialize_simple_string() {
        let value = BytesFrame::SimpleString("OK".into());
        let serialized = serialize_frame(&value);
        assert_eq!(serialized.as_ref(), b"+OK\r\n");
    }

    #[test]
    fn test_serialize_error() {
        let value = BytesFrame::Error("ERR something".into());
        let serialized = serialize_frame(&value);
        assert_eq!(serialized.as_ref(), b"-ERR something\r\n");
    }

    #[test]
    fn test_serialize_integer() {
        let value = BytesFrame::Integer(42);
        let serialized = serialize_frame(&value);
        assert_eq!(serialized.as_ref(), b":42\r\n");
    }

    #[test]
    fn test_serialize_bulk_string() {
        let value = BytesFrame::BulkString("hello".into());
        let serialized = serialize_frame(&value);
        assert_eq!(serialized.as_ref(), b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_serialize_null() {
        let value = BytesFrame::Null;
        let serialized = serialize_frame(&value);
        assert_eq!(serialized.as_ref(), b"$-1\r\n");
    }

    #[test]
    fn test_serialize_array() {
        let value = BytesFrame::Array(vec![
            BytesFrame::BulkString("GET".into()),
            BytesFrame::BulkString("key".into()),
        ]);
        let serialized = serialize_frame(&value);
        assert_eq!(serialized.as_ref(), b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
    }

    #[test]
    fn test_parse_cluster_failover_and_replicate() {
        assert_eq!(
            parse(&["CLUSTER", "FAILOVER"]).unwrap(),
            RedisCommand::ClusterFailover {
                mode: ClusterFailoverMode::Default
            }
        );
        assert_eq!(
            parse(&["cluster", "failover", "force"]).unwrap(),
            RedisCommand::ClusterFailover {
                mode: ClusterFailoverMode::Force
            }
        );
        assert_eq!(
            parse(&["CLUSTER", "FAILOVER", "TAKEOVER"]).unwrap(),
            RedisCommand::ClusterFailover {
                mode: ClusterFailoverMode::Takeover
            }
        );
        assert_parse_error(&["CLUSTER", "FAILOVER", "INVALID"], "FORCE or TAKEOVER");
        assert_parse_error(
            &["CLUSTER", "FAILOVER", "FORCE", "EXTRA"],
            "wrong number of arguments",
        );

        assert_eq!(
            parse(&["CLUSTER", "REPLICATE", "node-1"]).unwrap(),
            RedisCommand::ClusterReplicate {
                node_id: "node-1".to_string()
            }
        );
        assert_parse_error(&["CLUSTER", "REPLICATE"], "requires exactly 1 argument");
        assert_parse_error(
            &["CLUSTER", "REPLICATE", "a", "b"],
            "requires exactly 1 argument",
        );
        assert_parse_error(&["CLUSTER", "REPLICATE", ""], "cannot be empty");
    }
}

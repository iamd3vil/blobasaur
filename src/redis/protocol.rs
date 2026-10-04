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
    Command,
    // Cluster commands
    ClusterNodes,
    ClusterInfo,
    ClusterSlots,
    ClusterAddSlots {
        slots: Vec<u16>,
    },
    ClusterDelSlots {
        slots: Vec<u16>,
    },
    ClusterKeySlot {
        key: String,
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
    Hello {
        protover: Option<u64>,
    },
    Client {
        subcommand: String,
    },
    Quit,
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
            RedisCommand::Command => "COMMAND".to_string(),
            RedisCommand::ClusterNodes => "CLUSTER NODES".to_string(),
            RedisCommand::ClusterInfo => "CLUSTER INFO".to_string(),
            RedisCommand::ClusterSlots => "CLUSTER SLOTS".to_string(),
            RedisCommand::ClusterAddSlots { .. } => "CLUSTER ADDSLOTS".to_string(),
            RedisCommand::ClusterDelSlots { .. } => "CLUSTER DELSLOTS".to_string(),
            RedisCommand::ClusterKeySlot { .. } => "CLUSTER KEYSLOT".to_string(),
            RedisCommand::Ttl { .. } => "TTL".to_string(),
            RedisCommand::Expire { .. } => "EXPIRE".to_string(),
            RedisCommand::BlobasaurVacuum { .. } => "BLOBASAUR.VACUUM".to_string(),
            RedisCommand::Hello { .. } => "HELLO".to_string(),
            RedisCommand::Client { .. } => "CLIENT".to_string(),
            RedisCommand::Quit => "QUIT".to_string(),
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
                    "EX" => {
                        if i + 1 >= elements.len() {
                            return Err(ParseError::Invalid("EX requires a value".to_string()));
                        }
                        let seconds_str = extract_string(&elements[i + 1])?;
                        let seconds = seconds_str.parse::<u64>().map_err(|_| {
                            ParseError::Invalid(format!("Invalid EX value: {}", seconds_str))
                        })?;
                        ttl_seconds = Some(ttl_secs(seconds, false, "set")?);
                        i += 2;
                    }
                    "PX" => {
                        if i + 1 >= elements.len() {
                            return Err(ParseError::Invalid("PX requires a value".to_string()));
                        }
                        let millis_str = extract_string(&elements[i + 1])?;
                        let millis = millis_str.parse::<u64>().map_err(|_| {
                            ParseError::Invalid(format!("Invalid PX value: {}", millis_str))
                        })?;
                        ttl_seconds = Some(ttl_secs(millis, true, "set")?);
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
        "COMMAND" => Ok(RedisCommand::Command),
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

            // Parse options
            while idx < elements.len() {
                let opt = extract_string(&elements[idx])?.to_uppercase();
                match opt.as_str() {
                    "FNX" => {
                        fnx = true;
                        idx += 1;
                    }
                    "FXX" => {
                        fxx = true;
                        idx += 1;
                    }
                    "EX" => {
                        if idx + 1 >= elements.len() {
                            return Err(ParseError::Invalid("EX requires a value".to_string()));
                        }
                        let seconds = extract_string(&elements[idx + 1])?
                            .parse::<u64>()
                            .map_err(|_| ParseError::Invalid("Invalid EX value".to_string()))?;
                        ttl_secs(seconds, false, "hsetex")?;
                        expire_option = Some(ExpireOption::Ex(seconds));
                        idx += 2;
                    }
                    "PX" => {
                        if idx + 1 >= elements.len() {
                            return Err(ParseError::Invalid("PX requires a value".to_string()));
                        }
                        let millis = extract_string(&elements[idx + 1])?
                            .parse::<u64>()
                            .map_err(|_| ParseError::Invalid("Invalid PX value".to_string()))?;
                        ttl_secs(millis, true, "hsetex")?;
                        expire_option = Some(ExpireOption::Px(millis));
                        idx += 2;
                    }
                    "EXAT" => {
                        if idx + 1 >= elements.len() {
                            return Err(ParseError::Invalid("EXAT requires a value".to_string()));
                        }
                        let timestamp = extract_string(&elements[idx + 1])?
                            .parse::<i64>()
                            .map_err(|_| ParseError::Invalid("Invalid EXAT value".to_string()))?;
                        expire_option = Some(ExpireOption::ExAt(timestamp));
                        idx += 2;
                    }
                    "PXAT" => {
                        if idx + 1 >= elements.len() {
                            return Err(ParseError::Invalid("PXAT requires a value".to_string()));
                        }
                        let timestamp = extract_string(&elements[idx + 1])?
                            .parse::<i64>()
                            .map_err(|_| ParseError::Invalid("Invalid PXAT value".to_string()))?;
                        expire_option = Some(ExpireOption::PxAt(timestamp));
                        idx += 2;
                    }
                    "KEEPTTL" => {
                        expire_option = Some(ExpireOption::KeepTtl);
                        idx += 1;
                    }
                    "FIELDS" => {
                        break; // Found FIELDS keyword, exit options parsing
                    }
                    _ => {
                        return Err(ParseError::Invalid(format!("Unknown option: {}", opt)));
                    }
                }
            }

            // Check for FIELDS keyword
            if idx >= elements.len() || extract_string(&elements[idx])?.to_uppercase() != "FIELDS" {
                return Err(ParseError::Invalid(
                    "HSETEX requires FIELDS keyword".to_string(),
                ));
            }
            idx += 1;

            // Get number of fields
            if idx >= elements.len() {
                return Err(ParseError::Invalid(
                    "HSETEX requires field count after FIELDS".to_string(),
                ));
            }
            let num_fields = extract_string(&elements[idx])?
                .parse::<usize>()
                .map_err(|_| ParseError::Invalid("Invalid field count".to_string()))?;
            idx += 1;

            // Parse field-value pairs
            let mut fields = Vec::new();
            for _ in 0..num_fields {
                if idx + 1 >= elements.len() {
                    return Err(ParseError::Invalid(
                        "Not enough field-value pairs".to_string(),
                    ));
                }
                let field = extract_string(&elements[idx])?;
                let value = extract_bytes(&elements[idx + 1])?;
                fields.push((field, value));
                idx += 2;
            }

            if fields.is_empty() {
                return Err(ParseError::Invalid(
                    "HSETEX requires at least one field-value pair".to_string(),
                ));
            }

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
            let seconds_str = extract_string(&elements[2])?;
            let seconds = seconds_str.parse::<i64>().map_err(|_| {
                ParseError::Invalid(format!("Invalid seconds value: {}", seconds_str))
            })?;
            if seconds.unsigned_abs() > MAX_TTL_SECS {
                return Err(invalid_expire_time("hexpire"));
            }

            let mut idx = 3;
            let mut condition = None;

            // Parse optional condition (NX | XX | GT | LT)
            if idx < elements.len() {
                let token = extract_string(&elements[idx])?.to_uppercase();
                match token.as_str() {
                    "NX" => {
                        condition = Some(HExpireCondition::Nx);
                        idx += 1;
                    }
                    "XX" => {
                        condition = Some(HExpireCondition::Xx);
                        idx += 1;
                    }
                    "GT" => {
                        condition = Some(HExpireCondition::Gt);
                        idx += 1;
                    }
                    "LT" => {
                        condition = Some(HExpireCondition::Lt);
                        idx += 1;
                    }
                    "FIELDS" => {} // Not a condition, will be parsed below
                    _ => {
                        return Err(ParseError::Invalid(format!(
                            "Unknown HEXPIRE option: {}",
                            token
                        )));
                    }
                }
            }

            // Parse FIELDS keyword
            if idx >= elements.len() {
                return Err(ParseError::Invalid(
                    "HEXPIRE requires FIELDS keyword".to_string(),
                ));
            }
            let fields_keyword = extract_string(&elements[idx])?.to_uppercase();
            if fields_keyword != "FIELDS" {
                return Err(ParseError::Invalid(
                    "HEXPIRE requires FIELDS keyword".to_string(),
                ));
            }
            idx += 1;

            // Parse numfields
            if idx >= elements.len() {
                return Err(ParseError::Invalid(
                    "HEXPIRE requires field count after FIELDS".to_string(),
                ));
            }
            let num_fields = extract_string(&elements[idx])?
                .parse::<usize>()
                .map_err(|_| ParseError::Invalid("Invalid field count".to_string()))?;
            idx += 1;

            if num_fields == 0 {
                return Err(ParseError::Invalid(
                    "HEXPIRE requires at least one field".to_string(),
                ));
            }

            // Parse field names
            let mut fields = Vec::with_capacity(num_fields);
            for _ in 0..num_fields {
                if idx >= elements.len() {
                    return Err(ParseError::Invalid(
                        "Not enough fields provided".to_string(),
                    ));
                }
                fields.push(extract_string(&elements[idx])?);
                idx += 1;
            }

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
            let timestamp_str = extract_string(&elements[2])?;
            let unix_time_seconds = timestamp_str.parse::<i64>().map_err(|_| {
                ParseError::Invalid(format!("Invalid timestamp value: {}", timestamp_str))
            })?;

            let mut idx = 3;
            let mut condition = None;

            // Parse optional condition (NX | XX | GT | LT)
            if idx < elements.len() {
                let token = extract_string(&elements[idx])?.to_uppercase();
                match token.as_str() {
                    "NX" => {
                        condition = Some(HExpireCondition::Nx);
                        idx += 1;
                    }
                    "XX" => {
                        condition = Some(HExpireCondition::Xx);
                        idx += 1;
                    }
                    "GT" => {
                        condition = Some(HExpireCondition::Gt);
                        idx += 1;
                    }
                    "LT" => {
                        condition = Some(HExpireCondition::Lt);
                        idx += 1;
                    }
                    "FIELDS" => {} // Not a condition, will be parsed below
                    _ => {
                        return Err(ParseError::Invalid(format!(
                            "Unknown HEXPIREAT option: {}",
                            token
                        )));
                    }
                }
            }

            // Parse FIELDS keyword
            if idx >= elements.len() {
                return Err(ParseError::Invalid(
                    "HEXPIREAT requires FIELDS keyword".to_string(),
                ));
            }
            let fields_keyword = extract_string(&elements[idx])?.to_uppercase();
            if fields_keyword != "FIELDS" {
                return Err(ParseError::Invalid(
                    "HEXPIREAT requires FIELDS keyword".to_string(),
                ));
            }
            idx += 1;

            // Parse numfields
            if idx >= elements.len() {
                return Err(ParseError::Invalid(
                    "HEXPIREAT requires field count after FIELDS".to_string(),
                ));
            }
            let num_fields = extract_string(&elements[idx])?
                .parse::<usize>()
                .map_err(|_| ParseError::Invalid("Invalid field count".to_string()))?;
            idx += 1;

            if num_fields == 0 {
                return Err(ParseError::Invalid(
                    "HEXPIREAT requires at least one field".to_string(),
                ));
            }

            // Parse field names
            let mut fields = Vec::with_capacity(num_fields);
            for _ in 0..num_fields {
                if idx >= elements.len() {
                    return Err(ParseError::Invalid(
                        "Not enough fields provided".to_string(),
                    ));
                }
                fields.push(extract_string(&elements[idx])?);
                idx += 1;
            }

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
                "NODES" => Ok(RedisCommand::ClusterNodes),
                "INFO" => Ok(RedisCommand::ClusterInfo),
                "SLOTS" => Ok(RedisCommand::ClusterSlots),
                "ADDSLOTS" => {
                    if elements.len() < 3 {
                        return Err(ParseError::Invalid(
                            "CLUSTER ADDSLOTS requires at least 1 slot".to_string(),
                        ));
                    }
                    let mut slots = Vec::new();
                    for slot_arg in &elements[2..] {
                        let slot_str = extract_string(slot_arg)?;
                        let slot = slot_str.parse::<u16>().map_err(|_| {
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
                        let slot = slot_str.parse::<u16>().map_err(|_| {
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
            let seconds_str = extract_string(&elements[2])?;
            let seconds = seconds_str.parse::<u64>().map_err(|_| {
                ParseError::Invalid(format!("Invalid seconds value: {}", seconds_str))
            })?;
            // 0 is allowed: like Redis, it expires the key immediately.
            if seconds > MAX_TTL_SECS {
                return Err(invalid_expire_time("expire"));
            }
            Ok(RedisCommand::Expire { key, seconds })
        }
        "BLOBASAUR.VACUUM" => parse_blobasaur_vacuum_command(&elements),
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
            Ok(RedisCommand::Client { subcommand })
        }
        "QUIT" => Ok(RedisCommand::Quit),
        _ => Ok(RedisCommand::Unknown(command_name)),
    }
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

/// Checks a relative TTL for SET/HSETEX: must be positive and in range.
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

    fn parse(parts: &[&str]) -> Result<RedisCommand, ParseError> {
        parse_command(command_frame(parts))
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
        assert_parse_error(
            &["HSETEX", "ns", "EX", "0", "FIELDS", "1", "f", "v"],
            "invalid expire time",
        );
    }

    #[test]
    fn expire_and_hexpire_reject_out_of_range_ttls() {
        assert!(matches!(
            parse(&["EXPIRE", "k", "0"]),
            Ok(RedisCommand::Expire { seconds: 0, .. })
        ));
        let too_big = (MAX_TTL_SECS + 1).to_string();
        assert_parse_error(&["EXPIRE", "k", &too_big], "invalid expire time");
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
        assert!(result.unwrap_err().to_string().contains("Invalid EX value"));

        // Test SET with invalid PX value
        let input = b"*5\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$3\r\n-10\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let result = parse_command(resp);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid PX value"));
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
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid seconds value")
        );
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
    fn test_parse_command_command() {
        let input = b"*1\r\n$7\r\nCOMMAND\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(command, RedisCommand::Command);
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
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid seconds value")
        );
    }

    #[test]
    fn test_parse_hexpire_command_zero_fields() {
        // With a condition token to bring element count to 6 (the minimum)
        let result = parse_command(command_frame(&[
            "HEXPIRE", "myhash", "10", "NX", "FIELDS", "0",
        ]));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("at least one field")
        );
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
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid timestamp value")
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
}

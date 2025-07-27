use bytes::Bytes;
use redis_protocol::resp2::{decode::decode_bytes_mut, encode::extend_encode, types::BytesFrame};

/// Redis protocol data types (re-export from redis-protocol crate)
pub type RespValue = BytesFrame;

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
        key: String,
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
    HDel {
        namespace: String,
        key: String,
    },
    HExists {
        namespace: String,
        key: String,
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
            RedisCommand::HDel { .. } => "HDEL".to_string(),
            RedisCommand::HExists { .. } => "HEXISTS".to_string(),
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

/// Parse a single RESP message and return both the parsed value and remaining bytes
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
                        ttl_seconds = Some(seconds);
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
                        ttl_seconds = Some(millis / 1000); // Convert to seconds
                        i += 2;
                    }
                    _ => {
                        return Err(ParseError::Invalid(format!("Unknown SET option: {}", option)));
                    }
                }
            }
            
            Ok(RedisCommand::Set { key, value, ttl_seconds })
        }
        "DEL" => {
            if elements.len() != 2 {
                return Err(ParseError::Invalid(
                    "DEL requires exactly 1 argument".to_string(),
                ));
            }
            let key = extract_string(&elements[1])?;
            Ok(RedisCommand::Del { key })
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
            Ok(RedisCommand::Expire { key, seconds })
        }
        "QUIT" => Ok(RedisCommand::Quit),
        _ => Ok(RedisCommand::Unknown(command_name)),
    }
}

/// Extract string from RESP value
fn extract_string(value: &BytesFrame) -> Result<String, ParseError> {
    match value {
        BytesFrame::BulkString(data) => Ok(String::from_utf8_lossy(data).to_string()),
        BytesFrame::SimpleString(data) => Ok(String::from_utf8_lossy(data).to_string()),
        BytesFrame::Null => Err(ParseError::Invalid(
            "Cannot use null as string argument".to_string(),
        )),
        _ => Err(ParseError::Invalid("Expected string argument".to_string())),
    }
}

/// Extract bytes from RESP value
fn extract_bytes(value: &BytesFrame) -> Result<Bytes, ParseError> {
    match value {
        BytesFrame::BulkString(data) => Ok(Bytes::copy_from_slice(data)),
        BytesFrame::SimpleString(data) => Ok(Bytes::copy_from_slice(data)),
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
        let input = b"*5\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$11\r\nhello world\r\n$2\r\nEX\r\n$2\r\n60\r\n";
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
        let input = b"*5\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$11\r\nhello world\r\n$2\r\nPX\r\n$5\r\n60000\r\n";
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
        assert!(result.unwrap_err().to_string().contains("EX requires a value"));

        // Test SET with PX but no value
        let input = b"*4\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let result = parse_command(resp);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("PX requires a value"));
    }

    #[test]
    fn test_parse_set_command_unknown_option() {
        let input = b"*5\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nvalue\r\n$2\r\nXX\r\n$2\r\n60\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let result = parse_command(resp);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unknown SET option: XX"));
    }

    #[test]
    fn test_parse_expire_command_invalid_seconds() {
        let input = b"*3\r\n$6\r\nEXPIRE\r\n$5\r\nmykey\r\n$3\r\nabc\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let result = parse_command(resp);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid seconds value"));
    }

    #[test]
    fn test_parse_ttl_command_wrong_args() {
        // Too many arguments
        let input = b"*3\r\n$3\r\nTTL\r\n$5\r\nmykey\r\n$5\r\nextra\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let result = parse_command(resp);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("TTL requires exactly 1 argument"));

        // Too few arguments
        let input = b"*1\r\n$3\r\nTTL\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let result = parse_command(resp);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("TTL requires exactly 1 argument"));
    }

    #[test]
    fn test_parse_expire_command_wrong_args() {
        // Too many arguments
        let input = b"*4\r\n$6\r\nEXPIRE\r\n$5\r\nmykey\r\n$2\r\n60\r\n$5\r\nextra\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let result = parse_command(resp);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("EXPIRE requires exactly 2 arguments"));

        // Too few arguments
        let input = b"*2\r\n$6\r\nEXPIRE\r\n$5\r\nmykey\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let result = parse_command(resp);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("EXPIRE requires exactly 2 arguments"));
    }

    #[test]
    fn test_parse_del_command() {
        let input = b"*2\r\n$3\r\nDEL\r\n$5\r\nmykey\r\n";
        let (resp, _) = parse_resp_with_remaining(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(
            command,
            RedisCommand::Del {
                key: "mykey".to_string()
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

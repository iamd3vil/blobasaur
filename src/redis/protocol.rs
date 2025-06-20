use bytes::Bytes;
use nom::{
    IResult,
    branch::alt,
    bytes::complete::take,
    character::complete::{char, digit1, line_ending},
    combinator::opt,
    multi::count,
};

/// Redis protocol data types
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Bytes>),
    Array(Option<Vec<RespValue>>),
}

/// Commands supported by Blobnom
#[derive(Debug, Clone, PartialEq)]
pub enum RedisCommand {
    Get { key: String },
    Set { key: String, value: Bytes },
    Del { key: String },
    Exists { key: String },
    Ping { message: Option<String> },
    Info { section: Option<String> },
    Command,
    Quit,
    Unknown(String),
}

/// Parse error types
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("Incomplete data")]
    Incomplete,
    #[error("Invalid protocol: {0}")]
    Invalid(String),
}

/// Parse a complete RESP message
pub fn parse_resp(input: &[u8]) -> Result<RespValue, ParseError> {
    match resp_value(input) {
        Ok((remaining, value)) => {
            if remaining.is_empty() {
                Ok(value)
            } else {
                Err(ParseError::Invalid("Extra data after message".to_string()))
            }
        }
        Err(nom::Err::Incomplete(_)) => Err(ParseError::Incomplete),
        Err(e) => Err(ParseError::Invalid(format!("Parse error: {}", e))),
    }
}

/// Parse any RESP value
fn resp_value(input: &[u8]) -> IResult<&[u8], RespValue> {
    alt((simple_string, error_string, integer, bulk_string, array))(input)
}

/// Parse RESP simple string: +OK\r\n
fn simple_string(input: &[u8]) -> IResult<&[u8], RespValue> {
    let (input, _) = char('+')(input)?;
    let (input, content) = take_until_crlf(input)?;
    let (input, _) = line_ending(input)?;
    Ok((
        input,
        RespValue::SimpleString(String::from_utf8_lossy(content).to_string()),
    ))
}

/// Parse RESP error: -ERR message\r\n
fn error_string(input: &[u8]) -> IResult<&[u8], RespValue> {
    let (input, _) = char('-')(input)?;
    let (input, content) = take_until_crlf(input)?;
    let (input, _) = line_ending(input)?;
    Ok((
        input,
        RespValue::Error(String::from_utf8_lossy(content).to_string()),
    ))
}

/// Parse RESP integer: :1000\r\n
fn integer(input: &[u8]) -> IResult<&[u8], RespValue> {
    let (input, _) = char(':')(input)?;
    let (input, sign) = opt(char('-'))(input)?;
    let (input, digits) = digit1(input)?;
    let (input, _) = line_ending(input)?;

    let num_str = String::from_utf8_lossy(digits);
    let mut num: i64 = num_str.parse().map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Digit))
    })?;

    if sign.is_some() {
        num = -num;
    }

    Ok((input, RespValue::Integer(num)))
}

/// Parse RESP bulk string: $6\r\nfoobar\r\n or $-1\r\n for null
fn bulk_string(input: &[u8]) -> IResult<&[u8], RespValue> {
    let (input, _) = char('$')(input)?;
    let (input, length) = parse_length(input)?;
    let (input, _) = line_ending(input)?;

    if length == -1 {
        return Ok((input, RespValue::BulkString(None)));
    }

    let length = length as usize;
    let (input, data) = take(length)(input)?;
    let (input, _) = line_ending(input)?;

    Ok((
        input,
        RespValue::BulkString(Some(Bytes::copy_from_slice(data))),
    ))
}

/// Parse RESP array: *2\r\n$3\r\nGET\r\n$3\r\nkey\r\n or *-1\r\n for null
fn array(input: &[u8]) -> IResult<&[u8], RespValue> {
    let (input, _) = char('*')(input)?;
    let (input, length) = parse_length(input)?;
    let (input, _) = line_ending(input)?;

    if length == -1 {
        return Ok((input, RespValue::Array(None)));
    }

    let length = length as usize;
    let (input, elements) = count(resp_value, length)(input)?;

    Ok((input, RespValue::Array(Some(elements))))
}

/// Parse a signed integer length
fn parse_length(input: &[u8]) -> IResult<&[u8], i64> {
    let (input, sign) = opt(char('-'))(input)?;
    let (input, digits) = digit1(input)?;

    let num_str = String::from_utf8_lossy(digits);
    let mut num: i64 = num_str.parse().map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Digit))
    })?;

    if sign.is_some() {
        num = -num;
    }

    Ok((input, num))
}

/// Take bytes until CRLF (but don't consume CRLF)
fn take_until_crlf(input: &[u8]) -> IResult<&[u8], &[u8]> {
    let mut pos = 0;
    while pos + 1 < input.len() {
        if input[pos] == b'\r' && input[pos + 1] == b'\n' {
            return Ok((&input[pos..], &input[..pos]));
        }
        pos += 1;
    }
    Err(nom::Err::Incomplete(nom::Needed::Size(
        std::num::NonZeroUsize::new(2).unwrap(),
    )))
}

/// Parse a Redis command from RESP value
pub fn parse_command(resp: RespValue) -> Result<RedisCommand, ParseError> {
    match resp {
        RespValue::Array(Some(elements)) if !elements.is_empty() => parse_command_array(elements),
        RespValue::Array(Some(_)) => Err(ParseError::Invalid("Empty command array".to_string())),
        RespValue::Array(None) => Err(ParseError::Invalid("Null command array".to_string())),
        _ => Err(ParseError::Invalid("Commands must be arrays".to_string())),
    }
}

/// Parse command from array of RESP values
fn parse_command_array(elements: Vec<RespValue>) -> Result<RedisCommand, ParseError> {
    let command_name = match &elements[0] {
        RespValue::BulkString(Some(data)) => String::from_utf8_lossy(data).to_uppercase(),
        RespValue::SimpleString(s) => s.to_uppercase(),
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
            if elements.len() != 3 {
                return Err(ParseError::Invalid(
                    "SET requires exactly 2 arguments".to_string(),
                ));
            }
            let key = extract_string(&elements[1])?;
            let value = extract_bytes(&elements[2])?;
            Ok(RedisCommand::Set { key, value })
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
        "QUIT" => Ok(RedisCommand::Quit),
        _ => Ok(RedisCommand::Unknown(command_name)),
    }
}

/// Extract string from RESP value
fn extract_string(value: &RespValue) -> Result<String, ParseError> {
    match value {
        RespValue::BulkString(Some(data)) => Ok(String::from_utf8_lossy(data).to_string()),
        RespValue::SimpleString(s) => Ok(s.clone()),
        RespValue::BulkString(None) => Err(ParseError::Invalid(
            "Cannot use null as string argument".to_string(),
        )),
        _ => Err(ParseError::Invalid("Expected string argument".to_string())),
    }
}

/// Extract bytes from RESP value
fn extract_bytes(value: &RespValue) -> Result<Bytes, ParseError> {
    match value {
        RespValue::BulkString(Some(data)) => Ok(data.clone()),
        RespValue::SimpleString(s) => Ok(Bytes::copy_from_slice(s.as_bytes())),
        RespValue::BulkString(None) => Err(ParseError::Invalid(
            "Cannot use null as byte argument".to_string(),
        )),
        _ => Err(ParseError::Invalid(
            "Expected string/bytes argument".to_string(),
        )),
    }
}

/// Serialize RESP value to bytes
impl RespValue {
    pub fn serialize(&self) -> Bytes {
        match self {
            RespValue::SimpleString(s) => Bytes::from(format!("+{}\r\n", s)),
            RespValue::Error(s) => Bytes::from(format!("-{}\r\n", s)),
            RespValue::Integer(i) => Bytes::from(format!(":{}\r\n", i)),
            RespValue::BulkString(Some(data)) => {
                let mut result = format!("${}\r\n", data.len()).into_bytes();
                result.extend_from_slice(data);
                result.extend_from_slice(b"\r\n");
                Bytes::from(result)
            }
            RespValue::BulkString(None) => Bytes::from_static(b"$-1\r\n"),
            RespValue::Array(Some(elements)) => {
                let mut result = format!("*{}\r\n", elements.len()).into_bytes();
                for element in elements {
                    result.extend_from_slice(&element.serialize());
                }
                Bytes::from(result)
            }
            RespValue::Array(None) => Bytes::from_static(b"*-1\r\n"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_string() {
        let input = b"+OK\r\n";
        let result = parse_resp(input).unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    }

    #[test]
    fn test_parse_error() {
        let input = b"-ERR unknown command\r\n";
        let result = parse_resp(input).unwrap();
        assert_eq!(result, RespValue::Error("ERR unknown command".to_string()));
    }

    #[test]
    fn test_parse_integer() {
        let input = b":1000\r\n";
        let result = parse_resp(input).unwrap();
        assert_eq!(result, RespValue::Integer(1000));

        let input = b":-123\r\n";
        let result = parse_resp(input).unwrap();
        assert_eq!(result, RespValue::Integer(-123));
    }

    #[test]
    fn test_parse_bulk_string() {
        let input = b"$6\r\nfoobar\r\n";
        let result = parse_resp(input).unwrap();
        assert_eq!(
            result,
            RespValue::BulkString(Some(Bytes::from_static(b"foobar")))
        );

        // Null bulk string
        let input = b"$-1\r\n";
        let result = parse_resp(input).unwrap();
        assert_eq!(result, RespValue::BulkString(None));

        // Empty bulk string
        let input = b"$0\r\n\r\n";
        let result = parse_resp(input).unwrap();
        assert_eq!(result, RespValue::BulkString(Some(Bytes::new())));
    }

    #[test]
    fn test_parse_array() {
        let input = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let result = parse_resp(input).unwrap();
        let expected = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"GET"))),
            RespValue::BulkString(Some(Bytes::from_static(b"key"))),
        ]));
        assert_eq!(result, expected);

        // Null array
        let input = b"*-1\r\n";
        let result = parse_resp(input).unwrap();
        assert_eq!(result, RespValue::Array(None));

        // Empty array
        let input = b"*0\r\n";
        let result = parse_resp(input).unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![])));
    }

    #[test]
    fn test_parse_nested_array() {
        let input = b"*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n";
        let result = parse_resp(input).unwrap();
        let expected = RespValue::Array(Some(vec![
            RespValue::Array(Some(vec![
                RespValue::Integer(1),
                RespValue::Integer(2),
                RespValue::Integer(3),
            ])),
            RespValue::Array(Some(vec![
                RespValue::SimpleString("Foo".to_string()),
                RespValue::Error("Bar".to_string()),
            ])),
        ]));
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_get_command() {
        let input = b"*2\r\n$3\r\nGET\r\n$7\r\nmykey42\r\n";
        let resp = parse_resp(input).unwrap();
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
        let resp = parse_resp(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(
            command,
            RedisCommand::Set {
                key: "mykey".to_string(),
                value: Bytes::from_static(b"hello world")
            }
        );
    }

    #[test]
    fn test_parse_del_command() {
        let input = b"*2\r\n$3\r\nDEL\r\n$5\r\nmykey\r\n";
        let resp = parse_resp(input).unwrap();
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
        let resp = parse_resp(input).unwrap();
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
        let resp = parse_resp(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(command, RedisCommand::Ping { message: None });

        // PING with message
        let input = b"*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n";
        let resp = parse_resp(input).unwrap();
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
        let resp = parse_resp(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(command, RedisCommand::Info { section: None });

        // INFO with section
        let input = b"*2\r\n$4\r\nINFO\r\n$6\r\nserver\r\n";
        let resp = parse_resp(input).unwrap();
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
        let resp = parse_resp(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(command, RedisCommand::Quit);
    }

    #[test]
    fn test_parse_command_command() {
        let input = b"*1\r\n$7\r\nCOMMAND\r\n";
        let resp = parse_resp(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(command, RedisCommand::Command);
    }

    #[test]
    fn test_parse_unknown_command() {
        let input = b"*2\r\n$7\r\nUNKNOWN\r\n$3\r\narg\r\n";
        let resp = parse_resp(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(command, RedisCommand::Unknown("UNKNOWN".to_string()));
    }

    #[test]
    fn test_parse_case_insensitive_commands() {
        let input = b"*2\r\n$3\r\nget\r\n$5\r\nmykey\r\n";
        let resp = parse_resp(input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(
            command,
            RedisCommand::Get {
                key: "mykey".to_string()
            }
        );
    }

    #[test]
    fn test_parse_binary_data() {
        let binary_data = b"\x00\x01\x02\xff\xfe\xfd";
        let mut input = b"*3\r\n$3\r\nSET\r\n$6\r\nbinary\r\n$6\r\n".to_vec();
        input.extend_from_slice(binary_data);
        input.extend_from_slice(b"\r\n");

        let resp = parse_resp(&input).unwrap();
        let command = parse_command(resp).unwrap();
        assert_eq!(
            command,
            RedisCommand::Set {
                key: "binary".to_string(),
                value: Bytes::copy_from_slice(binary_data)
            }
        );
    }

    #[test]
    fn test_serialize_simple_string() {
        let value = RespValue::SimpleString("OK".to_string());
        assert_eq!(value.serialize().as_ref(), b"+OK\r\n");
    }

    #[test]
    fn test_serialize_error() {
        let value = RespValue::Error("ERR something".to_string());
        assert_eq!(value.serialize().as_ref(), b"-ERR something\r\n");
    }

    #[test]
    fn test_serialize_integer() {
        let value = RespValue::Integer(42);
        assert_eq!(value.serialize().as_ref(), b":42\r\n");

        let value = RespValue::Integer(-42);
        assert_eq!(value.serialize().as_ref(), b":-42\r\n");
    }

    #[test]
    fn test_serialize_bulk_string() {
        let value = RespValue::BulkString(Some(Bytes::from_static(b"hello")));
        assert_eq!(value.serialize().as_ref(), b"$5\r\nhello\r\n");

        let value = RespValue::BulkString(None);
        assert_eq!(value.serialize().as_ref(), b"$-1\r\n");

        let value = RespValue::BulkString(Some(Bytes::new()));
        assert_eq!(value.serialize().as_ref(), b"$0\r\n\r\n");
    }

    #[test]
    fn test_serialize_array() {
        let value = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"GET"))),
            RespValue::BulkString(Some(Bytes::from_static(b"key"))),
        ]));
        assert_eq!(
            value.serialize().as_ref(),
            b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"
        );

        let value = RespValue::Array(None);
        assert_eq!(value.serialize().as_ref(), b"*-1\r\n");

        let value = RespValue::Array(Some(vec![]));
        assert_eq!(value.serialize().as_ref(), b"*0\r\n");
    }

    #[test]
    fn test_incomplete_data() {
        let input = b"*2\r\n$3\r\nGET\r\n$3\r\nke"; // Incomplete
        let result = parse_resp(input);
        assert!(matches!(result, Err(ParseError::Invalid(_))));
    }

    #[test]
    fn test_invalid_protocol() {
        let input = b"invalid\r\n";
        let result = parse_resp(input);
        assert!(matches!(result, Err(ParseError::Invalid(_))));
    }

    #[test]
    fn test_command_validation() {
        // GET with wrong number of args
        let elements = vec![RespValue::BulkString(Some(Bytes::from_static(b"GET")))];
        let result = parse_command_array(elements);
        assert!(matches!(result, Err(ParseError::Invalid(_))));

        // SET with wrong number of args
        let elements = vec![
            RespValue::BulkString(Some(Bytes::from_static(b"SET"))),
            RespValue::BulkString(Some(Bytes::from_static(b"key"))),
        ];
        let result = parse_command_array(elements);
        assert!(matches!(result, Err(ParseError::Invalid(_))));
    }

    #[test]
    fn test_roundtrip_serialization() {
        let original = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"SET"))),
            RespValue::BulkString(Some(Bytes::from_static(b"mykey"))),
            RespValue::BulkString(Some(Bytes::from_static(b"myvalue"))),
        ]));

        let serialized = original.serialize();
        let parsed = parse_resp(&serialized).unwrap();
        assert_eq!(original, parsed);
    }
}

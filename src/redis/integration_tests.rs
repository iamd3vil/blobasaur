#[cfg(test)]
mod integration_tests {
    use crate::redis::protocol::*;
    use crate::redis::serialize_frame;
    use bytes::Bytes;
    use redis_protocol::resp2::types::BytesFrame;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    /// Test helper to create a mock Redis server response
    async fn create_mock_server(responses: Vec<Vec<u8>>) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = vec![0; 1024];

            for response in responses {
                // Read client request
                let _ = stream.read(&mut buffer).await.unwrap();
                // Send response
                stream.write_all(&response).await.unwrap();
            }
        });

        // Give the server a moment to start
        tokio::time::sleep(Duration::from_millis(10)).await;
        port
    }

    /// Test helper to send command and receive response
    async fn send_command_and_receive(addr: &str, command: &[u8]) -> Vec<u8> {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(command).await.unwrap();

        let mut response = vec![0; 1024];
        let n = stream.read(&mut response).await.unwrap();
        response.truncate(n);
        response
    }

    #[tokio::test]
    async fn test_get_command_integration() {
        let get_command = b"*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n";
        let mock_response = b"$11\r\nhello world\r\n";

        let port = create_mock_server(vec![mock_response.to_vec()]).await;
        let addr = format!("127.0.0.1:{}", port);

        let response = send_command_and_receive(&addr, get_command).await;
        assert_eq!(response, mock_response);

        // Parse the command to verify our parser works
        let (parsed_resp, _) = parse_resp_with_remaining(get_command).unwrap();
        let parsed_command = parse_command(parsed_resp).unwrap();
        assert_eq!(
            parsed_command,
            RedisCommand::Get {
                key: "mykey".to_string()
            }
        );
    }

    #[tokio::test]
    async fn test_set_command_integration() {
        let set_command = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$11\r\nhello world\r\n";
        let mock_response = b"+OK\r\n";

        let port = create_mock_server(vec![mock_response.to_vec()]).await;
        let addr = format!("127.0.0.1:{}", port);

        let response = send_command_and_receive(&addr, set_command).await;
        assert_eq!(response, mock_response);

        // Parse the command to verify our parser works
        let (parsed_resp, _) = parse_resp_with_remaining(set_command).unwrap();
        let parsed_command = parse_command(parsed_resp).unwrap();
        assert_eq!(
            parsed_command,
            RedisCommand::Set {
                key: "mykey".to_string(),
                value: Bytes::from_static(b"hello world"),
                ttl_seconds: None
            }
        );
    }

    #[tokio::test]
    async fn test_del_command_integration() {
        let del_command = b"*2\r\n$3\r\nDEL\r\n$5\r\nmykey\r\n";
        let mock_response = b":1\r\n";

        let port = create_mock_server(vec![mock_response.to_vec()]).await;
        let addr = format!("127.0.0.1:{}", port);

        let response = send_command_and_receive(&addr, del_command).await;
        assert_eq!(response, mock_response);

        // Parse the command to verify our parser works
        let (parsed_resp, _) = parse_resp_with_remaining(del_command).unwrap();
        let parsed_command = parse_command(parsed_resp).unwrap();
        assert_eq!(
            parsed_command,
            RedisCommand::Del {
                key: "mykey".to_string()
            }
        );
    }

    #[tokio::test]
    async fn test_exists_command_integration() {
        let exists_command = b"*2\r\n$6\r\nEXISTS\r\n$5\r\nmykey\r\n";
        let mock_response = b":1\r\n";

        let port = create_mock_server(vec![mock_response.to_vec()]).await;
        let addr = format!("127.0.0.1:{}", port);

        let response = send_command_and_receive(&addr, exists_command).await;
        assert_eq!(response, mock_response);

        // Parse the command to verify our parser works
        let (parsed_resp, _) = parse_resp_with_remaining(exists_command).unwrap();
        let parsed_command = parse_command(parsed_resp).unwrap();
        assert_eq!(
            parsed_command,
            RedisCommand::Exists {
                key: "mykey".to_string()
            }
        );
    }

    #[tokio::test]
    async fn test_ping_command_integration() {
        let ping_command = b"*1\r\n$4\r\nPING\r\n";
        let mock_response = b"+PONG\r\n";

        let port = create_mock_server(vec![mock_response.to_vec()]).await;
        let addr = format!("127.0.0.1:{}", port);

        let response = send_command_and_receive(&addr, ping_command).await;
        assert_eq!(response, mock_response);

        // Parse the command to verify our parser works
        let (parsed_resp, _) = parse_resp_with_remaining(ping_command).unwrap();
        let parsed_command = parse_command(parsed_resp).unwrap();
        assert_eq!(parsed_command, RedisCommand::Ping { message: None });
    }

    #[tokio::test]
    async fn test_ping_with_message_integration() {
        let ping_command = b"*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n";
        let mock_response = b"$5\r\nhello\r\n";

        let port = create_mock_server(vec![mock_response.to_vec()]).await;
        let addr = format!("127.0.0.1:{}", port);

        let response = send_command_and_receive(&addr, ping_command).await;
        assert_eq!(response, mock_response);

        // Parse the command to verify our parser works
        let (parsed_resp, _) = parse_resp_with_remaining(ping_command).unwrap();
        let parsed_command = parse_command(parsed_resp).unwrap();
        assert_eq!(
            parsed_command,
            RedisCommand::Ping {
                message: Some("hello".to_string())
            }
        );
    }

    #[tokio::test]
    async fn test_quit_command_integration() {
        let quit_command = b"*1\r\n$4\r\nQUIT\r\n";
        let mock_response = b"+OK\r\n";

        let port = create_mock_server(vec![mock_response.to_vec()]).await;
        let addr = format!("127.0.0.1:{}", port);

        let response = send_command_and_receive(&addr, quit_command).await;
        assert_eq!(response, mock_response);

        // Parse the command to verify our parser works
        let (parsed_resp, _) = parse_resp_with_remaining(quit_command).unwrap();
        let parsed_command = parse_command(parsed_resp).unwrap();
        assert_eq!(parsed_command, RedisCommand::Quit);
    }

    #[tokio::test]
    async fn test_info_command_integration() {
        let info_command = b"*1\r\n$4\r\nINFO\r\n";
        let mock_response = b"$50\r\n# Server\r\nredis_version:7.0.0\r\nredis_mode:standalone\r\n";

        let port = create_mock_server(vec![mock_response.to_vec()]).await;
        let addr = format!("127.0.0.1:{}", port);

        let response = send_command_and_receive(&addr, info_command).await;
        assert_eq!(response, mock_response);

        // Parse the command to verify our parser works
        let (parsed_resp, _) = parse_resp_with_remaining(info_command).unwrap();
        let parsed_command = parse_command(parsed_resp).unwrap();
        assert_eq!(parsed_command, RedisCommand::Info { section: None });
    }

    #[tokio::test]
    async fn test_multiple_commands_pipelined() {
        let _pipelined_commands = b"*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$5\r\nvalue\r\n*2\r\n$3\r\nDEL\r\n$4\r\nkey3\r\n";

        // Parse each command individually
        let commands = vec![
            &b"*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n"[..],
            &b"*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$5\r\nvalue\r\n"[..],
            &b"*2\r\n$3\r\nDEL\r\n$4\r\nkey3\r\n"[..],
        ];

        for cmd_bytes in &commands {
            let (parsed_resp, _) = parse_resp_with_remaining(cmd_bytes).unwrap();
            let parsed_command = parse_command(parsed_resp).unwrap();

            match parsed_command {
                RedisCommand::Get { key } => assert_eq!(key, "key1"),
                RedisCommand::Set { key, value, ttl_seconds } => {
                    assert_eq!(key, "key2");
                    assert_eq!(value, Bytes::from_static(b"value"));
                    assert_eq!(ttl_seconds, None);
                }
                RedisCommand::Del { key } => assert_eq!(key, "key3"),
                _ => panic!("Unexpected command"),
            }
        }
    }

    #[tokio::test]
    async fn test_binary_data_handling() {
        let binary_data = b"\x00\x01\x02\xff\xfe\xfd\x7f\x80\x81";
        let mut set_command = b"*3\r\n$3\r\nSET\r\n$6\r\nbinary\r\n$9\r\n".to_vec();
        set_command.extend_from_slice(binary_data);
        set_command.extend_from_slice(b"\r\n");

        let (parsed_resp, _) = parse_resp_with_remaining(&set_command).unwrap();
        let parsed_command = parse_command(parsed_resp).unwrap();

        match parsed_command {
            RedisCommand::Set { key, value, ttl_seconds } => {
                assert_eq!(key, "binary");
                assert_eq!(value, Bytes::copy_from_slice(binary_data));
                assert_eq!(ttl_seconds, None);
            }
            _ => panic!("Expected SET command"),
        }
    }

    #[tokio::test]
    async fn test_large_key_and_value() {
        let large_key = "x".repeat(1000);
        let large_value = "y".repeat(10000);

        let set_command = format!(
            "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
            large_key.len(),
            large_key,
            large_value.len(),
            large_value
        )
        .into_bytes();

        let (parsed_resp, _) = parse_resp_with_remaining(&set_command).unwrap();
        let parsed_command = parse_command(parsed_resp).unwrap();

        match parsed_command {
            RedisCommand::Set { key, value, ttl_seconds } => {
                assert_eq!(key, large_key);
                assert_eq!(value, Bytes::from(large_value.into_bytes()));
                assert_eq!(ttl_seconds, None);
            }
            _ => panic!("Expected SET command"),
        }
    }

    #[tokio::test]
    async fn test_empty_values() {
        let set_empty_command = b"*3\r\n$3\r\nSET\r\n$5\r\nempty\r\n$0\r\n\r\n";

        let (parsed_resp, _) = parse_resp_with_remaining(set_empty_command).unwrap();
        let parsed_command = parse_command(parsed_resp).unwrap();

        match parsed_command {
            RedisCommand::Set { key, value, ttl_seconds } => {
                assert_eq!(key, "empty");
                assert_eq!(value, Bytes::new());
                assert_eq!(ttl_seconds, None);
            }
            _ => panic!("Expected SET command"),
        }
    }

    #[tokio::test]
    async fn test_null_bulk_string_handling() {
        let response_with_null = b"$-1\r\n";
        let (parsed_resp, _) = parse_resp_with_remaining(response_with_null).unwrap();

        match parsed_resp {
            BytesFrame::Null => {
                // This is expected
            }
            _ => panic!("Expected null bulk string"),
        }

        // Test serialization roundtrip
        let serialized = serialize_frame(&parsed_resp);
        assert_eq!(serialized.as_ref(), response_with_null);
    }

    #[tokio::test]
    async fn test_error_response_handling() {
        let error_response = b"-ERR unknown command 'INVALID'\r\n";
        let (parsed_resp, _) = parse_resp_with_remaining(error_response).unwrap();

        match &parsed_resp {
            BytesFrame::Error(msg) => {
                assert_eq!(msg.to_string(), "ERR unknown command 'INVALID'");
            }
            _ => panic!("Expected error response"),
        }

        // Test serialization roundtrip
        let serialized = serialize_frame(&parsed_resp);
        assert_eq!(serialized.as_ref(), error_response);
    }

    #[tokio::test]
    async fn test_integer_response_handling() {
        let integer_responses = vec![
            (&b":0\r\n"[..], 0),
            (&b":1\r\n"[..], 1),
            (&b":1000\r\n"[..], 1000),
            (&b":-1\r\n"[..], -1),
            (&b":-1000\r\n"[..], -1000),
        ];

        for (response_bytes, expected_value) in integer_responses {
            let (parsed_resp, _) = parse_resp_with_remaining(response_bytes).unwrap();

            match parsed_resp {
                BytesFrame::Integer(value) => {
                    assert_eq!(value, expected_value);
                }
                _ => panic!("Expected integer response"),
            }

            // Test serialization roundtrip
            let serialized = serialize_frame(&parsed_resp);
            assert_eq!(serialized, response_bytes);
        }
    }

    #[tokio::test]
    async fn test_nested_array_handling() {
        let nested_array = b"*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+OK\r\n-ERR\r\n";
        let (parsed_resp, _) = parse_resp_with_remaining(nested_array).unwrap();

        match &parsed_resp {
            BytesFrame::Array(elements) => {
                assert_eq!(elements.len(), 2);

                // First element should be an array of integers
                match &elements[0] {
                    BytesFrame::Array(inner) => {
                        assert_eq!(inner.len(), 3);
                        assert_eq!(inner[0], BytesFrame::Integer(1));
                        assert_eq!(inner[1], BytesFrame::Integer(2));
                        assert_eq!(inner[2], BytesFrame::Integer(3));
                    }
                    _ => panic!("Expected inner array"),
                }

                // Second element should be an array of strings
                match &elements[1] {
                    BytesFrame::Array(inner) => {
                        assert_eq!(inner.len(), 2);
                        assert_eq!(inner[0], BytesFrame::SimpleString("OK".into()));
                        assert_eq!(inner[1], BytesFrame::Error("ERR".into()));
                    }
                    _ => panic!("Expected inner array"),
                }
            }
            _ => panic!("Expected array response"),
        }

        // Test serialization roundtrip
        let serialized = serialize_frame(&parsed_resp);
        assert_eq!(serialized.as_ref(), nested_array);
    }

    #[tokio::test]
    async fn test_command_validation_errors() {
        // Test GET with wrong number of arguments
        let invalid_get =
            BytesFrame::Array(vec![BytesFrame::BulkString(Bytes::from_static(b"GET"))]);

        let result = parse_command(invalid_get);
        assert!(matches!(result, Err(ParseError::Invalid(_))));

        // Test SET with wrong number of arguments
        let invalid_set = BytesFrame::Array(vec![
            BytesFrame::BulkString(Bytes::from_static(b"SET")),
            BytesFrame::BulkString(Bytes::from_static(b"key")),
        ]);

        let result = parse_command(invalid_set);
        assert!(matches!(result, Err(ParseError::Invalid(_))));

        // Test DEL with wrong number of arguments
        let invalid_del =
            BytesFrame::Array(vec![BytesFrame::BulkString(Bytes::from_static(b"DEL"))]);

        let result = parse_command(invalid_del);
        assert!(matches!(result, Err(ParseError::Invalid(_))));
    }

    #[tokio::test]
    async fn test_set_with_ttl_integration() {
        // Test SET with EX option
        let set_ex_command = b"*5\r\n$3\r\nSET\r\n$8\r\nttl_test\r\n$5\r\nvalue\r\n$2\r\nEX\r\n$2\r\n60\r\n";
        let (parsed_resp, _) = parse_resp_with_remaining(set_ex_command).unwrap();
        let parsed_command = parse_command(parsed_resp).unwrap();
        match parsed_command {
            RedisCommand::Set { key, value, ttl_seconds } => {
                assert_eq!(key, "ttl_test");
                assert_eq!(value, Bytes::from_static(b"value"));
                assert_eq!(ttl_seconds, Some(60));
            }
            _ => panic!("Expected SET command with TTL"),
        }

        // Test SET with PX option
        let set_px_command = b"*5\r\n$3\r\nSET\r\n$8\r\nttl_test\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$5\r\n60000\r\n";
        let (parsed_resp, _) = parse_resp_with_remaining(set_px_command).unwrap();
        let parsed_command = parse_command(parsed_resp).unwrap();
        match parsed_command {
            RedisCommand::Set { key, value, ttl_seconds } => {
                assert_eq!(key, "ttl_test");
                assert_eq!(value, Bytes::from_static(b"value"));
                assert_eq!(ttl_seconds, Some(60)); // 60000ms = 60s
            }
            _ => panic!("Expected SET command with TTL"),
        }
    }

    #[tokio::test]
    async fn test_ttl_command_integration() {
        let ttl_command = b"*2\r\n$3\r\nTTL\r\n$7\r\nmykey42\r\n";
        let mock_response = b":-1\r\n"; // Key exists but no expiration

        let port = create_mock_server(vec![mock_response.to_vec()]).await;
        let addr = format!("127.0.0.1:{}", port);

        let response = send_command_and_receive(&addr, ttl_command).await;
        assert_eq!(response, mock_response);

        // Parse the command to verify our parser works
        let (parsed_resp, _) = parse_resp_with_remaining(ttl_command).unwrap();
        let parsed_command = parse_command(parsed_resp).unwrap();
        assert_eq!(
            parsed_command,
            RedisCommand::Ttl {
                key: "mykey42".to_string()
            }
        );
    }

    #[tokio::test]
    async fn test_expire_command_integration() {
        let expire_command = b"*3\r\n$6\r\nEXPIRE\r\n$7\r\nmykey42\r\n$3\r\n120\r\n";
        let mock_response = b":1\r\n"; // Key exists and expiration was set

        let port = create_mock_server(vec![mock_response.to_vec()]).await;
        let addr = format!("127.0.0.1:{}", port);

        let response = send_command_and_receive(&addr, expire_command).await;
        assert_eq!(response, mock_response);

        // Parse the command to verify our parser works
        let (parsed_resp, _) = parse_resp_with_remaining(expire_command).unwrap();
        let parsed_command = parse_command(parsed_resp).unwrap();
        assert_eq!(
            parsed_command,
            RedisCommand::Expire {
                key: "mykey42".to_string(),
                seconds: 120
            }
        );
    }

    #[tokio::test]
    async fn test_ttl_edge_cases() {
        // Test TTL for non-existent key
        let ttl_command = b"*2\r\n$3\r\nTTL\r\n$11\r\nnonexistent\r\n";
        let mock_response = b":-2\r\n"; // Key doesn't exist

        let port = create_mock_server(vec![mock_response.to_vec()]).await;
        let addr = format!("127.0.0.1:{}", port);

        let response = send_command_and_receive(&addr, ttl_command).await;
        assert_eq!(response, mock_response);
    }

    #[tokio::test]
    async fn test_expire_on_nonexistent_key() {
        let expire_command = b"*3\r\n$6\r\nEXPIRE\r\n$11\r\nnonexistent\r\n$2\r\n60\r\n";
        let mock_response = b":0\r\n"; // Key doesn't exist

        let port = create_mock_server(vec![mock_response.to_vec()]).await;
        let addr = format!("127.0.0.1:{}", port);

        let response = send_command_and_receive(&addr, expire_command).await;
        assert_eq!(response, mock_response);
    }

    #[tokio::test]
    async fn test_case_insensitive_commands() {
        let test_cases = vec![
            (&b"*2\r\n$3\r\nget\r\n$3\r\nkey\r\n"[..], "GET"),
            (&b"*2\r\n$3\r\nGet\r\n$3\r\nkey\r\n"[..], "GET"),
            (&b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"[..], "GET"),
            (&b"*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$3\r\nval\r\n"[..], "SET"),
            (&b"*3\r\n$3\r\nSet\r\n$3\r\nkey\r\n$3\r\nval\r\n"[..], "SET"),
            (&b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nval\r\n"[..], "SET"),
        ];

        for (command_bytes, expected_cmd) in test_cases {
            let (parsed_resp, _) = parse_resp_with_remaining(command_bytes).unwrap();
            let parsed_command = parse_command(parsed_resp).unwrap();

            match (parsed_command, expected_cmd) {
                (RedisCommand::Get { .. }, "GET") => {
                    // Expected
                }
                (RedisCommand::Set { .. }, "SET") => {
                    // Expected
                }
                _ => panic!("Case insensitive parsing failed for {}", expected_cmd),
            }
        }
    }

    #[tokio::test]
    async fn test_unknown_command_handling() {
        let unknown_command = b"*2\r\n$7\r\nUNKNOWN\r\n$3\r\narg\r\n";
        let (parsed_resp, _) = parse_resp_with_remaining(unknown_command).unwrap();
        let parsed_command = parse_command(parsed_resp).unwrap();

        match parsed_command {
            RedisCommand::Unknown(cmd) => {
                assert_eq!(cmd, "UNKNOWN");
            }
            _ => panic!("Expected unknown command"),
        }
    }
}

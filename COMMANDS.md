# Redis Commands

This document tracks all Redis commands - both implemented and planned - in Blobasaur.

## Command Status Legend

- [x] Implemented
- [ ] Planned

## String Commands

| Command | Status | Description | Notes |
|---------|--------|-------------|-------|
| GET | [x] | Get the value of a key | Excludes expired keys |
| SET | [x] | Set a key to a value | Supports `EX`, `PX` options |
| DEL | [x] | Delete one or more keys | Multiple keys supported |
| EXISTS | [x] | Check if a key exists | Excludes expired keys |
| MGET | [x] | Get values of multiple keys | Batch read operation |
| MSET | [x] | Set multiple keys | Batch write operation |
| SETNX | [ ] | Set key only if it doesn't exist | Useful for locking |
| SETEX | [ ] | Set key with expiration (seconds) | Atomic set+expire |
| PSETEX | [ ] | Set key with expiration (milliseconds) | Atomic set+expire |
| GETEX | [ ] | Get value and optionally set expiration | |
| APPEND | [ ] | Append value to a key | |
| STRLEN | [ ] | Get length of string value | |
| GETRANGE | [ ] | Get substring of string value | Useful for partial blob reads |
| SETRANGE | [ ] | Overwrite part of string at offset | |
| GETSET | [ ] | Set key and return old value | Deprecated in Redis 6.2+ |
| GETDEL | [ ] | Get value and delete key | |

## Key Expiration Commands

| Command | Status | Description | Notes |
|---------|--------|-------------|-------|
| TTL | [x] | Get TTL in seconds | Returns -1 (no expiry), -2 (not found) |
| EXPIRE | [x] | Set expiration in seconds | |
| PTTL | [ ] | Get TTL in milliseconds | |
| PEXPIRE | [ ] | Set expiration in milliseconds | |
| EXPIREAT | [ ] | Set expiration at Unix timestamp (seconds) | |
| PEXPIREAT | [ ] | Set expiration at Unix timestamp (milliseconds) | |
| PERSIST | [ ] | Remove expiration from key | |
| EXPIRETIME | [ ] | Get expiration Unix timestamp (seconds) | Redis 7.0+ |
| PEXPIRETIME | [ ] | Get expiration Unix timestamp (milliseconds) | Redis 7.0+ |

## Hash Commands

| Command | Status | Description | Notes |
|---------|--------|-------------|-------|
| HGET | [x] | Get value of hash field | Uses namespace as hash name |
| HSET | [x] | Set hash field value | Uses namespace as hash name |
| HSETEX | [x] | Set hash fields with expiration | Supports `FNX`, `FXX`, `EX`, `PX`, `EXAT`, `PXAT`, `KEEPTTL`, `FIELDS` |
| HDEL | [x] | Delete hash field | |
| HEXISTS | [x] | Check if hash field exists | |
| HMGET | [x] | Get values of multiple hash fields | Batch read |
| HMSET | [x] | Set multiple hash fields | Deprecated but widely used |
| HGETALL | [ ] | Get all fields and values | Useful for namespace inspection |
| HKEYS | [ ] | Get all field names in hash | |
| HVALS | [ ] | Get all values in hash | |
| HLEN | [ ] | Get number of fields in hash | |
| HSCAN | [ ] | Iterate hash fields | Non-blocking iteration |
| HINCRBY | [ ] | Increment hash field by integer | |
| HINCRBYFLOAT | [ ] | Increment hash field by float | |
| HSETNX | [ ] | Set hash field only if not exists | |
| HSTRLEN | [ ] | Get length of hash field value | |
| HRANDFIELD | [ ] | Get random field(s) from hash | Redis 6.2+ |
| HTTL | [ ] | Get TTL of hash field | Redis 7.4+ |
| HPTTL | [ ] | Get TTL of hash field in milliseconds | Redis 7.4+ |
| HEXPIRE | [ ] | Set expiration on hash field | Redis 7.4+ |
| HPEXPIRE | [ ] | Set expiration on hash field (milliseconds) | Redis 7.4+ |
| HPERSIST | [ ] | Remove expiration from hash field | Redis 7.4+ |

## Key Management Commands

| Command | Status | Description | Notes |
|---------|--------|-------------|-------|
| KEYS | [ ] | Find keys matching pattern | Warning: blocks on large datasets |
| SCAN | [ ] | Iterate keys incrementally | Safe for production |
| TYPE | [ ] | Get type of key | |
| RENAME | [ ] | Rename a key | |
| RENAMENX | [ ] | Rename key only if new name doesn't exist | |
| COPY | [ ] | Copy key to another key | Redis 6.2+ |
| OBJECT | [ ] | Inspect internals of keys | |
| TOUCH | [ ] | Update last access time | |
| UNLINK | [ ] | Delete keys asynchronously | Non-blocking DEL |
| DUMP | [ ] | Serialize key value | |
| RESTORE | [ ] | Deserialize and restore key | |

## Server Commands

| Command | Status | Description | Notes |
|---------|--------|-------------|-------|
| PING | [x] | Test connectivity | Supports optional message |
| INFO | [x] | Get server information | Supports `server`, `stats` sections |
| COMMAND | [x] | Get command information | Returns empty array (minimal) |
| QUIT | [x] | Close connection | |
| DBSIZE | [ ] | Get total number of keys | Useful for monitoring |
| FLUSHDB | [ ] | Delete all keys in database | Use with caution |
| FLUSHALL | [ ] | Delete all keys in all databases | Use with caution |
| TIME | [ ] | Get server time | |
| DEBUG | [ ] | Debug commands | Development only |
| MEMORY | [ ] | Memory usage commands | |
| CLIENT | [ ] | Client management | ID, LIST, KILL, etc. |
| CONFIG | [ ] | Get/set configuration | |
| SLOWLOG | [ ] | Get slow query log | |

## Cluster Commands

| Command | Status | Description | Notes |
|---------|--------|-------------|-------|
| CLUSTER NODES | [x] | Get cluster node information | |
| CLUSTER INFO | [x] | Get cluster status | |
| CLUSTER SLOTS | [x] | Get slot-to-node mapping | |
| CLUSTER ADDSLOTS | [x] | Add slots to node | |
| CLUSTER DELSLOTS | [x] | Remove slots from node | |
| CLUSTER KEYSLOT | [x] | Get slot number for key | |
| CLUSTER COUNTKEYSINSLOT | [ ] | Count keys in slot | |
| CLUSTER GETKEYSINSLOT | [ ] | Get keys in slot | |
| CLUSTER SETSLOT | [ ] | Set slot state | |
| CLUSTER REPLICATE | [ ] | Make node replica of another | |
| CLUSTER FAILOVER | [ ] | Manual failover | |
| CLUSTER RESET | [ ] | Reset cluster configuration | |
| CLUSTER MEET | [ ] | Add node to cluster | |
| CLUSTER FORGET | [ ] | Remove node from cluster | |
| CLUSTER SAVECONFIG | [ ] | Save cluster config | |
| CLUSTER MYID | [ ] | Get node ID | |
| CLUSTER SHARDS | [ ] | Get shard information | Redis 7.0+ |

## Transaction Commands

These commands are not planned as Blobasaur uses SQLite transactions internally.

| Command | Status | Description | Notes |
|---------|--------|-------------|-------|
| MULTI | [ ] | Start transaction | Not planned |
| EXEC | [ ] | Execute transaction | Not planned |
| DISCARD | [ ] | Discard transaction | Not planned |
| WATCH | [ ] | Watch keys for changes | Not planned |
| UNWATCH | [ ] | Unwatch all keys | Not planned |

## Pub/Sub Commands

Not currently planned - Blobasaur is focused on blob storage.

| Command | Status | Description | Notes |
|---------|--------|-------------|-------|
| SUBSCRIBE | [ ] | Subscribe to channels | Not planned |
| UNSUBSCRIBE | [ ] | Unsubscribe from channels | Not planned |
| PUBLISH | [ ] | Publish message to channel | Not planned |
| PSUBSCRIBE | [ ] | Subscribe to pattern | Not planned |
| PUNSUBSCRIBE | [ ] | Unsubscribe from pattern | Not planned |

## Scripting Commands

Not currently planned.

| Command | Status | Description | Notes |
|---------|--------|-------------|-------|
| EVAL | [ ] | Execute Lua script | Not planned |
| EVALSHA | [ ] | Execute cached Lua script | Not planned |
| SCRIPT | [ ] | Script management | Not planned |

## Data Structure Commands (Not Planned)

Blobasaur focuses on blob/string storage. The following Redis data structures are not planned:

- **Lists**: LPUSH, RPUSH, LPOP, RPOP, LRANGE, etc.
- **Sets**: SADD, SREM, SMEMBERS, SINTER, etc.
- **Sorted Sets**: ZADD, ZREM, ZRANGE, ZRANK, etc.
- **Streams**: XADD, XREAD, XRANGE, etc.
- **HyperLogLog**: PFADD, PFCOUNT, PFMERGE
- **Geospatial**: GEOADD, GEODIST, GEOSEARCH, etc.
- **Bitmaps**: SETBIT, GETBIT, BITCOUNT, etc.

## Implementation Priority

### High Priority (Recommended Next)

1. **SCAN** - Safe key iteration for production use
2. **HGETALL** - Inspect entire namespaces
3. **SETNX** - Conditional writes for locking/uniqueness
4. **KEYS** - Admin/debugging (with performance warnings)
5. **DBSIZE** - Monitoring support

### Medium Priority

6. **PTTL/PEXPIRE** - Millisecond precision TTL
7. **PERSIST** - Remove key expiration
8. **HKEYS/HVALS/HLEN** - Hash introspection
9. **HSCAN** - Safe hash field iteration
10. **TYPE** - Key type inspection

### Lower Priority

11. **SETEX/PSETEX** - Convenience commands (SET EX works)
12. **GETEX** - Get with expiration update
13. **FLUSHDB** - Database clearing (testing use)
14. **RENAME** - Key renaming
15. **TYPE** - Key type inspection

## Summary

| Category | Implemented | Planned | Total |
|----------|-------------|---------|-------|
| String | 6 | 10 | 16 |
| Key Expiration | 2 | 7 | 9 |
| Hash | 7 | 14 | 21 |
| Key Management | 0 | 12 | 12 |
| Server | 4 | 10 | 14 |
| Cluster | 6 | 10 | 16 |
| **Total** | **25** | **63** | **88** |

*Note: Transaction, Pub/Sub, Scripting, and other data structure commands are not planned as they fall outside Blobasaur's scope as a blob storage server.*

// Client compatibility tests: drive a running blobasaur with go-redis the way
// applications do. Run by tests/goredis_test.rs, which starts the server and
// sets BLOBASAUR_ADDR; each test runs over both RESP2 and RESP3 handshakes.
package goredis

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

// forEachProtocol runs fn against a fresh client per RESP version. Keys are
// prefixed with the test name and protocol so runs don't interfere.
func forEachProtocol(t *testing.T, fn func(t *testing.T, rdb *redis.Client, prefix string)) {
	addr := os.Getenv("BLOBASAUR_ADDR")
	if addr == "" {
		t.Skip("BLOBASAUR_ADDR not set; run via `cargo test --test goredis_test`")
	}
	for _, protocol := range []int{2, 3} {
		t.Run(fmt.Sprintf("resp%d", protocol), func(t *testing.T) {
			rdb := redis.NewClient(&redis.Options{Addr: addr, Protocol: protocol, PoolSize: 20})
			t.Cleanup(func() { rdb.Close() })
			fn(t, rdb, fmt.Sprintf("%s:resp%d:", t.Name(), protocol))
		})
	}
}

type resulter[T any] interface{ Result() (T, error) }

// expect runs a go-redis command's Result and compares it to want.
func expect[T comparable](t *testing.T, what string, cmd resulter[T], want T) {
	t.Helper()
	got, err := cmd.Result()
	if err != nil {
		t.Fatalf("%s: unexpected error: %v", what, err)
	}
	if got != want {
		t.Fatalf("%s: got %v, want %v", what, got, want)
	}
}

// namespace turns a key prefix into a valid hash namespace ([A-Za-z0-9_]).
func namespace(prefix string) string {
	return regexp.MustCompile(`[^A-Za-z0-9_]`).ReplaceAllString(prefix, "_")
}

func checkNil(t *testing.T, what string, err error) {
	t.Helper()
	if !errors.Is(err, redis.Nil) {
		t.Fatalf("%s: want redis.Nil, got %v", what, err)
	}
}

// Every byte value plus CRLF and RESP-looking content.
func binaryValue() []byte {
	b := make([]byte, 0, 300)
	for i := 0; i < 256; i++ {
		b = append(b, byte(i))
	}
	return append(b, []byte("\r\n*1\r\n$4\r\nPING\r\n")...)
}

func TestStrings(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, rdb *redis.Client, p string) {
		expect(t, "PING", rdb.Ping(ctx), "PONG")

		expect(t, "SET", rdb.Set(ctx, p+"k", "v", 0), "OK")
		expect(t, "GET", rdb.Get(ctx, p+"k"), "v")
		checkNil(t, "GET missing", rdb.Get(ctx, p+"missing").Err())

		expect(t, "SET empty", rdb.Set(ctx, p+"empty", "", 0), "OK")
		expect(t, "GET empty", rdb.Get(ctx, p+"empty"), "")

		expect(t, "SET unicode key", rdb.Set(ctx, p+"ключ/🔑", "значение", 0), "OK")
		expect(t, "GET unicode key", rdb.Get(ctx, p+"ключ/🔑"), "значение")

		bin := binaryValue()
		expect(t, "SET binary", rdb.Set(ctx, p+"bin", bin, 0), "OK")
		got, err := rdb.Get(ctx, p+"bin").Bytes()
		if err != nil || !bytes.Equal(got, bin) {
			t.Fatalf("GET binary: %v, equal=%v", err, bytes.Equal(got, bin))
		}

		expect(t, "EXISTS", rdb.Exists(ctx, p+"k"), int64(1))
		expect(t, "DEL", rdb.Del(ctx, p+"k", p+"empty", p+"missing"), int64(2))
		expect(t, "EXISTS after DEL", rdb.Exists(ctx, p+"k"), int64(0))
		checkNil(t, "GET after DEL", rdb.Get(ctx, p+"k").Err())
	})
}

func TestTTL(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, rdb *redis.Client, p string) {
		expect(t, "SET EX", rdb.Set(ctx, p+"ttl", "v", 100*time.Second), "OK")
		expect(t, "EXPIRE", rdb.Expire(ctx, p+"ttl", 50*time.Second), true)
		// TTL reads the DB only, so in async mode wait for the write to land.
		waitFor(t, "TTL after EXPIRE", func() bool {
			ttl := rdb.TTL(ctx, p+"ttl").Val()
			return ttl > 45*time.Second && ttl <= 50*time.Second
		})

		// go-redis sends PX for sub-second precision; blobasaur rounds up to
		// whole seconds instead of expiring the key at once.
		expect(t, "SET PX 500", rdb.Set(ctx, p+"px", "v", 500*time.Millisecond), "OK")
		expect(t, "GET right after PX 500", rdb.Get(ctx, p+"px"), "v")

		if err := rdb.Do(ctx, "SET", p+"ex0", "v", "EX", "0").Err(); err == nil {
			t.Fatal("SET EX 0 should be rejected")
		}
	})
}

func TestHashes(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, rdb *redis.Client, p string) {
		ns := namespace(p)
		expect(t, "HSET new", rdb.HSet(ctx, ns, p+"f1", "v1"), int64(1))
		expect(t, "HSET overwrite", rdb.HSet(ctx, ns, p+"f1", "v2"), int64(0))
		expect(t, "HGET", rdb.HGet(ctx, ns, p+"f1"), "v2")
		checkNil(t, "HGET missing", rdb.HGet(ctx, ns, p+"nope").Err())
		expect(t, "HEXISTS", rdb.HExists(ctx, ns, p+"f1"), true)

		bin := binaryValue()
		expect(t, "HSET binary", rdb.HSet(ctx, ns, p+"fbin", bin), int64(1))
		got, err := rdb.HGet(ctx, ns, p+"fbin").Bytes()
		if err != nil || !bytes.Equal(got, bin) {
			t.Fatalf("HGET binary: %v, equal=%v", err, bytes.Equal(got, bin))
		}

		expect(t, "HDEL", rdb.HDel(ctx, ns, p+"f1"), int64(1))
		checkNil(t, "HGET after HDEL", rdb.HGet(ctx, ns, p+"f1").Err())
		expect(t, "HEXISTS after HDEL", rdb.HExists(ctx, ns, p+"f1"), false)
		expect(t, "HDEL again", rdb.HDel(ctx, ns, p+"f1"), int64(0))
	})
}

// Writes acknowledged in async mode are visible to the very next command,
// including DEL: a queued SET must not come back after it.
func TestReadAfterWrite(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, rdb *redis.Client, p string) {
		for i := 0; i < 50; i++ {
			k := fmt.Sprintf("%sraw:%d", p, i)
			expect(t, "SET", rdb.Set(ctx, k, "v", 0), "OK")
			expect(t, "EXISTS right after SET", rdb.Exists(ctx, k), int64(1))
			expect(t, "DEL right after SET", rdb.Del(ctx, k), int64(1))
		}
		ns := namespace(p)
		for i := 0; i < 50; i++ {
			f := fmt.Sprintf("%sf:%d", p, i)
			expect(t, "HSET", rdb.HSet(ctx, ns, f, "v"), int64(1))
			expect(t, "HEXISTS right after HSET", rdb.HExists(ctx, ns, f), true)
			expect(t, "HDEL right after HSET", rdb.HDel(ctx, ns, f), int64(1))
		}
		// Let every queued op commit, then nothing may have come back.
		time.Sleep(200 * time.Millisecond)
		for i := 0; i < 50; i++ {
			k := fmt.Sprintf("%sraw:%d", p, i)
			expect(t, "deleted key stays deleted", rdb.Exists(ctx, k), int64(0))
			f := fmt.Sprintf("%sf:%d", p, i)
			expect(t, "deleted field stays deleted", rdb.HExists(ctx, ns, f), false)
		}
	})
}

// A command error must leave the pooled connection usable.
func TestCommandErrorsKeepConnection(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, rdb *redis.Client, p string) {
		if err := rdb.Do(ctx, "NOSUCHCMD", "x").Err(); err == nil {
			t.Fatal("unknown command should fail")
		}
		if err := rdb.Do(ctx, "SET", p+"k", "v", "NX").Err(); err == nil {
			t.Fatal("unsupported SET option should fail")
		}
		expect(t, "PING after errors", rdb.Ping(ctx), "PONG")
	})
}

func TestPipeline(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, rdb *redis.Client, p string) {
		pipe := rdb.Pipeline()
		for i := 0; i < 1000; i++ {
			pipe.Set(ctx, fmt.Sprintf("%spipe:%d", p, i), fmt.Sprintf("val:%d", i), 0)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			t.Fatalf("pipelined SETs: %v", err)
		}
		gets := make([]*redis.StringCmd, 1000)
		for i := range gets {
			gets[i] = pipe.Get(ctx, fmt.Sprintf("%spipe:%d", p, i))
		}
		if _, err := pipe.Exec(ctx); err != nil {
			t.Fatalf("pipelined GETs: %v", err)
		}
		for i, g := range gets {
			expect(t, "pipelined GET", g, fmt.Sprintf("val:%d", i))
		}
	})
}

// Concurrent clients: every acknowledged write must be readable. This caught
// sync writes acked as OK but lost under SQLite lock contention.
func TestConcurrentClients(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, rdb *redis.Client, p string) {
		var wg sync.WaitGroup
		errs := make(chan string, 50*100*3)
		for w := 0; w < 50; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := 0; i < 100; i++ {
					k := fmt.Sprintf("%sc:%d:%d", p, w, i)
					if err := rdb.Set(ctx, k, k, 0).Err(); err != nil {
						errs <- "SET " + k + ": " + err.Error()
						continue
					}
					if v, err := rdb.Get(ctx, k).Result(); err != nil || v != k {
						errs <- fmt.Sprintf("GET %s: %q %v", k, v, err)
					}
					if err := rdb.HSet(ctx, "concurrent", k, k).Err(); err != nil {
						errs <- "HSET " + k + ": " + err.Error()
					}
				}
			}(w)
		}
		wg.Wait()
		close(errs)
		var failures []string
		for e := range errs {
			failures = append(failures, e)
		}
		if len(failures) > 0 {
			t.Fatalf("%d failures, e.g. %q", len(failures), failures[:min(4, len(failures))])
		}
	})
}

func TestLargeValues(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, rdb *redis.Client, p string) {
		for _, mb := range []int{1, 2, 10, 50} {
			big := bytes.Repeat([]byte{byte(mb)}, mb*1024*1024)
			if err := rdb.Set(ctx, p+"big", big, 0).Err(); err != nil {
				t.Fatalf("SET %d MB: %v", mb, err)
			}
			got, err := rdb.Get(ctx, p+"big").Bytes()
			if err != nil || !bytes.Equal(got, big) {
				t.Fatalf("GET %d MB: err=%v len=%d", mb, err, len(got))
			}
		}
		expect(t, "PING after large values", rdb.Ping(ctx), "PONG")
	})
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: condition not met within 2s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

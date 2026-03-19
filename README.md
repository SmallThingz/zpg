# zpg

Dependency-free Postgres client primitives for Zig (`std.Io`-native, TLS, compile-time prepared queries, pipelining).

## Quick start

This is a library. For a tiny runnable client, see `examples/smoke.zig`.

```zig
const std = @import("std");
const zpg = @import("zpg");

const Db = zpg.Statements(.{
    .health = zpg.statement(
        "health",
        "select 'ok' as status",
        struct {},
        struct { status: []const u8 },
    ),
});
```

## Examples

- Build the smoke client: `zig build example-smoke`
- Run tests: `zig build test`
- Run the full benchmark suite: `zig build benchmark`
- Run only the latency suite: `zig build benchmark-latency`
- Run only the throughput suite: `zig build benchmark-throughput`
- Override benchmark settings: `zig build benchmark -- --mode throughput --throughput-workers 8 --throughput-per-worker 20000 --throughput-pipeline-depth 256`

Latency runs the one-request-at-a-time prepared path. Throughput runs the prepared pipeline path by default.

The benchmark suite starts a temporary local PostgreSQL instance and compares:

- `zpg` prepared mode
- `zpg` prepared-pipeline mode
- a C++ `libpq` pipeline-mode shim

Requirements for benchmarking:

- `/usr/bin/initdb`
- `/usr/bin/pg_ctl`
- `libpq` headers and library available to the Zig C++ build

## Package surface

- `zpg.Config.parseUri(...)`
- `zpg.Conn.connect(...)`
- `zpg.statement("short_name", "...", Args, Row)`
- `zpg.Statements(.{ .name = zpg.statement(...), ... })`
- `Registry.connect(...)`
- `Registry.prepare(conn)`
- `zpg.Pool.init(...)`
- `zpg.Pool.initUri(...)`
- `conn.pipeline()`
- `Registry.stmt.name.query(conn, allocator, args)`
- `Registry.stmt.name.exec(conn, allocator, args)`
- `Registry.stmt.name.queue(&pipeline, args)`
- `zpg.Date`, `zpg.Time`, `zpg.Timestamp`

## Current features

- startup + authentication
- PostgreSQL SSL negotiation backed by Zig std TLS
- cleartext, MD5, and SCRAM-SHA-256 auth
- compile-time prepared query execution
- pipelined prepared execution on one connection
- fixed-size lazy connection pool
- text and binary result decoding
- typed decode coverage for UUID plus common temporal types (`date`, `time`, `timestamp`, `timestamptz`)
- pool only reuses connections that are healthy and back in idle transaction state

## Constraints

- user queries are compile-time only
- all user-facing execution goes through prepared statements
- runtime SQL strings are not a supported public API

## Current tradeoffs

- connections left inside a transaction are discarded on release instead of being reused
- `connect_timeout` is parsed from the URI but currently ignored on Zig `0.16.0-dev`, because `std.Io.net` still panics when a TCP connect timeout is passed through
- the Docker TLS integration test is present, but currently auto-skips on this Zig toolchain when std TLS fails PostgreSQL interop with `TlsUnexpectedMessage` / `EndOfStream`

## Current non-goals

- client certificates

Zig std currently exposes server-side TLS verification for `std.crypto.tls.Client`, but not a public API for supplying a client certificate/private key pair, so mTLS client auth is not implemented in `zpg` yet.

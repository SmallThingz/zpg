# zpg

Dependency-free Postgres client primitives for Zig (`std.Io`-native, simple-query focused).

## Quick start

This is a library. For a tiny runnable client, see `examples/smoke.zig`.

## Examples

- Build the smoke client: `zig build example-smoke`
- Run tests: `zig build test`
- Run the local benchmark: `zig build benchmark -- [workers] [per_worker] [pool] [sql]`

## Package surface

- `zpg.Config.parseUri(...)`
- `zpg.Conn.connect(...)`
- `zpg.Pool.init(...)`
- `zpg.Pool.initUri(...)`
- `conn.query(...)` / `pool.query(...)`
- `conn.exec(...)` / `pool.exec(...)`

## Current features

- startup + authentication
- cleartext, MD5, and SCRAM-SHA-256 auth
- simple query protocol
- fixed-size lazy connection pool
- row description + text row decoding
- pool only reuses connections that are healthy and back in idle transaction state

## Current tradeoffs

- `sslmode` must be `disable`
- `connect_timeout` is parsed but not yet enforced
- connections left inside a transaction are discarded on release instead of being reused

## Current non-goals

- TLS
- prepared statements / extended protocol
- binary result decoding

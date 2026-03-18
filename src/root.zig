//! zpg: dependency-free Postgres client primitives for Zig.
//!
//! ## Quick start
//!
//! This is a library. For a tiny runnable client, see `examples/smoke.zig`.
//!
//! ## Package surface
//!
//! - `zpg.Config.parseUri(...)` parses `postgres://...` / `postgresql://...` URLs
//! - `zpg.Conn.connect(...)` opens one connection
//! - `zpg.Pool.init(...)` and `zpg.Pool.initUri(...)` create a lazy fixed-size pool
//! - `conn.query(...)` / `pool.query(...)` run SQL and return a `zpg.Result`
//! - `conn.exec(...)` / `pool.exec(...)` run SQL and return the command tag
//! - `conn.queryValues(...)`, `conn.execValues(...)`, and `conn.prepare(...)` expose the extended protocol
//! - `conn.pipeline(...)` exposes pipelined simple or extended execution on one connection
//!
//! ## Supported today
//!
//! - PostgreSQL SSL negotiation backed by Zig std TLS
//! - startup + authentication
//! - cleartext, MD5, and SCRAM-SHA-256 auth
//! - simple query protocol
//! - extended protocol with prepared statements
//! - pipelined execution on a single connection
//! - fixed-size lazy connection pool
//! - text and binary result decoding
//!
//! ## Current tradeoffs
//!
//! - `connect_timeout` is parsed from the URI but currently ignored on Zig
//!   `0.16.0-dev`, because `std.Io.net` still panics when given a TCP connect
//!   timeout
//! - the Docker TLS integration test currently auto-skips on toolchains where
//!   Zig std TLS fails PostgreSQL interop with `TlsUnexpectedMessage` /
//!   `EndOfStream`
//!
//! ## Current non-goals
//!
//! - client certificates
//!
//! Zig std currently exposes server-side TLS verification for
//! `std.crypto.tls.Client`, but not a public API for supplying a client
//! certificate/private key pair, so mTLS client auth is not implemented yet.
const std = @import("std");

pub const Config = @import("pg/config.zig").Config;
pub const SslMode = Config.SslMode;
pub const Conn = @import("pg/conn.zig").Conn;
pub const Result = @import("pg/conn.zig").Result;
pub const Row = @import("pg/conn.zig").Row;
pub const Column = @import("pg/conn.zig").Column;
pub const Value = @import("pg/conn.zig").Value;
pub const QueryOptions = @import("pg/conn.zig").QueryOptions;
pub const QueryProtocol = @import("pg/conn.zig").QueryProtocol;
pub const CompiledQuery = @import("pg/conn.zig").CompiledQuery;
pub const CompiledResult = @import("pg/conn.zig").CompiledResult;
pub const Pipeline = @import("pg/conn.zig").Pipeline;
pub const Statement = @import("pg/conn.zig").Statement;
pub const Pool = @import("pg/pool.zig").Pool;
pub const Stats = @import("pg/pool.zig").Stats;
pub const ErrorResponse = @import("pg/proto.zig").ErrorResponse;
pub const FormatCode = @import("pg/proto.zig").FormatCode;

test {
    _ = std.testing.refAllDecls(@This());
}

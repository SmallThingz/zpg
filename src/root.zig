//! zpg: dependency-free Postgres client primitives for Zig.
//!
//! ## Quick start
//!
//! This is a library. For a tiny runnable client, see `examples/smoke.zig`.
//!
//! ## Package surface
//!
//! - `zpg.Config.parseUri(...)` parses `postgres://...` / `postgresql://...` URLs
//! - `zpg.statement("short_name", "...", Args, Row)` defines one prepared statement
//! - `zpg.Statements(.{ .name = zpg.statement(...), ... })` defines a compile-time statement registry
//! - `Registry.connect(...)` opens one connection and prepares the registry up front
//! - `zpg.Pool.init(...)` and `zpg.Pool.initUri(...)` create a lazy fixed-size pool
//! - `Registry.stmt.name.query(conn, allocator, args)` executes one prepared statement
//! - `Registry.stmt.name.exec(conn, allocator, args)` executes and returns the command tag
//! - `Registry.stmt.name.queue(&pipeline, args)` queues one prepared statement on a pipeline
//! - `zpg.Date`, `zpg.Time`, and `zpg.Timestamp` decode common PostgreSQL temporal types
//!
//! ## Supported today
//!
//! - PostgreSQL SSL negotiation backed by Zig std TLS
//! - startup + authentication
//! - cleartext, MD5, and SCRAM-SHA-256 auth
//! - compile-time named prepared statements only
//! - pipelined execution on a single connection
//! - parameterized pipelining on a single connection
//! - fixed-size lazy connection pool
//! - text and binary result decoding
//! - typed decoding for UUID, date, time, timestamp, and timestamptz values
//! - compile-time typed row decoding for compiled queries
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

pub const Config = @import("config.zig").Config;
pub const SslMode = Config.SslMode;
pub const Conn = @import("conn.zig").Conn;
pub const Result = @import("conn.zig").Result;
pub const Row = @import("conn.zig").Row;
pub const Column = @import("conn.zig").Column;
pub const Date = @import("conn.zig").Date;
pub const Time = @import("conn.zig").Time;
pub const Timestamp = @import("conn.zig").Timestamp;
pub const statement = @import("conn.zig").statement;
pub const Statements = @import("conn.zig").Statements;
pub const CompiledResult = @import("conn.zig").CompiledResult;
pub const Pipeline = @import("conn.zig").Pipeline;
pub const Pool = @import("pool.zig").Pool;
pub const Stats = @import("pool.zig").Stats;
pub const ErrorResponse = @import("proto.zig").ErrorResponse;
pub const FormatCode = @import("proto.zig").FormatCode;

test {
    _ = std.testing.refAllDecls(@This());
}

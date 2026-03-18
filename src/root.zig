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
//! - `conn.query(...)` / `pool.query(...)` run simple-query SQL and return a `zpg.Result`
//! - `conn.exec(...)` / `pool.exec(...)` run simple-query SQL and return the command tag
//!
//! ## Supported today
//!
//! - startup + authentication
//! - cleartext, MD5, and SCRAM-SHA-256 auth
//! - simple query protocol
//! - fixed-size lazy connection pool
//! - row/column metadata plus text result values
//!
//! ## Current non-goals
//!
//! - TLS
//! - prepared statements / extended protocol
//! - binary result decoding
const std = @import("std");

pub const Config = @import("pg/config.zig").Config;
pub const Conn = @import("pg/conn.zig").Conn;
pub const Result = @import("pg/conn.zig").Result;
pub const Row = @import("pg/conn.zig").Row;
pub const Column = @import("pg/conn.zig").Column;
pub const Pool = @import("pg/pool.zig").Pool;
pub const Stats = @import("pg/pool.zig").Stats;
pub const ErrorResponse = @import("pg/proto.zig").ErrorResponse;

test {
    _ = std.testing.refAllDecls(@This());
}

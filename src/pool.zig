const std = @import("std");
const Config = @import("config.zig").Config;
const Conn = @import("conn.zig").Conn;
const Result = @import("conn.zig").Result;
const QueryOptions = @import("conn.zig").QueryOptions;
const Value = @import("conn.zig").Value;

pub const Stats = struct {
    max_size: usize,
    open: usize,
    idle: usize,
    in_use: usize,
};

pub const Pool = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    config: Config,
    idle: std.ArrayList(*Conn) = .empty,
    max_size: usize,
    open_count: usize = 0,
    closed: bool = false,
    mutex: std.Io.Mutex = .init,
    condition: std.Io.Condition = .init,

    pub fn init(allocator: std.mem.Allocator, io: std.Io, config: Config, max_size: usize) Pool {
        return .{
            .allocator = allocator,
            .io = io,
            .config = config,
            .max_size = max_size,
        };
    }

    pub fn initUri(allocator: std.mem.Allocator, io: std.Io, uri: []const u8, max_size: usize) !Pool {
        return .init(allocator, io, try Config.parseUri(allocator, uri), max_size);
    }

    pub fn deinit(pool: *Pool) void {
        pool.mutex.lockUncancelable(pool.io);
        pool.closed = true;
        pool.condition.broadcast(pool.io);
        std.debug.assert(pool.open_count == pool.idle.items.len);
        while (pool.idle.pop()) |conn| {
            pool.open_count -= 1;
            pool.mutex.unlock(pool.io);
            conn.destroy();
            pool.mutex.lockUncancelable(pool.io);
        }
        pool.mutex.unlock(pool.io);
        pool.idle.deinit(pool.allocator);
        pool.config.deinit(pool.allocator);
    }

    pub fn acquire(pool: *Pool) !*Conn {
        while (true) {
            pool.mutex.lockUncancelable(pool.io);
            if (pool.closed) {
                pool.mutex.unlock(pool.io);
                return error.Closed;
            }
            if (pool.idle.pop()) |conn| {
                pool.mutex.unlock(pool.io);
                return conn;
            }
            if (pool.open_count < pool.max_size) {
                pool.open_count += 1;
                pool.mutex.unlock(pool.io);
                return Conn.create(pool.allocator, pool.io, &pool.config) catch |err| {
                    pool.mutex.lockUncancelable(pool.io);
                    pool.open_count -= 1;
                    pool.condition.signal(pool.io);
                    pool.mutex.unlock(pool.io);
                    return err;
                };
            }
            pool.condition.waitUncancelable(pool.io, &pool.mutex);
            pool.mutex.unlock(pool.io);
        }
    }

    pub fn release(pool: *Pool, conn: *Conn) void {
        if (!canReuseConnection(pool, conn)) {
            pool.mutex.lockUncancelable(pool.io);
            pool.open_count -= 1;
            pool.condition.signal(pool.io);
            pool.mutex.unlock(pool.io);
            conn.destroy();
            return;
        }
        pool.mutex.lockUncancelable(pool.io);
        pool.idle.append(pool.allocator, conn) catch {
            pool.open_count -= 1;
            pool.condition.signal(pool.io);
            pool.mutex.unlock(pool.io);
            conn.destroy();
            return;
        };
        pool.condition.signal(pool.io);
        pool.mutex.unlock(pool.io);
    }

    pub fn query(pool: *Pool, allocator: std.mem.Allocator, sql: []const u8) !Result {
        const conn = try pool.acquire();
        defer pool.release(conn);
        return conn.query(allocator, sql);
    }

    pub fn queryOpts(pool: *Pool, allocator: std.mem.Allocator, sql: []const u8, opts: QueryOptions) !Result {
        const conn = try pool.acquire();
        defer pool.release(conn);
        return conn.queryOpts(allocator, sql, opts);
    }

    pub fn queryValues(pool: *Pool, allocator: std.mem.Allocator, sql: []const u8, values: []const Value, opts: QueryOptions) !Result {
        const conn = try pool.acquire();
        defer pool.release(conn);
        return conn.queryValues(allocator, sql, values, opts);
    }

    pub fn exec(pool: *Pool, allocator: std.mem.Allocator, sql: []const u8) ![]const u8 {
        const conn = try pool.acquire();
        defer pool.release(conn);
        return conn.exec(allocator, sql);
    }

    pub fn execOpts(pool: *Pool, allocator: std.mem.Allocator, sql: []const u8, opts: QueryOptions) ![]const u8 {
        const conn = try pool.acquire();
        defer pool.release(conn);
        return conn.execOpts(allocator, sql, opts);
    }

    pub fn execValues(pool: *Pool, allocator: std.mem.Allocator, sql: []const u8, values: []const Value, opts: QueryOptions) ![]const u8 {
        const conn = try pool.acquire();
        defer pool.release(conn);
        return conn.execValues(allocator, sql, values, opts);
    }

    pub fn stats(pool: *Pool) Stats {
        pool.mutex.lockUncancelable(pool.io);
        defer pool.mutex.unlock(pool.io);
        return .{
            .max_size = pool.max_size,
            .open = pool.open_count,
            .idle = pool.idle.items.len,
            .in_use = pool.open_count - pool.idle.items.len,
        };
    }
};

fn canReuseConnection(pool: *const Pool, conn: *const Conn) bool {
    return !pool.closed and conn.healthy and conn.tx_status == 'I';
}

test "pool reuse guard rejects transaction states" {
    var dummy_pool = Pool.init(std.testing.allocator, std.testing.io, .{
        .host = try std.testing.allocator.dupe(u8, "localhost"),
        .port = 5432,
        .user = try std.testing.allocator.dupe(u8, "u"),
        .password = null,
        .database = try std.testing.allocator.dupe(u8, "d"),
        .application_name = try std.testing.allocator.dupe(u8, "a"),
        .ssl_mode = .disable,
        .ssl_root_cert = null,
        .connect_timeout_ms = 0,
        .max_message_len = 1024,
    }, 1);
    defer dummy_pool.config.deinit(std.testing.allocator);

    var conn: Conn = undefined;
    conn.healthy = true;
    conn.tx_status = 'I';
    try std.testing.expect(canReuseConnection(&dummy_pool, &conn));
    conn.tx_status = 'T';
    try std.testing.expect(!canReuseConnection(&dummy_pool, &conn));
    conn.tx_status = 'E';
    try std.testing.expect(!canReuseConnection(&dummy_pool, &conn));
    conn.tx_status = 'I';
    conn.healthy = false;
    try std.testing.expect(!canReuseConnection(&dummy_pool, &conn));
}

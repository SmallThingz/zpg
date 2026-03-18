const std = @import("std");

pub const Config = struct {
    pub const SslMode = enum {
        disable,
        prefer,
        require,
        verify_ca,
        verify_full,
    };

    host: []u8,
    port: u16,
    user: []u8,
    password: ?[]u8,
    database: []u8,
    application_name: []u8,
    ssl_mode: SslMode,
    ssl_root_cert: ?[]u8,
    connect_timeout_ms: u32,
    max_message_len: u32,

    pub fn parseUri(allocator: std.mem.Allocator, uri_text: []const u8) !Config {
        var scratch = std.heap.ArenaAllocator.init(allocator);
        defer scratch.deinit();
        const temp = scratch.allocator();
        const uri = try std.Uri.parse(uri_text);
        if (!std.mem.eql(u8, uri.scheme, "postgres") and !std.mem.eql(u8, uri.scheme, "postgresql")) {
            return error.InvalidScheme;
        }
        var host_buffer: [std.Io.net.HostName.max_len]u8 = undefined;
        const host_name = try uri.getHost(&host_buffer);
        const user_raw = if (uri.user) |user| try user.toRawMaybeAlloc(temp) else return error.MissingUser;
        const password_raw = if (uri.password) |password| try password.toRawMaybeAlloc(temp) else null;
        const database_raw = if (!uri.path.isEmpty()) blk: {
            const raw = try uri.path.toRawMaybeAlloc(temp);
            if (raw.len == 0 or raw[0] != '/') return error.MissingDatabase;
            if (raw.len == 1) break :blk user_raw;
            break :blk raw[1..];
        } else user_raw;

        var connect_timeout_ms: u32 = 5_000;
        var application_name: []const u8 = "zpg";
        var max_message_len: u32 = 16 * 1024 * 1024;
        var ssl_mode: SslMode = .prefer;
        var ssl_root_cert: ?[]const u8 = null;
        if (uri.query) |query| {
            const raw_query = try query.toRawMaybeAlloc(temp);
            var it = std.mem.splitScalar(u8, raw_query, '&');
            while (it.next()) |pair| {
                if (pair.len == 0) continue;
                const eq = std.mem.indexOfScalar(u8, pair, '=') orelse pair.len;
                const key = pair[0..eq];
                const value = if (eq < pair.len) pair[eq + 1 ..] else "";
                if (std.mem.eql(u8, key, "connect_timeout")) {
                    connect_timeout_ms = @as(u32, @intCast((std.fmt.parseInt(u32, value, 10) catch 5) * 1000));
                } else if (std.mem.eql(u8, key, "application_name") and value.len != 0) {
                    application_name = value;
                } else if (std.mem.eql(u8, key, "max_message_len") and value.len != 0) {
                    max_message_len = std.fmt.parseInt(u32, value, 10) catch max_message_len;
                } else if (std.mem.eql(u8, key, "sslmode")) {
                    ssl_mode = parseSslMode(value) orelse return error.InvalidSslMode;
                } else if (std.mem.eql(u8, key, "sslrootcert") and value.len != 0) {
                    ssl_root_cert = value;
                }
            }
        }
        return .{
            .host = try allocator.dupe(u8, host_name.bytes),
            .port = uri.port orelse 5432,
            .user = try allocator.dupe(u8, user_raw),
            .password = if (password_raw) |value| try allocator.dupe(u8, value) else null,
            .database = try allocator.dupe(u8, database_raw),
            .application_name = try allocator.dupe(u8, application_name),
            .ssl_mode = ssl_mode,
            .ssl_root_cert = if (ssl_root_cert) |path| try allocator.dupe(u8, path) else null,
            .connect_timeout_ms = connect_timeout_ms,
            .max_message_len = max_message_len,
        };
    }

    pub fn deinit(config: *Config, allocator: std.mem.Allocator) void {
        allocator.free(config.host);
        allocator.free(config.user);
        if (config.password) |password| allocator.free(password);
        allocator.free(config.database);
        allocator.free(config.application_name);
        if (config.ssl_root_cert) |path| allocator.free(path);
        config.* = undefined;
    }
};

fn parseSslMode(value: []const u8) ?Config.SslMode {
    if (std.mem.eql(u8, value, "disable")) return .disable;
    if (std.mem.eql(u8, value, "prefer")) return .prefer;
    if (std.mem.eql(u8, value, "require")) return .require;
    if (std.mem.eql(u8, value, "verify-ca")) return .verify_ca;
    if (std.mem.eql(u8, value, "verify-full")) return .verify_full;
    return null;
}

test "parse postgres uri" {
    var cfg = try Config.parseUri(std.testing.allocator, "postgres://user:pass@localhost:5544/dbname?application_name=tester&connect_timeout=9");
    defer cfg.deinit(std.testing.allocator);

    try std.testing.expectEqualStrings("localhost", cfg.host);
    try std.testing.expectEqual(@as(u16, 5544), cfg.port);
    try std.testing.expectEqualStrings("user", cfg.user);
    try std.testing.expectEqualStrings("pass", cfg.password.?);
    try std.testing.expectEqualStrings("dbname", cfg.database);
    try std.testing.expectEqualStrings("tester", cfg.application_name);
    try std.testing.expectEqual(Config.SslMode.prefer, cfg.ssl_mode);
    try std.testing.expectEqual(@as(u32, 9000), cfg.connect_timeout_ms);
}

test "parse postgres uri with escaped fields" {
    var cfg = try Config.parseUri(std.testing.allocator, "postgres://user%2Cname:pass%3Dword@localhost/db%2Fmain?application_name=app%20name");
    defer cfg.deinit(std.testing.allocator);

    try std.testing.expectEqualStrings("user,name", cfg.user);
    try std.testing.expectEqualStrings("pass=word", cfg.password.?);
    try std.testing.expectEqualStrings("db/main", cfg.database);
    try std.testing.expectEqualStrings("app name", cfg.application_name);
}

test "parse postgres uri root path falls back to user database" {
    var cfg = try Config.parseUri(std.testing.allocator, "postgres://alice@localhost/");
    defer cfg.deinit(std.testing.allocator);

    try std.testing.expectEqualStrings("alice", cfg.database);
}

test "parse postgres uri accepts ssl modes" {
    var cfg = try Config.parseUri(std.testing.allocator, "postgres://alice@localhost/db?sslmode=verify-full&sslrootcert=%2Ftmp%2Fca.pem");
    defer cfg.deinit(std.testing.allocator);

    try std.testing.expectEqual(Config.SslMode.verify_full, cfg.ssl_mode);
    try std.testing.expectEqualStrings("/tmp/ca.pem", cfg.ssl_root_cert.?);
}

test "parse postgres uri rejects invalid ssl mode" {
    try std.testing.expectError(error.InvalidSslMode, Config.parseUri(std.testing.allocator, "postgres://alice@localhost/db?sslmode=allow"));
}

test "parse postgres uri applies defaults and ignores bad numeric overrides" {
    var cfg = try Config.parseUri(std.testing.allocator, "postgres://alice@localhost/db?connect_timeout=bad&max_message_len=nope");
    defer cfg.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 5432), cfg.port);
    try std.testing.expectEqualStrings("zpg", cfg.application_name);
    try std.testing.expectEqual(@as(u32, 5_000), cfg.connect_timeout_ms);
    try std.testing.expectEqual(@as(u32, 16 * 1024 * 1024), cfg.max_message_len);
}

test "parse postgres uri rejects bad scheme and missing user" {
    try std.testing.expectError(error.InvalidScheme, Config.parseUri(std.testing.allocator, "mysql://alice@localhost/db"));
    try std.testing.expectError(error.MissingUser, Config.parseUri(std.testing.allocator, "postgres://localhost/db"));
}

test "parse postgres uri decodes hostless unix style fields and blank query pairs" {
    var cfg = try Config.parseUri(std.testing.allocator, "postgresql://bob:pw@localhost/mydb?&application_name=x&&sslrootcert=%2Fca.pem");
    defer cfg.deinit(std.testing.allocator);

    try std.testing.expectEqualStrings("bob", cfg.user);
    try std.testing.expectEqualStrings("pw", cfg.password.?);
    try std.testing.expectEqualStrings("mydb", cfg.database);
    try std.testing.expectEqualStrings("x", cfg.application_name);
    try std.testing.expectEqualStrings("/ca.pem", cfg.ssl_root_cert.?);
}

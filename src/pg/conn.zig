const std = @import("std");
const Config = @import("config.zig").Config;
const proto = @import("proto.zig");
const scram = @import("scram.zig");

pub const Column = struct {
    name: []const u8,
    type_oid: u32,
    format: u16,
};

pub const Row = struct {
    values: []const ?[]const u8,

    pub fn get(row: Row, index: usize) ?[]const u8 {
        return row.values[index];
    }
};

pub const Result = struct {
    arena: std.heap.ArenaAllocator,
    columns: []const Column = &.{},
    rows: []const Row = &.{},
    command_tag: []const u8 = "",

    pub fn columnIndex(result: Result, name: []const u8) ?usize {
        for (result.columns, 0..) |column, i| {
            if (std.mem.eql(u8, column.name, name)) return i;
        }
        return null;
    }

    pub fn deinit(result: *Result) void {
        result.arena.deinit();
        result.* = undefined;
    }
};

pub const Conn = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    config: *const Config,
    stream: std.Io.net.Stream,
    reader: std.Io.net.Stream.Reader,
    writer: std.Io.net.Stream.Writer,
    read_buffer: [16 * 1024]u8,
    write_buffer: [8 * 1024]u8,
    message_buffer: []u8 = &.{},
    backend_pid: i32 = 0,
    backend_key: i32 = 0,
    tx_status: u8 = 'I',
    healthy: bool = true,
    last_error: ?proto.ErrorResponse = null,

    pub fn connect(allocator: std.mem.Allocator, io: std.Io, config: *const Config) !*Conn {
        return create(allocator, io, config);
    }

    pub fn create(allocator: std.mem.Allocator, io: std.Io, config: *const Config) !*Conn {
        const conn = try allocator.create(Conn);
        errdefer allocator.destroy(conn);
        conn.* = .{
            .allocator = allocator,
            .io = io,
            .config = config,
            .stream = try connectStream(config, io),
            .reader = undefined,
            .writer = undefined,
            .read_buffer = undefined,
            .write_buffer = undefined,
        };
        conn.reader = conn.stream.reader(io, &conn.read_buffer);
        conn.writer = conn.stream.writer(io, &conn.write_buffer);
        errdefer conn.close();
        try conn.startup();
        return conn;
    }

    pub fn destroy(conn: *Conn) void {
        const allocator = conn.allocator;
        conn.close();
        allocator.destroy(conn);
    }

    pub fn close(conn: *Conn) void {
        if (conn.last_error) |*err| err.deinit(conn.allocator);
        proto.writeTerminate(&conn.writer.interface) catch {};
        conn.stream.close(conn.io);
        if (conn.message_buffer.len != 0) conn.allocator.free(conn.message_buffer);
        conn.* = undefined;
    }

    pub fn queryAlloc(conn: *Conn, allocator: std.mem.Allocator, sql: []const u8) !Result {
        conn.clearLastError();
        try proto.writeQuery(&conn.writer.interface, sql);

        var result = Result{ .arena = std.heap.ArenaAllocator.init(allocator) };
        errdefer result.deinit();
        const arena = result.arena.allocator();
        var columns: std.ArrayList(Column) = .empty;
        defer columns.deinit(arena);
        var rows: std.ArrayList(Row) = .empty;
        defer rows.deinit(arena);
        var command_tag: []const u8 = "";
        var failed = false;

        while (true) {
            const msg = try conn.readMessage();
            switch (msg.tag) {
                'T' => try parseRowDescription(arena, &columns, msg.payload),
                'D' => try rows.append(arena, try parseDataRow(arena, msg.payload)),
                'C' => command_tag = try arena.dupe(u8, try parseCommandComplete(msg.payload)),
                'Z' => {
                    if (msg.payload.len != 1) return conn.protocolError();
                    conn.tx_status = msg.payload[0];
                    if (conn.tx_status != 'I') conn.healthy = false;
                    result.columns = try columns.toOwnedSlice(arena);
                    result.rows = try rows.toOwnedSlice(arena);
                    result.command_tag = command_tag;
                    if (failed) return error.PgServer;
                    return result;
                },
                'E' => {
                    failed = true;
                    conn.setLastError(msg.payload) catch return conn.protocolError();
                },
                'S', 'N' => {},
                else => return conn.protocolError(),
            }
        }
    }

    pub fn exec(conn: *Conn, allocator: std.mem.Allocator, sql: []const u8) ![]const u8 {
        conn.clearLastError();
        try proto.writeQuery(&conn.writer.interface, sql);
        var command_tag: ?[]const u8 = null;
        var failed = false;
        while (true) {
            const msg = try conn.readMessage();
            switch (msg.tag) {
                'C' => command_tag = try allocator.dupe(u8, try parseCommandComplete(msg.payload)),
                'Z' => {
                    if (msg.payload.len != 1) return conn.protocolError();
                    conn.tx_status = msg.payload[0];
                    if (conn.tx_status != 'I') conn.healthy = false;
                    if (failed) return error.PgServer;
                    return command_tag orelse try allocator.dupe(u8, "");
                },
                'E' => {
                    failed = true;
                    conn.setLastError(msg.payload) catch return conn.protocolError();
                },
                'T', 'D', 'S', 'N', 'I' => {},
                else => return conn.protocolError(),
            }
        }
    }

    pub fn lastErrorMessage(conn: *const Conn) ?[]const u8 {
        return if (conn.last_error) |err| err.message else null;
    }

    pub fn lastError(conn: *const Conn) ?*const proto.ErrorResponse {
        return if (conn.last_error) |*err| err else null;
    }

    pub fn query(conn: *Conn, allocator: std.mem.Allocator, sql: []const u8) !Result {
        return conn.queryAlloc(allocator, sql);
    }

    fn startup(conn: *Conn) !void {
        try proto.writeStartup(&conn.writer.interface, conn.config.user, conn.config.database, conn.config.application_name);
        while (true) {
            const msg = try conn.readMessage();
            switch (msg.tag) {
                'R' => try conn.handleAuth(msg.payload),
                'S', 'N' => {},
                'K' => {
                    if (msg.payload.len != 8) return conn.protocolError();
                    conn.backend_pid = std.mem.readInt(i32, msg.payload[0..4], .big);
                    conn.backend_key = std.mem.readInt(i32, msg.payload[4..8], .big);
                },
                'Z' => {
                    if (msg.payload.len != 1) return conn.protocolError();
                    conn.tx_status = msg.payload[0];
                    return;
                },
                'E' => {
                    try conn.setLastError(msg.payload);
                    return error.PgServer;
                },
                else => return conn.protocolError(),
            }
        }
    }

    fn handleAuth(conn: *Conn, payload: []const u8) !void {
        switch (try proto.decodeAuthentication(payload)) {
            .ok => {},
            .cleartext => try proto.writePassword(&conn.writer.interface, conn.config.password orelse ""),
            .md5 => |salt| {
                const password = conn.config.password orelse "";
                var digest: [16]u8 = undefined;
                var hasher = std.crypto.hash.Md5.init(.{});
                hasher.update(password);
                hasher.update(conn.config.user);
                hasher.final(&digest);
                const hex1 = std.fmt.bytesToHex(digest, .lower);
                hasher = std.crypto.hash.Md5.init(.{});
                hasher.update(&hex1);
                hasher.update(&salt);
                hasher.final(&digest);
                var text: [35]u8 = undefined;
                const md5_text = try std.fmt.bufPrint(&text, "md5{s}", .{&std.fmt.bytesToHex(digest, .lower)});
                try proto.writePassword(&conn.writer.interface, md5_text);
            },
            .sasl => |mechanisms| {
                if (!mechanisms.has_scram_sha_256) return error.UnsupportedAuthentication;
                var client = try scram.Client.init(conn.io, conn.config.password orelse "");
                try proto.writeSaslInitialResponse(&conn.writer.interface, "SCRAM-SHA-256", client.initialMessage());
                const cont = try conn.readMessage();
                if (cont.tag != 'R') return conn.protocolError();
                const challenge = switch (try proto.decodeAuthentication(cont.payload)) {
                    .sasl_continue => |value| value,
                    else => return conn.protocolError(),
                };
                var response_buffer: [256]u8 = undefined;
                const client_final = try client.serverFirst(challenge, &response_buffer);
                try proto.writeSaslResponse(&conn.writer.interface, client_final);
                const final = try conn.readMessage();
                if (final.tag != 'R') return conn.protocolError();
                switch (try proto.decodeAuthentication(final.payload)) {
                    .ok => {},
                    .sasl_final => |value| try client.verifyServerFinal(value),
                    else => return conn.protocolError(),
                }
            },
            .sasl_continue, .sasl_final => return conn.protocolError(),
        }
    }

    fn clearLastError(conn: *Conn) void {
        if (conn.last_error) |*err| {
            err.deinit(conn.allocator);
            conn.last_error = null;
        }
    }

    fn setLastError(conn: *Conn, payload: []const u8) !void {
        conn.clearLastError();
        conn.last_error = try proto.parseErrorResponse(conn.allocator, payload);
    }

    fn protocolError(conn: *Conn) error{ProtocolViolation} {
        conn.healthy = false;
        return error.ProtocolViolation;
    }

    fn readMessage(conn: *Conn) !proto.MessageView {
        return proto.readMessageInto(conn.allocator, &conn.reader.interface, conn.config.max_message_len, &conn.message_buffer);
    }
};

fn connectStream(config: *const Config, io: std.Io) !std.Io.net.Stream {
    const options: std.Io.net.IpAddress.ConnectOptions = .{
        .mode = .stream,
        .timeout = .none,
    };
    if (std.mem.eql(u8, config.host, "localhost")) {
        const address: std.Io.net.IpAddress = .{ .ip4 = .loopback(config.port) };
        return address.connect(io, options);
    }
    const address = std.Io.net.IpAddress.parse(config.host, config.port) catch {
        const host: std.Io.net.HostName = .{ .bytes = config.host };
        return host.connect(io, config.port, options);
    };
    return address.connect(io, options);
}

fn parseRowDescription(arena: std.mem.Allocator, columns: *std.ArrayList(Column), payload: []const u8) !void {
    var i: usize = 0;
    const count = try readIntAt(u16, payload, &i);
    i += 2;
    try columns.ensureTotalCapacity(arena, count);
    for (0..count) |_| {
        const name = try proto.fieldCString(payload, &i);
        _ = try readIntAt(u32, payload, &i);
        i += 4;
        _ = try readIntAt(u16, payload, &i);
        i += 2;
        const type_oid = try readIntAt(u32, payload, &i);
        i += 4;
        _ = try readIntAt(i16, payload, &i);
        i += 2;
        _ = try readIntAt(i32, payload, &i);
        i += 4;
        const format = try readIntAt(u16, payload, &i);
        i += 2;
        try columns.append(arena, .{
            .name = try arena.dupe(u8, name),
            .type_oid = type_oid,
            .format = format,
        });
    }
}

fn parseDataRow(arena: std.mem.Allocator, payload: []const u8) !Row {
    var i: usize = 0;
    const count = try readIntAt(u16, payload, &i);
    i += 2;
    const values = try arena.alloc(?[]const u8, count);
    for (0..count) |idx| {
        const len = try readIntAt(i32, payload, &i);
        i += 4;
        if (len == -1) {
            values[idx] = null;
            continue;
        }
        if (len < 0) return error.InvalidDataRow;
        if (payload.len - i < @as(usize, @intCast(len))) return error.InvalidDataRow;
        values[idx] = try arena.dupe(u8, payload[i..][0..@as(usize, @intCast(len))]);
        i += @as(usize, @intCast(len));
    }
    return .{ .values = values };
}

fn parseCommandComplete(payload: []const u8) ![]const u8 {
    var i: usize = 0;
    return proto.fieldCString(payload, &i);
}

fn readIntAt(comptime T: type, payload: []const u8, index: *usize) !T {
    if (payload.len - index.* < @sizeOf(T)) return error.InvalidMessage;
    return std.mem.readInt(T, payload[index.*..][0..@sizeOf(T)], .big);
}

test "parse row description rejects truncated payload" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var columns: std.ArrayList(Column) = .empty;
    defer columns.deinit(arena.allocator());
    try std.testing.expectError(error.InvalidField, parseRowDescription(arena.allocator(), &columns, &.{ 0, 1, 'x' }));
}

test "parse data row rejects truncated payload" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    try std.testing.expectError(error.InvalidDataRow, parseDataRow(arena.allocator(), &.{ 0, 1, 0, 0, 0, 3, 'a' }));
}

test "data row parser fuzz stays bounded" {
    var prng = std.Random.DefaultPrng.init(0x5eed1234);
    const random = prng.random();
    for (0..2000) |_| {
        const len = random.intRangeLessThan(usize, 0, 96);
        const payload = try std.testing.allocator.alloc(u8, len);
        defer std.testing.allocator.free(payload);
        random.bytes(payload);
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        _ = parseDataRow(arena.allocator(), payload) catch {};
    }
}

test "result column index lookup" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const columns = try arena.allocator().dupe(Column, &.{
        .{ .name = "id", .type_oid = 23, .format = 0 },
        .{ .name = "name", .type_oid = 25, .format = 0 },
    });
    const result: Result = .{
        .arena = arena,
        .columns = columns,
    };

    try std.testing.expectEqual(@as(?usize, 1), result.columnIndex("name"));
    try std.testing.expectEqual(@as(?usize, null), result.columnIndex("missing"));
}

test "row description parser fuzz stays bounded" {
    var prng = std.Random.DefaultPrng.init(0x5eed5678);
    const random = prng.random();
    for (0..2000) |_| {
        const len = random.intRangeLessThan(usize, 0, 128);
        const payload = try std.testing.allocator.alloc(u8, len);
        defer std.testing.allocator.free(payload);
        random.bytes(payload);
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        var columns: std.ArrayList(Column) = .empty;
        defer columns.deinit(arena.allocator());
        _ = parseRowDescription(arena.allocator(), &columns, payload) catch {};
    }
}

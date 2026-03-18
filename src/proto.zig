const std = @import("std");

pub const MessageView = struct {
    tag: u8,
    payload: []u8,
};

pub const FormatCode = enum(u16) {
    text = 0,
    binary = 1,
};

pub const CloseTarget = enum(u8) {
    statement = 'S',
    portal = 'P',
};

pub const Param = struct {
    type_oid: u32 = 0,
    format: FormatCode = .text,
    value: ?[]const u8 = null,
};

pub const ErrorResponse = struct {
    severity: []u8 = &.{},
    code: []u8 = &.{},
    message: []u8 = &.{},

    pub fn deinit(err: *ErrorResponse, allocator: std.mem.Allocator) void {
        allocator.free(err.severity);
        allocator.free(err.code);
        allocator.free(err.message);
        err.* = undefined;
    }
};

pub fn readMessageInto(allocator: std.mem.Allocator, reader: *std.Io.Reader, max_message_len: u32, buffer: *[]u8) !MessageView {
    const tag = try reader.takeByte();
    const len = try reader.takeInt(u32, .big);
    if (len < 4 or len - 4 > max_message_len) return error.InvalidMessageLength;
    const payload_len = len - 4;
    if (buffer.*.len < payload_len) {
        if (buffer.*.len != 0) allocator.free(buffer.*);
        buffer.* = try allocator.alloc(u8, payload_len);
    }
    try reader.readSliceAll(buffer.*[0..payload_len]);
    return .{ .tag = tag, .payload = buffer.*[0..payload_len] };
}

pub fn appendSslRequest(out: *std.ArrayList(u8), allocator: std.mem.Allocator) !void {
    try appendInt(out, allocator, u32, 8);
    try appendInt(out, allocator, u32, 80877103);
}

pub fn appendStartup(out: *std.ArrayList(u8), allocator: std.mem.Allocator, user: []const u8, database: []const u8, application_name: []const u8) !void {
    const payload_len = 4 +
        ("user".len + 1 + user.len + 1) +
        ("database".len + 1 + database.len + 1) +
        ("application_name".len + 1 + application_name.len + 1) +
        ("client_encoding".len + 1 + "UTF8".len + 1) +
        1;
    try appendInt(out, allocator, u32, @as(u32, @intCast(payload_len + 4)));
    try appendInt(out, allocator, u32, 196608);
    try appendCString(out, allocator, "user");
    try appendCString(out, allocator, user);
    try appendCString(out, allocator, "database");
    try appendCString(out, allocator, database);
    try appendCString(out, allocator, "application_name");
    try appendCString(out, allocator, application_name);
    try appendCString(out, allocator, "client_encoding");
    try appendCString(out, allocator, "UTF8");
    try out.append(allocator, 0);
}

pub fn appendQuery(out: *std.ArrayList(u8), allocator: std.mem.Allocator, sql: []const u8) !void {
    try appendTaggedHeader(out, allocator, 'Q', sql.len + 1);
    try appendCString(out, allocator, sql);
}

pub fn appendPasswordMessage(out: *std.ArrayList(u8), allocator: std.mem.Allocator, password: []const u8) !void {
    try appendTaggedHeader(out, allocator, 'p', password.len + 1);
    try appendCString(out, allocator, password);
}

pub fn appendSaslInitialResponse(out: *std.ArrayList(u8), allocator: std.mem.Allocator, mechanism: []const u8, response: []const u8) !void {
    try appendTaggedHeader(out, allocator, 'p', mechanism.len + 1 + 4 + response.len);
    try appendCString(out, allocator, mechanism);
    try appendInt(out, allocator, u32, @as(u32, @intCast(response.len)));
    try out.appendSlice(allocator, response);
}

pub fn appendSaslResponse(out: *std.ArrayList(u8), allocator: std.mem.Allocator, response: []const u8) !void {
    try appendTaggedHeader(out, allocator, 'p', response.len);
    try out.appendSlice(allocator, response);
}

pub fn appendParse(out: *std.ArrayList(u8), allocator: std.mem.Allocator, statement_name: []const u8, sql: []const u8, param_types: []const u32) !void {
    const payload_len = statement_name.len + 1 + sql.len + 1 + 2 + (4 * param_types.len);
    try appendTaggedHeader(out, allocator, 'P', payload_len);
    try appendCString(out, allocator, statement_name);
    try appendCString(out, allocator, sql);
    try appendInt(out, allocator, u16, @as(u16, @intCast(param_types.len)));
    for (param_types) |oid| try appendInt(out, allocator, u32, oid);
}

pub fn appendBind(out: *std.ArrayList(u8), allocator: std.mem.Allocator, portal_name: []const u8, statement_name: []const u8, params: []const Param, result_format: FormatCode) !void {
    var payload_len: usize = portal_name.len + 1 + statement_name.len + 1;
    payload_len += 2 + (2 * params.len);
    payload_len += 2;
    for (params) |param| {
        payload_len += 4;
        if (param.value) |value| payload_len += value.len;
    }
    payload_len += 2 + 2;

    try appendTaggedHeader(out, allocator, 'B', payload_len);
    try appendCString(out, allocator, portal_name);
    try appendCString(out, allocator, statement_name);
    try appendInt(out, allocator, u16, @as(u16, @intCast(params.len)));
    for (params) |param| try appendInt(out, allocator, u16, @intFromEnum(param.format));
    try appendInt(out, allocator, u16, @as(u16, @intCast(params.len)));
    for (params) |param| {
        if (param.value) |value| {
            try appendInt(out, allocator, i32, @as(i32, @intCast(value.len)));
            try out.appendSlice(allocator, value);
        } else {
            try appendInt(out, allocator, i32, -1);
        }
    }
    try appendInt(out, allocator, u16, 1);
    try appendInt(out, allocator, u16, @intFromEnum(result_format));
}

pub fn appendExecute(out: *std.ArrayList(u8), allocator: std.mem.Allocator, portal_name: []const u8, max_rows: u32) !void {
    try appendTaggedHeader(out, allocator, 'E', portal_name.len + 1 + 4);
    try appendCString(out, allocator, portal_name);
    try appendInt(out, allocator, u32, max_rows);
}

pub fn appendSync(out: *std.ArrayList(u8), allocator: std.mem.Allocator) !void {
    try appendTaggedHeader(out, allocator, 'S', 0);
}

pub fn appendClose(out: *std.ArrayList(u8), allocator: std.mem.Allocator, target: CloseTarget, name: []const u8) !void {
    try appendTaggedHeader(out, allocator, 'C', 1 + name.len + 1);
    try out.append(allocator, @intFromEnum(target));
    try appendCString(out, allocator, name);
}

pub fn appendTerminate(out: *std.ArrayList(u8), allocator: std.mem.Allocator) !void {
    try appendTaggedHeader(out, allocator, 'X', 0);
}

pub fn decodeAuthentication(payload: []const u8) !Authentication {
    if (payload.len < 4) return error.InvalidAuthentication;
    const kind = std.mem.readInt(u32, payload[0..4], .big);
    return switch (kind) {
        0 => .ok,
        3 => .cleartext,
        5 => if (payload.len == 8) .{ .md5 = payload[4..8].* } else error.InvalidAuthentication,
        10 => .{ .sasl = try decodeSaslMechanisms(payload[4..]) },
        11 => .{ .sasl_continue = payload[4..] },
        12 => .{ .sasl_final = payload[4..] },
        else => error.UnsupportedAuthentication,
    };
}

pub const Authentication = union(enum) {
    ok,
    cleartext,
    md5: [4]u8,
    sasl: SaslMechanisms,
    sasl_continue: []const u8,
    sasl_final: []const u8,
};

pub const SaslMechanisms = struct {
    has_scram_sha_256: bool,
};

fn decodeSaslMechanisms(payload: []const u8) !SaslMechanisms {
    var found = false;
    var i: usize = 0;
    while (i < payload.len) {
        const end = std.mem.indexOfScalarPos(u8, payload, i, 0) orelse return error.InvalidAuthentication;
        if (end == i) break;
        if (std.mem.eql(u8, payload[i..end], "SCRAM-SHA-256")) found = true;
        i = end + 1;
    }
    return .{ .has_scram_sha_256 = found };
}

pub fn parseErrorResponse(allocator: std.mem.Allocator, payload: []const u8) !ErrorResponse {
    var out: ErrorResponse = .{};
    errdefer out.deinit(allocator);
    var i: usize = 0;
    while (i < payload.len and payload[i] != 0) {
        const field_type = payload[i];
        i += 1;
        const end = std.mem.indexOfScalarPos(u8, payload, i, 0) orelse return error.InvalidErrorResponse;
        const value = payload[i..end];
        switch (field_type) {
            'S' => {
                allocator.free(out.severity);
                out.severity = try allocator.dupe(u8, value);
            },
            'C' => {
                allocator.free(out.code);
                out.code = try allocator.dupe(u8, value);
            },
            'M' => {
                allocator.free(out.message);
                out.message = try allocator.dupe(u8, value);
            },
            else => {},
        }
        i = end + 1;
    }
    return out;
}

pub fn fieldCString(payload: []const u8, index: *usize) ![]const u8 {
    const end = std.mem.indexOfScalarPos(u8, payload, index.*, 0) orelse return error.InvalidField;
    const value = payload[index.*..end];
    index.* = end + 1;
    return value;
}

fn appendTaggedHeader(out: *std.ArrayList(u8), allocator: std.mem.Allocator, tag: u8, payload_len: usize) !void {
    try out.append(allocator, tag);
    try appendInt(out, allocator, u32, @as(u32, @intCast(payload_len + 4)));
}

fn appendCString(out: *std.ArrayList(u8), allocator: std.mem.Allocator, text: []const u8) !void {
    try out.appendSlice(allocator, text);
    try out.append(allocator, 0);
}

fn appendInt(out: *std.ArrayList(u8), allocator: std.mem.Allocator, comptime T: type, value: T) !void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .big);
    try out.appendSlice(allocator, &bytes);
}

test "decode auth sasl" {
    const auth = try decodeAuthentication(&[_]u8{
        0,   0,   0,   10,
        'S', 'C', 'R', 'A',
        'M', '-', 'S', 'H',
        'A', '-', '2', '5',
        '6', 0,   0,
    });
    try std.testing.expect(auth == .sasl);
    try std.testing.expect(auth.sasl.has_scram_sha_256);
}

test "parse error response extracts primary fields" {
    var err = try parseErrorResponse(std.testing.allocator, &[_]u8{
        'S', 'E', 'R', 'R', 'O', 'R', 0,
        'C', '4', '2', 'P', '0', '1', 0,
        'M', 'b', 'a', 'd', ' ', 's', 'q',
        'l', 0,   0,
    });
    defer err.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("ERROR", err.severity);
    try std.testing.expectEqualStrings("42P01", err.code);
    try std.testing.expectEqualStrings("bad sql", err.message);
}

test "read message enforces maximum length" {
    var bytes = [_]u8{ 'Z', 0, 0, 0, 8, 'I', 0, 0, 0 };
    var reader = std.Io.Reader.fixed(&bytes);
    var buffer: []u8 = &.{};
    defer if (buffer.len != 0) std.testing.allocator.free(buffer);
    try std.testing.expectError(error.InvalidMessageLength, readMessageInto(std.testing.allocator, &reader, 1, &buffer));
}

test "parse error response keeps last duplicate field without leaking" {
    var err = try parseErrorResponse(std.testing.allocator, &[_]u8{
        'M', 'o', 'n', 'e', 0,
        'M', 't', 'w', 'o', 0,
        0,
    });
    defer err.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("two", err.message);
}

test "append bind encodes null and binary parameters" {
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(std.testing.allocator);

    try appendBind(&out, std.testing.allocator, "", "stmt", &.{
        .{ .format = .text, .value = "abc" },
        .{ .type_oid = 17, .format = .binary, .value = &.{ 1, 2, 3 } },
        .{ .type_oid = 23, .format = .text, .value = null },
    }, .binary);

    try std.testing.expectEqual(@as(u8, 'B'), out.items[0]);
    try std.testing.expect(std.mem.indexOf(u8, out.items, "stmt") != null);
}

test "append parse encodes statement and parameter types" {
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(std.testing.allocator);

    try appendParse(&out, std.testing.allocator, "s", "select $1", &.{ 23, 25 });

    try std.testing.expectEqual(@as(u8, 'P'), out.items[0]);
    try std.testing.expect(std.mem.indexOf(u8, out.items, "select $1") != null);
    try std.testing.expect(std.mem.indexOf(u8, out.items, "s") != null);
}

test "append startup query sync and close encode expected tags" {
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(std.testing.allocator);

    try appendStartup(&out, std.testing.allocator, "u", "d", "app");
    try std.testing.expectEqual(@as(u8, 0), out.items[0]);

    out.clearRetainingCapacity();
    try appendQuery(&out, std.testing.allocator, "select 1");
    try std.testing.expectEqual(@as(u8, 'Q'), out.items[0]);
    try std.testing.expectEqual(@as(u8, 0), out.items[out.items.len - 1]);

    out.clearRetainingCapacity();
    try appendSync(&out, std.testing.allocator);
    try std.testing.expectEqual(@as(u8, 'S'), out.items[0]);
    try std.testing.expectEqualSlices(u8, &.{ 'S', 0, 0, 0, 4 }, out.items);

    out.clearRetainingCapacity();
    try appendClose(&out, std.testing.allocator, .portal, "p1");
    try std.testing.expectEqual(@as(u8, 'C'), out.items[0]);
    try std.testing.expectEqual(@as(u8, 'P'), out.items[5]);
}

test "field c string rejects missing terminator and advances index" {
    var index: usize = 0;
    try std.testing.expectEqualStrings("abc", try fieldCString("abc\x00rest", &index));
    try std.testing.expectEqual(@as(usize, 4), index);

    index = 0;
    try std.testing.expectError(error.InvalidField, fieldCString("unterminated", &index));
}

test "read message into reuses and grows buffer" {
    var bytes = [_]u8{ 'Z', 0, 0, 0, 5, 'I' };
    var reader = std.Io.Reader.fixed(&bytes);
    var buffer: []u8 = &.{};
    defer if (buffer.len != 0) std.testing.allocator.free(buffer);

    const msg = try readMessageInto(std.testing.allocator, &reader, 16, &buffer);
    try std.testing.expectEqual(@as(u8, 'Z'), msg.tag);
    try std.testing.expectEqual(@as(usize, 1), msg.payload.len);
    try std.testing.expectEqual(@as(u8, 'I'), msg.payload[0]);
    try std.testing.expect(buffer.len >= 1);
}

test "decode authentication rejects truncated and unsupported payloads" {
    try std.testing.expectError(error.InvalidAuthentication, decodeAuthentication(&.{ 0, 0, 0 }));
    try std.testing.expectError(error.UnsupportedAuthentication, decodeAuthentication(&.{ 0, 0, 0, 99 }));
    try std.testing.expectError(error.InvalidAuthentication, decodeAuthentication(&.{ 0, 0, 0, 5, 1, 2, 3 }));
}

test "protocol fuzz parsers stay bounded" {
    var prng = std.Random.DefaultPrng.init(0x1234abcd);
    const random = prng.random();
    for (0..3000) |_| {
        const len = random.intRangeLessThan(usize, 0, 96);
        const bytes = try std.testing.allocator.alloc(u8, len);
        defer std.testing.allocator.free(bytes);
        random.bytes(bytes);
        _ = decodeAuthentication(bytes) catch {};
        if (parseErrorResponse(std.testing.allocator, bytes)) |err| {
            var owned = err;
            owned.deinit(std.testing.allocator);
        } else |_| {}
        if (len != 0) {
            var index: usize = random.intRangeLessThan(usize, 0, len);
            _ = fieldCString(bytes, &index) catch {};
        }
    }
}

const std = @import("std");

pub const Message = struct {
    tag: u8,
    payload: []u8,

    pub fn deinit(msg: *Message, allocator: std.mem.Allocator) void {
        allocator.free(msg.payload);
        msg.* = undefined;
    }
};

pub const MessageView = struct {
    tag: u8,
    payload: []u8,
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

pub fn readMessage(allocator: std.mem.Allocator, reader: *std.Io.Reader, max_message_len: u32) !Message {
    const tag = try reader.takeByte();
    const len = try reader.takeInt(u32, .big);
    if (len < 4 or len - 4 > max_message_len) return error.InvalidMessageLength;
    const payload = try allocator.alloc(u8, len - 4);
    errdefer allocator.free(payload);
    try reader.readSliceAll(payload);
    return .{ .tag = tag, .payload = payload };
}

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

pub fn writeStartup(writer: *std.Io.Writer, user: []const u8, database: []const u8, application_name: []const u8) !void {
    const payload_len = 4 +
        ("user".len + 1 + user.len + 1) +
        ("database".len + 1 + database.len + 1) +
        ("application_name".len + 1 + application_name.len + 1) +
        ("client_encoding".len + 1 + "UTF8".len + 1) +
        1;
    var header: [8]u8 = undefined;
    std.mem.writeInt(u32, header[0..4], @as(u32, @intCast(payload_len + 4)), .big);
    std.mem.writeInt(u32, header[4..8], 196608, .big);
    var vec = [_][]const u8{
        header[0..],
        "user",
        &.{0},
        user,
        &.{0},
        "database",
        &.{0},
        database,
        &.{0},
        "application_name",
        &.{0},
        application_name,
        &.{0},
        "client_encoding",
        &.{0},
        "UTF8",
        &.{0},
        &.{0},
    };
    try writer.writeVecAll(&vec);
    try writer.flush();
}

pub fn writeQuery(writer: *std.Io.Writer, sql: []const u8) !void {
    var header: [5]u8 = undefined;
    header[0] = 'Q';
    std.mem.writeInt(u32, header[1..5], @as(u32, @intCast(sql.len + 5)), .big);
    var vec = [_][]const u8{ header[0..], sql, &.{0} };
    try writer.writeVecAll(&vec);
    try writer.flush();
}

pub fn writePassword(writer: *std.Io.Writer, password: []const u8) !void {
    var header: [5]u8 = undefined;
    header[0] = 'p';
    std.mem.writeInt(u32, header[1..5], @as(u32, @intCast(password.len + 5)), .big);
    var vec = [_][]const u8{ header[0..], password, &.{0} };
    try writer.writeVecAll(&vec);
    try writer.flush();
}

pub fn writeSaslInitialResponse(writer: *std.Io.Writer, mechanism: []const u8, response: []const u8) !void {
    var header: [5]u8 = undefined;
    const len = mechanism.len + 1 + 4 + response.len + 4;
    header[0] = 'p';
    std.mem.writeInt(u32, header[1..5], @as(u32, @intCast(len)), .big);
    var resp_len: [4]u8 = undefined;
    std.mem.writeInt(u32, &resp_len, @as(u32, @intCast(response.len)), .big);
    var vec = [_][]const u8{ header[0..], mechanism, &.{0}, resp_len[0..], response };
    try writer.writeVecAll(&vec);
    try writer.flush();
}

pub fn writeSaslResponse(writer: *std.Io.Writer, response: []const u8) !void {
    var header: [5]u8 = undefined;
    header[0] = 'p';
    std.mem.writeInt(u32, header[1..5], @as(u32, @intCast(response.len + 4)), .big);
    var vec = [_][]const u8{ header[0..], response };
    try writer.writeVecAll(&vec);
    try writer.flush();
}

pub fn writeTerminate(writer: *std.Io.Writer) !void {
    var header: [5]u8 = .{ 'X', 0, 0, 0, 4 };
    try writer.writeAll(&header);
    try writer.flush();
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
    try std.testing.expectError(error.InvalidMessageLength, readMessage(std.testing.allocator, &reader, 1));
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

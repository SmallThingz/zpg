const std = @import("std");
const Config = @import("config.zig").Config;
const proto = @import("proto.zig");
const scram = @import("scram.zig");
const TlsClient = std.crypto.tls.Client;
const TlsHost = @FieldType(TlsClient.Options, "host");
const TlsCa = @FieldType(TlsClient.Options, "ca");

pub const Column = struct {
    name: []const u8,
    type_oid: u32,
    format: proto.FormatCode,
};

pub const Value = union(enum) {
    null,
    typed_null: u32,
    bool: bool,
    int2: i16,
    int4: i32,
    int8: i64,
    uint2: u16,
    uint4: u32,
    uint8: u64,
    float4: f32,
    float8: f64,
    text: []const u8,
    bytea: []const u8,
    raw_text: []const u8,
    raw_binary: struct {
        type_oid: u32,
        bytes: []const u8,
    },
};

pub const QueryProtocol = enum {
    simple,
    extended,
};

pub const QueryOptions = struct {
    protocol: QueryProtocol = .simple,
    result_format: proto.FormatCode = .text,
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

    pub fn decode(result: Result, row_index: usize, column_index: usize, comptime T: type) !?T {
        if (row_index >= result.rows.len) return error.RowOutOfBounds;
        return result.decodeRowValue(result.rows[row_index], column_index, T);
    }

    pub fn decodeByName(result: Result, row_index: usize, name: []const u8, comptime T: type) !?T {
        const column_index = result.columnIndex(name) orelse return error.UnknownColumn;
        return result.decode(row_index, column_index, T);
    }

    pub fn deinit(result: *Result) void {
        result.arena.deinit();
        result.* = undefined;
    }

    fn decodeRowValue(result: Result, row: Row, column_index: usize, comptime T: type) !?T {
        if (column_index >= result.columns.len or column_index >= row.values.len) return error.ColumnOutOfBounds;
        const raw = row.values[column_index] orelse return null;
        const column = result.columns[column_index];
        const value: T = switch (column.format) {
            .text => try decodeTextValue(T, raw),
            .binary => try decodeBinaryValue(T, column.type_oid, raw),
        };
        return value;
    }
};

pub const Statement = struct {
    allocator: std.mem.Allocator,
    conn: *Conn,
    name: []u8,
    closed: bool = false,

    pub fn query(stmt: *Statement, allocator: std.mem.Allocator) !Result {
        return stmt.queryValues(allocator, &.{}, .{});
    }

    pub fn queryValues(stmt: *Statement, allocator: std.mem.Allocator, values: []const Value, opts: QueryOptions) !Result {
        return stmt.conn.queryPrepared(stmt.name, allocator, values, opts);
    }

    pub fn exec(stmt: *Statement, allocator: std.mem.Allocator) ![]const u8 {
        return stmt.execValues(allocator, &.{}, .{});
    }

    pub fn execValues(stmt: *Statement, allocator: std.mem.Allocator, values: []const Value, opts: QueryOptions) ![]const u8 {
        return stmt.conn.execPrepared(stmt.name, allocator, values, opts);
    }

    pub fn close(stmt: *Statement) !void {
        if (stmt.closed) return;
        stmt.closed = true;
        defer stmt.allocator.free(stmt.name);
        try stmt.conn.closePreparedStatement(stmt.name);
    }

    pub fn deinit(stmt: *Statement) void {
        stmt.close() catch stmt.allocator.free(stmt.name);
        stmt.* = undefined;
    }
};

const TlsState = struct {
    client: TlsClient,
    read_buffer: []u8,
    write_buffer: []u8,
    ca_bundle: ?std.crypto.Certificate.Bundle = null,

    fn deinit(state: *TlsState, allocator: std.mem.Allocator) void {
        state.client.end() catch {};
        if (state.ca_bundle) |*bundle| bundle.deinit(allocator);
        allocator.free(state.read_buffer);
        allocator.free(state.write_buffer);
        allocator.destroy(state);
    }
};

pub const Conn = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    config: *const Config,
    stream: std.Io.net.Stream,
    reader: std.Io.net.Stream.Reader,
    writer: std.Io.net.Stream.Writer,
    read_buffer: []u8,
    write_buffer: []u8,
    tls: ?*TlsState = null,
    message_buffer: []u8 = &.{},
    backend_pid: i32 = 0,
    backend_key: i32 = 0,
    tx_status: u8 = 'I',
    healthy: bool = true,
    last_error: ?proto.ErrorResponse = null,
    next_statement_id: u64 = 0,

    pub fn connect(allocator: std.mem.Allocator, io: std.Io, config: *const Config) !*Conn {
        return create(allocator, io, config);
    }

    pub fn create(allocator: std.mem.Allocator, io: std.Io, config: *const Config) !*Conn {
        const conn = try allocator.create(Conn);
        errdefer allocator.destroy(conn);

        const stream = try connectStream(config, io);
        var initialized = false;
        errdefer if (initialized) conn.close() else stream.close(io);
        const read_buffer = try allocator.alloc(u8, networkBufferLen(config));
        errdefer if (!initialized) allocator.free(read_buffer);
        const write_buffer = try allocator.alloc(u8, networkBufferLen(config));
        errdefer if (!initialized) allocator.free(write_buffer);

        conn.* = .{
            .allocator = allocator,
            .io = io,
            .config = config,
            .stream = stream,
            .reader = undefined,
            .writer = undefined,
            .read_buffer = read_buffer,
            .write_buffer = write_buffer,
        };
        conn.reader = conn.stream.reader(io, conn.read_buffer);
        conn.writer = conn.stream.writer(io, conn.write_buffer);
        initialized = true;
        try conn.negotiateTls();
        try conn.startup();
        return conn;
    }

    pub fn destroy(conn: *Conn) void {
        const allocator = conn.allocator;
        conn.close();
        allocator.destroy(conn);
    }

    pub fn close(conn: *Conn) void {
        const allocator = conn.allocator;
        defer conn.* = undefined;
        if (conn.last_error) |*err| err.deinit(allocator);
        var out: std.ArrayList(u8) = .empty;
        defer out.deinit(allocator);
        proto.appendTerminate(&out, allocator) catch {};
        conn.writeMessages(out.items) catch {};
        if (conn.tls) |tls| {
            tls.deinit(allocator);
            conn.tls = null;
        }
        conn.stream.close(conn.io);
        if (conn.message_buffer.len != 0) allocator.free(conn.message_buffer);
        allocator.free(conn.read_buffer);
        allocator.free(conn.write_buffer);
    }

    pub fn queryAlloc(conn: *Conn, allocator: std.mem.Allocator, sql: []const u8) !Result {
        return conn.queryOpts(allocator, sql, .{});
    }

    pub fn query(conn: *Conn, allocator: std.mem.Allocator, sql: []const u8) !Result {
        return conn.queryOpts(allocator, sql, .{});
    }

    pub fn queryOpts(conn: *Conn, allocator: std.mem.Allocator, sql: []const u8, opts: QueryOptions) !Result {
        if (opts.protocol == .simple and opts.result_format == .text) {
            return conn.querySimple(allocator, sql);
        }
        return conn.queryExtendedRaw(allocator, sql, &.{}, opts);
    }

    pub fn queryValues(conn: *Conn, allocator: std.mem.Allocator, sql: []const u8, values: []const Value, opts: QueryOptions) !Result {
        return conn.queryExtendedValues(null, allocator, sql, values, opts);
    }

    pub fn exec(conn: *Conn, allocator: std.mem.Allocator, sql: []const u8) ![]const u8 {
        return conn.execOpts(allocator, sql, .{});
    }

    pub fn execOpts(conn: *Conn, allocator: std.mem.Allocator, sql: []const u8, opts: QueryOptions) ![]const u8 {
        if (opts.protocol == .simple and opts.result_format == .text) {
            return conn.execSimple(allocator, sql);
        }
        return conn.execExtendedRaw(allocator, sql, &.{}, opts);
    }

    pub fn execValues(conn: *Conn, allocator: std.mem.Allocator, sql: []const u8, values: []const Value, opts: QueryOptions) ![]const u8 {
        return conn.execExtendedValues(null, allocator, sql, values, opts);
    }

    pub fn prepare(conn: *Conn, allocator: std.mem.Allocator, sql: []const u8) !Statement {
        conn.clearLastError();
        const name = try std.fmt.allocPrint(allocator, "zpg_stmt_{x}", .{conn.next_statement_id});
        errdefer allocator.free(name);
        conn.next_statement_id += 1;

        var out: std.ArrayList(u8) = .empty;
        defer out.deinit(conn.allocator);
        try proto.appendParse(&out, conn.allocator, name, sql, &.{});
        try proto.appendSync(&out, conn.allocator);
        try conn.writeMessages(out.items);

        var saw_parse_complete = false;
        var failed = false;
        while (true) {
            const msg = try conn.readMessage();
            switch (msg.tag) {
                '1' => saw_parse_complete = true,
                'E' => {
                    failed = true;
                    conn.setLastError(msg.payload) catch return conn.protocolError();
                },
                'S', 'N' => {},
                'Z' => {
                    try conn.finishReady(msg.payload);
                    if (failed) return error.PgServer;
                    if (!saw_parse_complete) return conn.protocolError();
                    return .{
                        .allocator = allocator,
                        .conn = conn,
                        .name = name,
                    };
                },
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

    fn queryPrepared(conn: *Conn, statement_name: []const u8, allocator: std.mem.Allocator, values: []const Value, opts: QueryOptions) !Result {
        return conn.queryExtendedValues(statement_name, allocator, "", values, .{
            .protocol = .extended,
            .result_format = opts.result_format,
        });
    }

    fn execPrepared(conn: *Conn, statement_name: []const u8, allocator: std.mem.Allocator, values: []const Value, opts: QueryOptions) ![]const u8 {
        return conn.execExtendedValues(statement_name, allocator, "", values, .{
            .protocol = .extended,
            .result_format = opts.result_format,
        });
    }

    fn closePreparedStatement(conn: *Conn, statement_name: []const u8) !void {
        conn.clearLastError();
        var out: std.ArrayList(u8) = .empty;
        defer out.deinit(conn.allocator);
        try proto.appendClose(&out, conn.allocator, .statement, statement_name);
        try proto.appendSync(&out, conn.allocator);
        try conn.writeMessages(out.items);

        var saw_close_complete = false;
        var failed = false;
        while (true) {
            const msg = try conn.readMessage();
            switch (msg.tag) {
                '3' => saw_close_complete = true,
                'E' => {
                    failed = true;
                    conn.setLastError(msg.payload) catch return conn.protocolError();
                },
                'S', 'N' => {},
                'Z' => {
                    try conn.finishReady(msg.payload);
                    if (failed) return error.PgServer;
                    if (!saw_close_complete) return conn.protocolError();
                    return;
                },
                else => return conn.protocolError(),
            }
        }
    }

    fn querySimple(conn: *Conn, allocator: std.mem.Allocator, sql: []const u8) !Result {
        conn.clearLastError();
        var out: std.ArrayList(u8) = .empty;
        defer out.deinit(conn.allocator);
        try proto.appendQuery(&out, conn.allocator, sql);
        try conn.writeMessages(out.items);
        return conn.readQueryResult(allocator, false);
    }

    fn execSimple(conn: *Conn, allocator: std.mem.Allocator, sql: []const u8) ![]const u8 {
        conn.clearLastError();
        var out: std.ArrayList(u8) = .empty;
        defer out.deinit(conn.allocator);
        try proto.appendQuery(&out, conn.allocator, sql);
        try conn.writeMessages(out.items);
        return conn.readExecResult(allocator, false);
    }

    fn queryExtendedValues(conn: *Conn, statement_name: ?[]const u8, allocator: std.mem.Allocator, sql: []const u8, values: []const Value, opts: QueryOptions) !Result {
        var scratch = std.heap.ArenaAllocator.init(allocator);
        defer scratch.deinit();
        const params = try encodeValues(scratch.allocator(), values);
        if (statement_name) |name| {
            return conn.queryPreparedRaw(name, allocator, params, opts);
        }
        return conn.queryExtendedRaw(allocator, sql, params, opts);
    }

    fn execExtendedValues(conn: *Conn, statement_name: ?[]const u8, allocator: std.mem.Allocator, sql: []const u8, values: []const Value, opts: QueryOptions) ![]const u8 {
        var scratch = std.heap.ArenaAllocator.init(allocator);
        defer scratch.deinit();
        const params = try encodeValues(scratch.allocator(), values);
        if (statement_name) |name| {
            return conn.execPreparedRaw(name, allocator, params, opts);
        }
        return conn.execExtendedRaw(allocator, sql, params, opts);
    }

    fn queryExtendedRaw(conn: *Conn, allocator: std.mem.Allocator, sql: []const u8, params: []const proto.Param, opts: QueryOptions) !Result {
        conn.clearLastError();
        try conn.sendExtendedQuery("", sql, params, opts.result_format, true);
        return conn.readQueryResult(allocator, true);
    }

    fn queryPreparedRaw(conn: *Conn, statement_name: []const u8, allocator: std.mem.Allocator, params: []const proto.Param, opts: QueryOptions) !Result {
        conn.clearLastError();
        try conn.sendExtendedExecute(statement_name, params, opts.result_format);
        return conn.readQueryResult(allocator, true);
    }

    fn execExtendedRaw(conn: *Conn, allocator: std.mem.Allocator, sql: []const u8, params: []const proto.Param, opts: QueryOptions) ![]const u8 {
        conn.clearLastError();
        try conn.sendExtendedQuery("", sql, params, opts.result_format, true);
        return conn.readExecResult(allocator, true);
    }

    fn execPreparedRaw(conn: *Conn, statement_name: []const u8, allocator: std.mem.Allocator, params: []const proto.Param, opts: QueryOptions) ![]const u8 {
        conn.clearLastError();
        try conn.sendExtendedExecute(statement_name, params, opts.result_format);
        return conn.readExecResult(allocator, true);
    }

    fn sendExtendedQuery(conn: *Conn, statement_name: []const u8, sql: []const u8, params: []const proto.Param, result_format: proto.FormatCode, include_parse: bool) !void {
        var out: std.ArrayList(u8) = .empty;
        defer out.deinit(conn.allocator);
        if (include_parse) {
            const param_types = try collectParamTypes(conn.allocator, params);
            defer conn.allocator.free(param_types);
            try proto.appendParse(&out, conn.allocator, statement_name, sql, param_types);
        }
        try proto.appendBind(&out, conn.allocator, "", statement_name, params, result_format);
        try proto.appendDescribe(&out, conn.allocator, .portal, "");
        try proto.appendExecute(&out, conn.allocator, "", 0);
        try proto.appendSync(&out, conn.allocator);
        try conn.writeMessages(out.items);
    }

    fn sendExtendedExecute(conn: *Conn, statement_name: []const u8, params: []const proto.Param, result_format: proto.FormatCode) !void {
        var out: std.ArrayList(u8) = .empty;
        defer out.deinit(conn.allocator);
        try proto.appendBind(&out, conn.allocator, "", statement_name, params, result_format);
        try proto.appendDescribe(&out, conn.allocator, .portal, "");
        try proto.appendExecute(&out, conn.allocator, "", 0);
        try proto.appendSync(&out, conn.allocator);
        try conn.writeMessages(out.items);
    }

    fn readQueryResult(conn: *Conn, allocator: std.mem.Allocator, extended: bool) !Result {
        var result = Result{ .arena = std.heap.ArenaAllocator.init(allocator) };
        errdefer result.deinit();
        const arena = result.arena.allocator();
        var columns: std.ArrayList(Column) = .empty;
        defer columns.deinit(arena);
        var rows: std.ArrayList(Row) = .empty;
        defer rows.deinit(arena);
        var command_tag: []const u8 = "";
        var failed = false;
        var saw_extended_control = !extended;

        while (true) {
            const msg = try conn.readMessage();
            switch (msg.tag) {
                '1', '2' => saw_extended_control = true,
                'T' => try parseRowDescription(arena, &columns, msg.payload),
                'D' => try rows.append(arena, try parseDataRow(arena, msg.payload)),
                'C' => command_tag = try arena.dupe(u8, try parseCommandComplete(msg.payload)),
                'I', 'n' => {},
                'E' => {
                    failed = true;
                    conn.setLastError(msg.payload) catch return conn.protocolError();
                },
                'S', 'N' => {},
                'Z' => {
                    try conn.finishReady(msg.payload);
                    if (!saw_extended_control) return conn.protocolError();
                    result.columns = try columns.toOwnedSlice(arena);
                    result.rows = try rows.toOwnedSlice(arena);
                    result.command_tag = command_tag;
                    if (failed) return error.PgServer;
                    return result;
                },
                else => return conn.protocolError(),
            }
        }
    }

    fn readExecResult(conn: *Conn, allocator: std.mem.Allocator, extended: bool) ![]const u8 {
        var command_tag: ?[]const u8 = null;
        var failed = false;
        var saw_extended_control = !extended;
        while (true) {
            const msg = try conn.readMessage();
            switch (msg.tag) {
                '1', '2' => saw_extended_control = true,
                'C' => command_tag = try allocator.dupe(u8, try parseCommandComplete(msg.payload)),
                'T', 'D', 'I', 'n', 'S', 'N' => {},
                'E' => {
                    failed = true;
                    conn.setLastError(msg.payload) catch return conn.protocolError();
                },
                'Z' => {
                    try conn.finishReady(msg.payload);
                    if (!saw_extended_control) return conn.protocolError();
                    if (failed) return error.PgServer;
                    return command_tag orelse try allocator.dupe(u8, "");
                },
                else => return conn.protocolError(),
            }
        }
    }

    fn startup(conn: *Conn) !void {
        var out: std.ArrayList(u8) = .empty;
        defer out.deinit(conn.allocator);
        try proto.appendStartup(&out, conn.allocator, conn.config.user, conn.config.database, conn.config.application_name);
        try conn.writeMessages(out.items);
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
                    try conn.finishReady(msg.payload);
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
            .cleartext => {
                var out: std.ArrayList(u8) = .empty;
                defer out.deinit(conn.allocator);
                try proto.appendPasswordMessage(&out, conn.allocator, conn.config.password orelse "");
                try conn.writeMessages(out.items);
            },
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
                var out: std.ArrayList(u8) = .empty;
                defer out.deinit(conn.allocator);
                try proto.appendPasswordMessage(&out, conn.allocator, md5_text);
                try conn.writeMessages(out.items);
            },
            .sasl => |mechanisms| {
                if (!mechanisms.has_scram_sha_256) return error.UnsupportedAuthentication;
                var client = try scram.Client.init(conn.io, conn.config.password orelse "");
                {
                    var out: std.ArrayList(u8) = .empty;
                    defer out.deinit(conn.allocator);
                    try proto.appendSaslInitialResponse(&out, conn.allocator, "SCRAM-SHA-256", client.initialMessage());
                    try conn.writeMessages(out.items);
                }
                const cont = try conn.readMessage();
                if (cont.tag != 'R') return conn.protocolError();
                const challenge = switch (try proto.decodeAuthentication(cont.payload)) {
                    .sasl_continue => |value| value,
                    else => return conn.protocolError(),
                };
                var response_buffer: [256]u8 = undefined;
                const client_final = try client.serverFirst(challenge, &response_buffer);
                {
                    var out: std.ArrayList(u8) = .empty;
                    defer out.deinit(conn.allocator);
                    try proto.appendSaslResponse(&out, conn.allocator, client_final);
                    try conn.writeMessages(out.items);
                }
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

    fn finishReady(conn: *Conn, payload: []const u8) !void {
        if (payload.len != 1) return conn.protocolError();
        conn.tx_status = payload[0];
        if (conn.tx_status != 'I') conn.healthy = false;
    }

    fn readMessage(conn: *Conn) !proto.MessageView {
        return proto.readMessageInto(conn.allocator, conn.currentReader(), conn.config.max_message_len, &conn.message_buffer);
    }

    fn currentReader(conn: *Conn) *std.Io.Reader {
        if (conn.tls) |tls| return &tls.client.reader;
        return &conn.reader.interface;
    }

    fn writeMessages(conn: *Conn, bytes: []const u8) !void {
        if (conn.tls) |tls| {
            try tls.client.writer.writeAll(bytes);
            try tls.client.writer.flush();
            try conn.writer.interface.flush();
            return;
        }
        try conn.writer.interface.writeAll(bytes);
        try conn.writer.interface.flush();
    }

    fn negotiateTls(conn: *Conn) !void {
        if (conn.config.ssl_mode == .disable) return;

        var out: std.ArrayList(u8) = .empty;
        defer out.deinit(conn.allocator);
        try proto.appendSslRequest(&out, conn.allocator);
        try conn.writer.interface.writeAll(out.items);
        try conn.writer.interface.flush();

        const response = conn.reader.interface.takeByte() catch |err| switch (err) {
            error.EndOfStream => switch (conn.config.ssl_mode) {
                .prefer => return,
                else => return err,
            },
            else => return err,
        };
        switch (response) {
            'S' => try conn.enableTls(),
            'N' => switch (conn.config.ssl_mode) {
                .prefer => {},
                else => return error.TlsUnsupported,
            },
            else => return error.ProtocolViolation,
        }
    }

    fn enableTls(conn: *Conn) !void {
        const tls = try conn.allocator.create(TlsState);
        errdefer conn.allocator.destroy(tls);

        tls.read_buffer = try conn.allocator.alloc(u8, TlsClient.min_buffer_len);
        errdefer conn.allocator.free(tls.read_buffer);
        tls.write_buffer = try conn.allocator.alloc(u8, TlsClient.min_buffer_len);
        errdefer conn.allocator.free(tls.write_buffer);
        tls.ca_bundle = null;

        const now = std.Io.Timestamp.now(conn.io, .real);
        const ca: TlsCa = switch (conn.config.ssl_mode) {
            .require => .no_verification,
            .verify_ca, .verify_full => blk: {
                var bundle: std.crypto.Certificate.Bundle = .{};
                errdefer bundle.deinit(conn.allocator);
                if (conn.config.ssl_root_cert) |path| {
                    try bundle.addCertsFromFilePathAbsolute(conn.allocator, conn.io, now, path);
                } else {
                    try bundle.rescan(conn.allocator, conn.io, now);
                }
                tls.ca_bundle = bundle;
                break :blk .{ .bundle = tls.ca_bundle.? };
            },
            .prefer => .no_verification,
            .disable => unreachable,
        };
        const host: TlsHost = switch (conn.config.ssl_mode) {
            .verify_full => .{ .explicit = conn.config.host },
            else => .no_verification,
        };

        var random_buffer: [TlsClient.Options.entropy_len]u8 = undefined;
        conn.io.random(&random_buffer);
        tls.client = TlsClient.init(
            &conn.reader.interface,
            &conn.writer.interface,
            .{
                .host = host,
                .ca = ca,
                .read_buffer = tls.read_buffer,
                .write_buffer = tls.write_buffer,
                .entropy = &random_buffer,
                .realtime_now_seconds = now.toSeconds(),
                .allow_truncation_attacks = false,
            },
        ) catch |err| switch (err) {
            error.WriteFailed => return conn.writer.err.?,
            error.ReadFailed => return conn.reader.err.?,
            else => return err,
        };
        conn.tls = tls;
    }
};

fn connectStream(config: *const Config, io: std.Io) !std.Io.net.Stream {
    const options: std.Io.net.IpAddress.ConnectOptions = .{
        .mode = .stream,
        // Zig 0.16's stdlib still panics if a TCP connect timeout is passed
        // through `Io.net`. Ignore the parsed timeout here rather than crash
        // the process on the default `connect_timeout`.
        .timeout = .none,
    };
    const address = std.Io.net.IpAddress.parse(config.host, config.port) catch {
        const host: std.Io.net.HostName = .{ .bytes = config.host };
        return host.connect(io, config.port, options);
    };
    return address.connect(io, options);
}

fn networkBufferLen(config: *const Config) usize {
    const base = 16 * 1024;
    return if (config.ssl_mode == .disable) base else @max(base, TlsClient.min_buffer_len);
}

fn collectParamTypes(allocator: std.mem.Allocator, params: []const proto.Param) ![]u32 {
    const out = try allocator.alloc(u32, params.len);
    for (params, out) |param, *oid| oid.* = param.type_oid;
    return out;
}

fn encodeValues(allocator: std.mem.Allocator, values: []const Value) ![]proto.Param {
    const params = try allocator.alloc(proto.Param, values.len);
    for (values, params) |value, *param| {
        param.* = switch (value) {
            .null => .{ .value = null },
            .typed_null => |oid| .{ .type_oid = oid, .value = null },
            .bool => |v| .{ .type_oid = 16, .value = if (v) "t" else "f" },
            .int2 => |v| .{ .type_oid = 21, .value = try std.fmt.allocPrint(allocator, "{d}", .{v}) },
            .int4 => |v| .{ .type_oid = 23, .value = try std.fmt.allocPrint(allocator, "{d}", .{v}) },
            .int8 => |v| .{ .type_oid = 20, .value = try std.fmt.allocPrint(allocator, "{d}", .{v}) },
            .uint2 => |v| .{ .type_oid = 21, .value = try std.fmt.allocPrint(allocator, "{d}", .{v}) },
            .uint4 => |v| .{ .type_oid = 23, .value = try std.fmt.allocPrint(allocator, "{d}", .{v}) },
            .uint8 => |v| .{ .type_oid = 20, .value = try std.fmt.allocPrint(allocator, "{d}", .{v}) },
            .float4 => |v| .{ .type_oid = 700, .value = try std.fmt.allocPrint(allocator, "{}", .{v}) },
            .float8 => |v| .{ .type_oid = 701, .value = try std.fmt.allocPrint(allocator, "{}", .{v}) },
            .text => |v| .{ .type_oid = 25, .value = v },
            .bytea => |v| .{ .type_oid = 17, .format = .binary, .value = v },
            .raw_text => |v| .{ .value = v },
            .raw_binary => |v| .{ .type_oid = v.type_oid, .format = .binary, .value = v.bytes },
        };
    }
    return params;
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
        const format = try parseFormatCode(try readIntAt(u16, payload, &i));
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

fn parseFormatCode(raw: u16) !proto.FormatCode {
    return switch (raw) {
        0 => .text,
        1 => .binary,
        else => error.UnsupportedColumnFormat,
    };
}

fn readIntAt(comptime T: type, payload: []const u8, index: *usize) !T {
    if (payload.len - index.* < @sizeOf(T)) return error.InvalidMessage;
    return std.mem.readInt(T, payload[index.*..][0..@sizeOf(T)], .big);
}

fn decodeTextValue(comptime T: type, raw: []const u8) !T {
    return switch (@typeInfo(T)) {
        .bool => switch (raw.len) {
            1 => switch (raw[0]) {
                't', '1' => true,
                'f', '0' => false,
                else => error.BadValue,
            },
            else => if (std.ascii.eqlIgnoreCase(raw, "true"))
                true
            else if (std.ascii.eqlIgnoreCase(raw, "false"))
                false
            else
                error.BadValue,
        },
        .int => std.fmt.parseInt(T, raw, 10),
        .float => std.fmt.parseFloat(T, raw),
        .pointer => if (T == []const u8) raw else error.UnsupportedDecode,
        .array => if (T == [16]u8 and raw.len == 16) raw[0..16].* else error.UnsupportedDecode,
        else => error.UnsupportedDecode,
    };
}

fn decodeBinaryValue(comptime T: type, type_oid: u32, raw: []const u8) !T {
    if (T == []const u8) return raw;
    if (T == [16]u8) {
        if (raw.len != 16) return error.InvalidBinaryValue;
        return raw[0..16].*;
    }
    return switch (type_oid) {
        16 => blk: {
            if (T != bool or raw.len != 1) return error.TypeMismatch;
            break :blk raw[0] != 0;
        },
        20 => decodeBinaryInt(T, i64, raw),
        21 => decodeBinaryInt(T, i16, raw),
        23 => decodeBinaryInt(T, i32, raw),
        700 => decodeBinaryFloat(T, u32, f32, raw),
        701 => decodeBinaryFloat(T, u64, f64, raw),
        17, 25, 1043, 114, 3802, 2950 => if (T == []const u8 or T == [16]u8)
            decodeTextCompatibleBinary(T, raw)
        else
            error.TypeMismatch,
        else => error.UnsupportedBinaryType,
    };
}

fn decodeTextCompatibleBinary(comptime T: type, raw: []const u8) !T {
    if (T == []const u8) return raw;
    if (T == [16]u8) {
        if (raw.len != 16) return error.InvalidBinaryValue;
        return raw[0..16].*;
    }
    return error.UnsupportedDecode;
}

fn decodeBinaryInt(comptime T: type, comptime Src: type, raw: []const u8) !T {
    if (raw.len != @sizeOf(Src)) return error.InvalidBinaryValue;
    const source = std.mem.readInt(Src, raw[0..@sizeOf(Src)], .big);
    return castInt(T, source);
}

fn castInt(comptime T: type, value: anytype) !T {
    return switch (@typeInfo(T)) {
        .int => std.math.cast(T, value) orelse error.ValueOverflow,
        else => error.TypeMismatch,
    };
}

fn decodeBinaryFloat(comptime T: type, comptime Bits: type, comptime FloatT: type, raw: []const u8) !T {
    if (T != FloatT or raw.len != @sizeOf(Bits)) return error.TypeMismatch;
    const bits = std.mem.readInt(Bits, raw[0..@sizeOf(Bits)], .big);
    return @bitCast(bits);
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

test "binary decode supports common scalar types" {
    try std.testing.expectEqual(@as(i32, 42), try decodeBinaryValue(i32, 23, &.{ 0, 0, 0, 42 }));
    try std.testing.expectEqual(@as(i64, 42), try decodeBinaryValue(i64, 20, &.{ 0, 0, 0, 0, 0, 0, 0, 42 }));
    try std.testing.expectEqual(true, try decodeBinaryValue(bool, 16, &.{1}));
}

test "text decode supports bool int float and bytes" {
    try std.testing.expectEqual(@as(i32, 42), try decodeTextValue(i32, "42"));
    try std.testing.expectEqual(true, try decodeTextValue(bool, "t"));
    try std.testing.expectApproxEqAbs(@as(f64, 1.25), try decodeTextValue(f64, "1.25"), 0.000001);
    try std.testing.expectEqualStrings("hello", try decodeTextValue([]const u8, "hello"));
}

test "encode values covers text and binary params" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const params = try encodeValues(arena.allocator(), &.{
        .{ .int4 = 7 },
        .{ .text = "ok" },
        .{ .bytea = &.{ 1, 2, 3 } },
        .null,
    });
    try std.testing.expectEqual(@as(usize, 4), params.len);
    try std.testing.expectEqual(@as(u32, 23), params[0].type_oid);
    try std.testing.expectEqual(proto.FormatCode.binary, params[2].format);
    try std.testing.expect(params[3].value == null);
}

test "result column index lookup" {
    var result = Result{ .arena = std.heap.ArenaAllocator.init(std.testing.allocator) };
    defer result.deinit();
    const arena = result.arena.allocator();
    result.columns = try arena.dupe(Column, &.{
        .{ .name = "id", .type_oid = 23, .format = .text },
        .{ .name = "name", .type_oid = 25, .format = .text },
    });
    try std.testing.expectEqual(@as(?usize, 1), result.columnIndex("name"));
    try std.testing.expectEqual(@as(?usize, null), result.columnIndex("missing"));
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

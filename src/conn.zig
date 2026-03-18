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

pub const Date = struct {
    year: i32,
    month: u8,
    day: u8,
};

pub const Time = struct {
    hour: u8,
    minute: u8,
    second: u8,
    microsecond: u32 = 0,
};

pub const Timestamp = struct {
    date: Date,
    time: Time,
};

pub fn CompiledResult(comptime RowT: type) type {
    return struct {
        raw: Result,
        rows: []const RowT,

        pub fn deinit(result: *@This()) void {
            result.raw.deinit();
            result.* = undefined;
        }
    };
}

pub fn CompiledQuery(comptime sql_text: []const u8, comptime Args: type, comptime RowT: type) type {
    comptime validateCompiledQuerySpec(sql_text, Args, RowT);

    return struct {
        pub const sql = sql_text;
        pub const Params = Args;
        pub const Row = RowT;
        pub const protocol = inferCompiledProtocol(sql_text, Args);
        pub const has_static_param_types = compiledArgsHaveStaticTypes(Args);
        pub const static_param_types = compiledStaticParamTypes(Args);

        pub fn query(conn: *Conn, allocator: std.mem.Allocator, args: Args) !CompiledResult(RowT) {
            const raw = try runRawQuery(conn, allocator, args);
            return materializeCompiledResult(RowT, raw);
        }

        pub fn exec(conn: *Conn, allocator: std.mem.Allocator, args: Args) ![]const u8 {
            return switch (protocol) {
                .simple => conn.execOpts(allocator, sql_text, .{
                    .protocol = .simple,
                    .result_format = .text,
                }),
                .extended => if (has_static_param_types)
                    conn.execCompiled(allocator, sql_text, args, static_param_types[0..], .{
                        .protocol = .extended,
                        .result_format = .text,
                    })
                else
                    blk: {
                    var scratch = std.heap.ArenaAllocator.init(allocator);
                    defer scratch.deinit();
                    const params = try encodeCompiledParams(scratch.allocator(), args);
                    break :blk conn.execExtendedRawWithParamTypes(allocator, sql_text, params, compiledParamTypesForQuery(), .{
                        .protocol = .extended,
                        .result_format = .text,
                    });
                    },
            };
        }

        pub fn queue(pipeline: *Pipeline, allocator: std.mem.Allocator, args: Args) !void {
            switch (protocol) {
                .simple => {
                    if (pipeline.opts.protocol != .simple) return error.UnsupportedProtocol;
                    try pipeline.query(sql_text);
                },
                .extended => {
                    if (pipeline.opts.protocol != .extended) return error.UnsupportedProtocol;
                    if (has_static_param_types) {
                        try pipeline.conn.queuePipelineCompiledQuery(
                            sql_text,
                            args,
                            static_param_types[0..],
                            pipeline.opts.result_format,
                            pipeline.next_portal_id,
                        );
                    } else {
                        var scratch = std.heap.ArenaAllocator.init(allocator);
                        defer scratch.deinit();
                        const params = try encodeCompiledParams(scratch.allocator(), args);
                        try pipeline.conn.queuePipelineParameterizedQuery(
                        sql_text,
                        params,
                        compiledParamTypesForQuery(),
                        pipeline.opts.result_format,
                        pipeline.next_portal_id,
                        );
                    }
                    pipeline.next_portal_id += 1;
                    pipeline.pending += 1;
                    pipeline.queued_since_flush += 1;
                },
            }
        }

        pub fn read(pipeline: *Pipeline, allocator: std.mem.Allocator) !CompiledResult(RowT) {
            const raw = try pipeline.readQuery(allocator);
            return materializeCompiledResult(RowT, raw);
        }

        fn runRawQuery(conn: *Conn, allocator: std.mem.Allocator, args: Args) !Result {
            return switch (protocol) {
                .simple => conn.queryOpts(allocator, sql_text, .{
                    .protocol = .simple,
                    .result_format = .text,
                }),
                .extended => if (has_static_param_types)
                    conn.queryCompiled(allocator, sql_text, args, static_param_types[0..], .{
                        .protocol = .extended,
                        .result_format = .text,
                    })
                else
                    blk: {
                    var scratch = std.heap.ArenaAllocator.init(allocator);
                    defer scratch.deinit();
                    const params = try encodeCompiledParams(scratch.allocator(), args);
                    break :blk conn.queryExtendedRawWithParamTypes(allocator, sql_text, params, compiledParamTypesForQuery(), .{
                        .protocol = .extended,
                        .result_format = .text,
                    });
                    },
            };
        }

        fn compiledParamTypesForQuery() ?[]const u32 {
            if (!has_static_param_types) return null;
            return static_param_types[0..];
        }
    };
}

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

pub const Pipeline = struct {
    conn: *Conn,
    opts: QueryOptions,
    pending: usize = 0,
    pending_in_batch: usize = 0,
    queued_since_flush: usize = 0,
    next_portal_id: u64 = 0,

    pub fn init(conn: *Conn, opts: QueryOptions) Pipeline {
        return .{
            .conn = conn,
            .opts = opts,
        };
    }

    pub fn query(pipeline: *Pipeline, sql: []const u8) !void {
        switch (pipeline.opts.protocol) {
            .simple => {
                if (pipeline.opts.result_format != .text) return error.UnsupportedResultFormat;
                try pipeline.conn.queuePipelineSimpleQuery(sql);
            },
            .extended => {
                try pipeline.conn.queuePipelineCachedQuery(sql, pipeline.opts.result_format, pipeline.next_portal_id);
                pipeline.next_portal_id += 1;
            },
        }
        pipeline.pending += 1;
        pipeline.queued_since_flush += 1;
    }

    pub fn queryValues(pipeline: *Pipeline, allocator: std.mem.Allocator, sql: []const u8, values: []const Value) !void {
        _ = allocator;
        if (pipeline.opts.protocol == .simple) return error.UnsupportedProtocol;
        try pipeline.conn.queuePipelineValueQuery(sql, values, pipeline.opts.result_format, pipeline.next_portal_id);
        pipeline.next_portal_id += 1;
        pipeline.pending += 1;
        pipeline.queued_since_flush += 1;
    }

    pub fn execValues(pipeline: *Pipeline, allocator: std.mem.Allocator, sql: []const u8, values: []const Value) !void {
        try pipeline.queryValues(allocator, sql, values);
    }

    pub fn preparedQueryValues(pipeline: *Pipeline, allocator: std.mem.Allocator, statement_name: []const u8, values: []const Value) !void {
        _ = allocator;
        if (pipeline.opts.protocol == .simple) return error.UnsupportedProtocol;
        try pipeline.conn.queuePipelinePreparedValueExecute(statement_name, values, pipeline.opts.result_format, pipeline.next_portal_id);
        pipeline.next_portal_id += 1;
        pipeline.pending += 1;
        pipeline.queued_since_flush += 1;
    }

    pub fn exec(pipeline: *Pipeline, sql: []const u8) !void {
        try pipeline.query(sql);
    }

    pub fn preparedExecValues(pipeline: *Pipeline, allocator: std.mem.Allocator, statement_name: []const u8, values: []const Value) !void {
        try pipeline.preparedQueryValues(allocator, statement_name, values);
    }

    pub fn flush(pipeline: *Pipeline) !void {
        if (pipeline.pending_in_batch != 0) return error.PendingResults;
        if (pipeline.queued_since_flush == 0) return;
        switch (pipeline.opts.protocol) {
            .simple => try pipeline.conn.flushPipelineSimpleQueries(),
            .extended => {
                try proto.appendSync(&pipeline.conn.queued_writes, pipeline.conn.allocator);
                try pipeline.conn.flushWrites();
            },
        }
        pipeline.pending_in_batch = pipeline.queued_since_flush;
        pipeline.queued_since_flush = 0;
    }

    pub fn readQuery(pipeline: *Pipeline, allocator: std.mem.Allocator) !Result {
        if (pipeline.pending == 0) return error.NoPendingResults;
        if (pipeline.pending_in_batch == 0) return error.PendingFlush;
        pipeline.pending -= 1;
        return switch (pipeline.opts.protocol) {
            .simple => blk: {
                pipeline.pending_in_batch -= 1;
                break :blk pipeline.conn.readPipelineSimpleQueryResult(allocator, pipeline.pending_in_batch == 0);
            },
            .extended => blk: {
                pipeline.pending_in_batch -= 1;
                break :blk pipeline.conn.readPipelineQueryResult(allocator, pipeline.pending_in_batch == 0);
            },
        };
    }

    pub fn readExec(pipeline: *Pipeline, allocator: std.mem.Allocator) ![]const u8 {
        if (pipeline.pending == 0) return error.NoPendingResults;
        if (pipeline.pending_in_batch == 0) return error.PendingFlush;
        pipeline.pending -= 1;
        return switch (pipeline.opts.protocol) {
            .simple => blk: {
                pipeline.pending_in_batch -= 1;
                break :blk pipeline.conn.readPipelineSimpleExecResult(allocator, pipeline.pending_in_batch == 0);
            },
            .extended => blk: {
                pipeline.pending_in_batch -= 1;
                break :blk pipeline.conn.readPipelineExecResult(allocator, pipeline.pending_in_batch == 0);
            },
        };
    }

    pub fn discard(pipeline: *Pipeline) !void {
        if (pipeline.pending == 0) return error.NoPendingResults;
        if (pipeline.pending_in_batch == 0) return error.PendingFlush;
        pipeline.pending -= 1;
        return switch (pipeline.opts.protocol) {
            .simple => {
                pipeline.pending_in_batch -= 1;
                try pipeline.conn.discardPipelineSimpleQueryResult(pipeline.pending_in_batch == 0);
            },
            .extended => {
                pipeline.pending_in_batch -= 1;
                try pipeline.conn.discardPipelineExtendedResult(pipeline.pending_in_batch == 0);
            },
        };
    }
};

const TlsState = struct {
    base_reader: std.Io.net.Stream.Reader,
    base_writer: std.Io.net.Stream.Writer,
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
    reader_override: ?std.Io.Reader = null,
    read_buffer: []u8,
    write_buffer: []u8,
    queued_writes: std.ArrayList(u8) = .empty,
    queued_simple_pipeline_sql: std.ArrayList(u8) = .empty,
    tls: ?*TlsState = null,
    message_buffer: []u8 = &.{},
    tx_status: u8 = 'I',
    healthy: bool = true,
    last_error: ?proto.ErrorResponse = null,
    next_statement_id: u64 = 0,
    unnamed_statement_sql: []u8 = &.{},
    unnamed_statement_param_types: []u32 = &.{},
    pipeline_statement_sql: []u8 = &.{},
    pipeline_statement_param_types: []u32 = &.{},

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
        conn.clearUnnamedStatementCache();
        conn.clearPipelineStatementCache();
        conn.queued_writes.clearRetainingCapacity();
        conn.queued_simple_pipeline_sql.clearRetainingCapacity();
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
        conn.queued_writes.deinit(allocator);
        conn.queued_simple_pipeline_sql.deinit(allocator);
    }

    pub fn query(conn: *Conn, allocator: std.mem.Allocator, sql: []const u8) !Result {
        return conn.queryOpts(allocator, sql, .{});
    }

    pub fn pipeline(conn: *Conn, opts: QueryOptions) Pipeline {
        return Pipeline.init(conn, opts);
    }

    pub fn queueSimpleQuery(conn: *Conn, sql: []const u8) !void {
        conn.clearLastError();
        conn.clearUnnamedStatementCache();
        try conn.writeSimpleQueryBuffered(sql);
    }

    pub fn queuePipelineSimpleQuery(conn: *Conn, sql: []const u8) !void {
        conn.clearLastError();
        conn.clearUnnamedStatementCache();
        try conn.queued_simple_pipeline_sql.appendSlice(conn.allocator, sql);
        try conn.queued_simple_pipeline_sql.append(conn.allocator, ';');
    }

    pub fn flushPipelineSimpleQueries(conn: *Conn) !void {
        if (conn.queued_simple_pipeline_sql.items.len == 0) return;
        try conn.writeSimpleQueryBuffered(conn.queued_simple_pipeline_sql.items);
        conn.queued_simple_pipeline_sql.clearRetainingCapacity();
        try conn.flushWrites();
    }

    pub fn readSimpleQueryResult(conn: *Conn, allocator: std.mem.Allocator) !Result {
        return conn.readQueryResult(allocator, false);
    }

    pub fn readPipelineSimpleQueryResult(conn: *Conn, allocator: std.mem.Allocator, expect_ready: bool) !Result {
        return conn.readQueryResultMode(allocator, false, expect_ready);
    }

    pub fn readSimpleExecResult(conn: *Conn, allocator: std.mem.Allocator) ![]const u8 {
        return conn.readExecResult(allocator, false);
    }

    pub fn readPipelineSimpleExecResult(conn: *Conn, allocator: std.mem.Allocator, expect_ready: bool) ![]const u8 {
        return conn.readExecResultMode(allocator, false, expect_ready);
    }

    pub fn discardSimpleQueryResult(conn: *Conn) !void {
        var failed = false;
        while (true) {
            const msg = try conn.readMessageHeader();
            switch (msg.tag) {
                'T', 'D', 'C', 'I', 'S', 'N', 'A' => try conn.skipMessagePayload(msg.payload_len),
                'E' => {
                    const payload = try conn.readMessagePayload(msg.payload_len);
                    failed = true;
                    conn.setLastError(payload) catch return conn.protocolError();
                },
                'Z' => {
                    const payload = try conn.readMessagePayload(msg.payload_len);
                    try conn.finishReady(payload);
                    if (failed) return error.PgServer;
                    return;
                },
                else => return conn.protocolError(),
            }
        }
    }

    pub fn discardPipelineSimpleQueryResult(conn: *Conn, expect_ready: bool) !void {
        var failed = false;
        while (true) {
            const msg = try conn.readMessageHeader();
            switch (msg.tag) {
                'T', 'D', 'S', 'N', 'A' => try conn.skipMessagePayload(msg.payload_len),
                'C', 'I' => {
                    try conn.skipMessagePayload(msg.payload_len);
                    if (!expect_ready) {
                        if (failed) return error.PgServer;
                        return;
                    }
                },
                'E' => {
                    const payload = try conn.readMessagePayload(msg.payload_len);
                    failed = true;
                    conn.setLastError(payload) catch return conn.protocolError();
                },
                'Z' => {
                    const payload = try conn.readMessagePayload(msg.payload_len);
                    try conn.finishReady(payload);
                    if (failed) return error.PgServer;
                    return;
                },
                else => return conn.protocolError(),
            }
        }
    }

    pub fn discardExtendedResult(conn: *Conn) !void {
        var failed = false;
        var saw_extended_control = false;
        while (true) {
            const msg = try conn.readMessageHeader();
            switch (msg.tag) {
                '1', '2' => {
                    saw_extended_control = true;
                    try conn.skipMessagePayload(msg.payload_len);
                },
                'T', 'D', 'C', 'I', 'n', 'S', 'N', 't', 'A', 's' => try conn.skipMessagePayload(msg.payload_len),
                'E' => {
                    const payload = try conn.readMessagePayload(msg.payload_len);
                    failed = true;
                    conn.setLastError(payload) catch return conn.protocolError();
                },
                'Z' => {
                    const payload = try conn.readMessagePayload(msg.payload_len);
                    try conn.finishReady(payload);
                    if (!saw_extended_control) return conn.protocolError();
                    if (failed) return error.PgServer;
                    return;
                },
                else => return conn.protocolError(),
            }
        }
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
        conn.clearUnnamedStatementCache();
        try conn.writeSimpleQueryBuffered(sql);
        try conn.flushWrites();
        return conn.readQueryResult(allocator, false);
    }

    fn execSimple(conn: *Conn, allocator: std.mem.Allocator, sql: []const u8) ![]const u8 {
        conn.clearLastError();
        conn.clearUnnamedStatementCache();
        try conn.writeSimpleQueryBuffered(sql);
        try conn.flushWrites();
        return conn.readExecResult(allocator, false);
    }

    fn queryExtendedValues(conn: *Conn, statement_name: ?[]const u8, allocator: std.mem.Allocator, sql: []const u8, values: []const Value, opts: QueryOptions) !Result {
        if (statement_name) |name| {
            return conn.queryPreparedValues(name, allocator, values, opts);
        }
        return conn.queryExtendedDirect(allocator, sql, values, opts);
    }

    fn execExtendedValues(conn: *Conn, statement_name: ?[]const u8, allocator: std.mem.Allocator, sql: []const u8, values: []const Value, opts: QueryOptions) ![]const u8 {
        if (statement_name) |name| {
            return conn.execPreparedValues(name, allocator, values, opts);
        }
        return conn.execExtendedDirect(allocator, sql, values, opts);
    }

    fn queryCompiled(conn: *Conn, allocator: std.mem.Allocator, sql: []const u8, args: anytype, static_param_types: []const u32, opts: QueryOptions) !Result {
        conn.clearLastError();
        try conn.sendCompiledExtendedQuery("", sql, args, static_param_types, opts.result_format, true);
        return conn.readQueryResult(allocator, true);
    }

    fn execCompiled(conn: *Conn, allocator: std.mem.Allocator, sql: []const u8, args: anytype, static_param_types: []const u32, opts: QueryOptions) ![]const u8 {
        conn.clearLastError();
        try conn.sendCompiledExtendedQuery("", sql, args, static_param_types, opts.result_format, true);
        return conn.readExecResult(allocator, true);
    }

    fn queryExtendedDirect(conn: *Conn, allocator: std.mem.Allocator, sql: []const u8, values: []const Value, opts: QueryOptions) !Result {
        conn.clearLastError();
        try conn.sendValueExtendedQuery("", sql, values, opts.result_format, true);
        return conn.readQueryResult(allocator, true);
    }

    fn queryPreparedValues(conn: *Conn, statement_name: []const u8, allocator: std.mem.Allocator, values: []const Value, opts: QueryOptions) !Result {
        conn.clearLastError();
        try conn.sendValueExtendedExecute(statement_name, values, opts.result_format);
        return conn.readQueryResult(allocator, true);
    }

    fn execExtendedDirect(conn: *Conn, allocator: std.mem.Allocator, sql: []const u8, values: []const Value, opts: QueryOptions) ![]const u8 {
        conn.clearLastError();
        try conn.sendValueExtendedQuery("", sql, values, opts.result_format, true);
        return conn.readExecResult(allocator, true);
    }

    fn execPreparedValues(conn: *Conn, statement_name: []const u8, allocator: std.mem.Allocator, values: []const Value, opts: QueryOptions) ![]const u8 {
        conn.clearLastError();
        try conn.sendValueExtendedExecute(statement_name, values, opts.result_format);
        return conn.readExecResult(allocator, true);
    }

    fn queryExtendedRaw(conn: *Conn, allocator: std.mem.Allocator, sql: []const u8, params: []const proto.Param, opts: QueryOptions) !Result {
        return conn.queryExtendedRawWithParamTypes(allocator, sql, params, null, opts);
    }

    fn queryExtendedRawWithParamTypes(conn: *Conn, allocator: std.mem.Allocator, sql: []const u8, params: []const proto.Param, param_types_override: ?[]const u32, opts: QueryOptions) !Result {
        conn.clearLastError();
        try conn.sendExtendedQuery("", sql, params, param_types_override, opts.result_format, true);
        return conn.readQueryResult(allocator, true);
    }

    fn queryPreparedRaw(conn: *Conn, statement_name: []const u8, allocator: std.mem.Allocator, params: []const proto.Param, opts: QueryOptions) !Result {
        conn.clearLastError();
        try conn.sendExtendedExecute(statement_name, params, opts.result_format);
        return conn.readQueryResult(allocator, true);
    }

    fn execExtendedRaw(conn: *Conn, allocator: std.mem.Allocator, sql: []const u8, params: []const proto.Param, opts: QueryOptions) ![]const u8 {
        return conn.execExtendedRawWithParamTypes(allocator, sql, params, null, opts);
    }

    fn execExtendedRawWithParamTypes(conn: *Conn, allocator: std.mem.Allocator, sql: []const u8, params: []const proto.Param, param_types_override: ?[]const u32, opts: QueryOptions) ![]const u8 {
        conn.clearLastError();
        try conn.sendExtendedQuery("", sql, params, param_types_override, opts.result_format, true);
        return conn.readExecResult(allocator, true);
    }

    fn execPreparedRaw(conn: *Conn, statement_name: []const u8, allocator: std.mem.Allocator, params: []const proto.Param, opts: QueryOptions) ![]const u8 {
        conn.clearLastError();
        try conn.sendExtendedExecute(statement_name, params, opts.result_format);
        return conn.readExecResult(allocator, true);
    }

    fn sendExtendedQuery(conn: *Conn, statement_name: []const u8, sql: []const u8, params: []const proto.Param, param_types_override: ?[]const u32, result_format: proto.FormatCode, include_parse: bool) !void {
        try conn.queueExtendedQuery(statement_name, sql, params, param_types_override, result_format, include_parse);
        try conn.flushWrites();
    }

    fn sendValueExtendedQuery(conn: *Conn, statement_name: []const u8, sql: []const u8, values: []const Value, result_format: proto.FormatCode, include_parse: bool) !void {
        try conn.queueValueExtendedQuery(statement_name, sql, values, result_format, include_parse);
        try conn.flushWrites();
    }

    fn sendCompiledExtendedQuery(conn: *Conn, statement_name: []const u8, sql: []const u8, args: anytype, static_param_types: []const u32, result_format: proto.FormatCode, include_parse: bool) !void {
        try conn.queueCompiledExtendedQuery(statement_name, sql, args, static_param_types, result_format, include_parse);
        try conn.flushWrites();
    }

    fn queueExtendedQuery(conn: *Conn, statement_name: []const u8, sql: []const u8, params: []const proto.Param, param_types_override: ?[]const u32, result_format: proto.FormatCode, include_parse: bool) !void {
        const cache_unnamed = statement_name.len == 0 and include_parse;
        var param_types: []u32 = &.{};
        var need_parse = include_parse;
        var parse_param_types: []const u32 = &.{};
        if (include_parse) {
            if (param_types_override) |explicit| {
                param_types = try conn.allocator.dupe(u32, explicit);
                defer conn.allocator.free(param_types);
            } else {
                param_types = try collectParamTypes(conn.allocator, params);
                defer conn.allocator.free(param_types);
            }
            if (cache_unnamed) {
                need_parse = !conn.matchesUnnamedStatement(sql, param_types);
            }
            parse_param_types = if (cache_unnamed and param_types_override == null) &.{} else param_types;
        }
        const start = conn.queued_writes.items.len;
        errdefer conn.queued_writes.items.len = start;
        try conn.queued_writes.ensureUnusedCapacity(conn.allocator, extendedQueryMessageLenFor("", statement_name, sql, params, parse_param_types.len, need_parse, true));
        if (need_parse) {
            try proto.appendParse(&conn.queued_writes, conn.allocator, statement_name, sql, parse_param_types);
        }
        try proto.appendBind(&conn.queued_writes, conn.allocator, "", statement_name, params, result_format);
        try proto.appendExecute(&conn.queued_writes, conn.allocator, "", 0);
        try proto.appendSync(&conn.queued_writes, conn.allocator);
        if (cache_unnamed and need_parse) try conn.updateUnnamedStatementCache(sql, param_types);
    }

    fn queueValueExtendedQuery(conn: *Conn, statement_name: []const u8, sql: []const u8, values: []const Value, result_format: proto.FormatCode, include_parse: bool) !void {
        const cache_unnamed = statement_name.len == 0 and include_parse;
        var need_parse = include_parse;
        if (cache_unnamed) {
            need_parse = !conn.matchesUnnamedStatementValues(sql, values);
        }
        const parse_type_count = if (need_parse) values.len else 0;
        const start = conn.queued_writes.items.len;
        errdefer conn.queued_writes.items.len = start;
        try conn.queued_writes.ensureUnusedCapacity(conn.allocator, try valueExtendedQueryMessageLenFor("", statement_name, sql, values, parse_type_count, need_parse, true));
        if (need_parse) {
            appendParseValuesAssumeCapacity(&conn.queued_writes, statement_name, sql, values);
        }
        try appendBindValuesAssumeCapacity(&conn.queued_writes, "", statement_name, values, result_format);
        appendExecuteAssumeCapacity(&conn.queued_writes, "", 0);
        appendSyncAssumeCapacity(&conn.queued_writes);
        if (cache_unnamed and need_parse) try conn.updateUnnamedStatementCacheFromValues(sql, values);
    }

    fn queueCompiledExtendedQuery(conn: *Conn, statement_name: []const u8, sql: []const u8, args: anytype, static_param_types: []const u32, result_format: proto.FormatCode, include_parse: bool) !void {
        const cache_unnamed = statement_name.len == 0 and include_parse;
        var need_parse = include_parse;
        if (cache_unnamed) {
            need_parse = !conn.matchesUnnamedStatement(sql, static_param_types);
        }
        const parse_type_count = if (need_parse) static_param_types.len else 0;
        const start = conn.queued_writes.items.len;
        errdefer conn.queued_writes.items.len = start;
        try conn.queued_writes.ensureUnusedCapacity(conn.allocator, try compiledExtendedQueryMessageLenFor("", statement_name, sql, args, parse_type_count, need_parse, true));
        if (need_parse) {
            appendParseStaticTypesAssumeCapacity(&conn.queued_writes, statement_name, sql, static_param_types);
        }
        try appendBindCompiledArgsAssumeCapacity(&conn.queued_writes, "", statement_name, args, result_format);
        appendExecuteAssumeCapacity(&conn.queued_writes, "", 0);
        appendSyncAssumeCapacity(&conn.queued_writes);
        if (cache_unnamed and need_parse) try conn.updateUnnamedStatementCache(sql, static_param_types);
    }

    fn queuePipelineParameterizedQuery(conn: *Conn, sql: []const u8, params: []const proto.Param, param_types_override: ?[]const u32, result_format: proto.FormatCode, portal_id: u64) !void {
        const statement_name = "s";
        var owned_param_types: []u32 = &.{};
        if (param_types_override) |explicit| {
            owned_param_types = try conn.allocator.dupe(u32, explicit);
        } else {
            owned_param_types = try collectParamTypes(conn.allocator, params);
        }
        defer conn.allocator.free(owned_param_types);

        var portal_buf: [18]u8 = undefined;
        const portal_name = try std.fmt.bufPrint(&portal_buf, "p{x}", .{portal_id});
        const needs_parse = !conn.matchesPipelineStatement(sql, owned_param_types);

        const start = conn.queued_writes.items.len;
        errdefer conn.queued_writes.items.len = start;
        try conn.queued_writes.ensureUnusedCapacity(conn.allocator, extendedQueryMessageLenFor(portal_name, statement_name, sql, params, owned_param_types.len, needs_parse, false) + closeMessageLen(statement_name, needs_parse and conn.pipeline_statement_sql.len != 0));

        if (needs_parse and conn.pipeline_statement_sql.len != 0) {
            try proto.appendClose(&conn.queued_writes, conn.allocator, .statement, statement_name);
        }
        if (needs_parse) {
            try proto.appendParse(&conn.queued_writes, conn.allocator, statement_name, sql, owned_param_types);
        }
        try proto.appendBind(&conn.queued_writes, conn.allocator, portal_name, statement_name, params, result_format);
        try proto.appendExecute(&conn.queued_writes, conn.allocator, portal_name, 0);
        if (needs_parse) try conn.updatePipelineStatementCache(sql, owned_param_types);
    }

    fn queuePipelineValueQuery(conn: *Conn, sql: []const u8, values: []const Value, result_format: proto.FormatCode, portal_id: u64) !void {
        const statement_name = "s";
        var portal_buf: [18]u8 = undefined;
        const portal_name = try std.fmt.bufPrint(&portal_buf, "p{x}", .{portal_id});
        const needs_parse = !conn.matchesPipelineStatementValues(sql, values);

        const start = conn.queued_writes.items.len;
        errdefer conn.queued_writes.items.len = start;
        try conn.queued_writes.ensureUnusedCapacity(conn.allocator, try valueExtendedQueryMessageLenFor(portal_name, statement_name, sql, values, if (needs_parse) values.len else 0, needs_parse, false) + closeMessageLen(statement_name, needs_parse and conn.pipeline_statement_sql.len != 0));

        if (needs_parse and conn.pipeline_statement_sql.len != 0) {
            appendCloseAssumeCapacity(&conn.queued_writes, .statement, statement_name);
        }
        if (needs_parse) {
            appendParseValuesAssumeCapacity(&conn.queued_writes, statement_name, sql, values);
        }
        try appendBindValuesAssumeCapacity(&conn.queued_writes, portal_name, statement_name, values, result_format);
        appendExecuteAssumeCapacity(&conn.queued_writes, portal_name, 0);
        if (needs_parse) try conn.updatePipelineStatementCacheFromValues(sql, values);
    }

    fn queuePipelineCompiledQuery(conn: *Conn, sql: []const u8, args: anytype, static_param_types: []const u32, result_format: proto.FormatCode, portal_id: u64) !void {
        const statement_name = "s";
        var portal_buf: [18]u8 = undefined;
        const portal_name = try std.fmt.bufPrint(&portal_buf, "p{x}", .{portal_id});
        const needs_parse = !conn.matchesPipelineStatement(sql, static_param_types);

        const start = conn.queued_writes.items.len;
        errdefer conn.queued_writes.items.len = start;
        try conn.queued_writes.ensureUnusedCapacity(conn.allocator, try compiledExtendedQueryMessageLenFor(portal_name, statement_name, sql, args, if (needs_parse) static_param_types.len else 0, needs_parse, false) + closeMessageLen(statement_name, needs_parse and conn.pipeline_statement_sql.len != 0));

        if (needs_parse and conn.pipeline_statement_sql.len != 0) {
            appendCloseAssumeCapacity(&conn.queued_writes, .statement, statement_name);
        }
        if (needs_parse) {
            appendParseStaticTypesAssumeCapacity(&conn.queued_writes, statement_name, sql, static_param_types);
        }
        try appendBindCompiledArgsAssumeCapacity(&conn.queued_writes, portal_name, statement_name, args, result_format);
        appendExecuteAssumeCapacity(&conn.queued_writes, portal_name, 0);
        if (needs_parse) try conn.updatePipelineStatementCache(sql, static_param_types);
    }

    fn queuePipelineCachedQuery(conn: *Conn, sql: []const u8, result_format: proto.FormatCode, portal_id: u64) !void {
        try conn.queuePipelineParameterizedQuery(sql, &.{}, &.{}, result_format, portal_id);
    }

    fn sendExtendedExecute(conn: *Conn, statement_name: []const u8, params: []const proto.Param, result_format: proto.FormatCode) !void {
        try conn.queueExtendedExecute(statement_name, params, result_format);
        try conn.flushWrites();
    }

    fn sendValueExtendedExecute(conn: *Conn, statement_name: []const u8, values: []const Value, result_format: proto.FormatCode) !void {
        try conn.queueValueExtendedExecute(statement_name, values, result_format);
        try conn.flushWrites();
    }

    fn queueExtendedExecute(conn: *Conn, statement_name: []const u8, params: []const proto.Param, result_format: proto.FormatCode) !void {
        const start = conn.queued_writes.items.len;
        errdefer conn.queued_writes.items.len = start;
        try conn.queued_writes.ensureUnusedCapacity(conn.allocator, extendedExecuteMessageLenFor("", statement_name, params, true));
        try proto.appendBind(&conn.queued_writes, conn.allocator, "", statement_name, params, result_format);
        try proto.appendExecute(&conn.queued_writes, conn.allocator, "", 0);
        try proto.appendSync(&conn.queued_writes, conn.allocator);
    }

    fn queueValueExtendedExecute(conn: *Conn, statement_name: []const u8, values: []const Value, result_format: proto.FormatCode) !void {
        const start = conn.queued_writes.items.len;
        errdefer conn.queued_writes.items.len = start;
        try conn.queued_writes.ensureUnusedCapacity(conn.allocator, try valueExtendedExecuteMessageLenFor("", statement_name, values, true));
        try appendBindValuesAssumeCapacity(&conn.queued_writes, "", statement_name, values, result_format);
        appendExecuteAssumeCapacity(&conn.queued_writes, "", 0);
        appendSyncAssumeCapacity(&conn.queued_writes);
    }

    fn queuePipelinePreparedExecute(conn: *Conn, statement_name: []const u8, params: []const proto.Param, result_format: proto.FormatCode, portal_id: u64) !void {
        var portal_buf: [18]u8 = undefined;
        const portal_name = try std.fmt.bufPrint(&portal_buf, "p{x}", .{portal_id});
        const start = conn.queued_writes.items.len;
        errdefer conn.queued_writes.items.len = start;
        try conn.queued_writes.ensureUnusedCapacity(conn.allocator, extendedExecuteMessageLenFor(portal_name, statement_name, params, false));
        try proto.appendBind(&conn.queued_writes, conn.allocator, portal_name, statement_name, params, result_format);
        try proto.appendExecute(&conn.queued_writes, conn.allocator, portal_name, 0);
    }

    fn queuePipelinePreparedValueExecute(conn: *Conn, statement_name: []const u8, values: []const Value, result_format: proto.FormatCode, portal_id: u64) !void {
        var portal_buf: [18]u8 = undefined;
        const portal_name = try std.fmt.bufPrint(&portal_buf, "p{x}", .{portal_id});
        const start = conn.queued_writes.items.len;
        errdefer conn.queued_writes.items.len = start;
        try conn.queued_writes.ensureUnusedCapacity(conn.allocator, try valueExtendedExecuteMessageLenFor(portal_name, statement_name, values, false));
        try appendBindValuesAssumeCapacity(&conn.queued_writes, portal_name, statement_name, values, result_format);
        appendExecuteAssumeCapacity(&conn.queued_writes, portal_name, 0);
    }

    fn readQueryResult(conn: *Conn, allocator: std.mem.Allocator, extended: bool) !Result {
        return conn.readQueryResultMode(allocator, extended, true);
    }

    fn readPipelineQueryResult(conn: *Conn, allocator: std.mem.Allocator, expect_ready: bool) !Result {
        return conn.readQueryResultMode(allocator, true, expect_ready);
    }

    fn readQueryResultMode(conn: *Conn, allocator: std.mem.Allocator, extended: bool, expect_ready: bool) !Result {
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
                'C' => {
                    command_tag = try arena.dupe(u8, try parseCommandComplete(msg.payload));
                    if (!expect_ready) {
                        if (!saw_extended_control) return conn.protocolError();
                        result.columns = try columns.toOwnedSlice(arena);
                        result.rows = try rows.toOwnedSlice(arena);
                        result.command_tag = command_tag;
                        if (failed) return error.PgServer;
                        return result;
                    }
                },
                '3', 'n', 't', 'A', 's', 'S', 'N' => {},
                'I' => {
                    if (!expect_ready) {
                        if (!saw_extended_control) return conn.protocolError();
                        result.columns = try columns.toOwnedSlice(arena);
                        result.rows = try rows.toOwnedSlice(arena);
                        result.command_tag = command_tag;
                        if (failed) return error.PgServer;
                        return result;
                    }
                },
                'E' => {
                    failed = true;
                    conn.setLastError(msg.payload) catch return conn.protocolError();
                },
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
        return conn.readExecResultMode(allocator, extended, true);
    }

    fn readPipelineExecResult(conn: *Conn, allocator: std.mem.Allocator, expect_ready: bool) ![]const u8 {
        return conn.readExecResultMode(allocator, true, expect_ready);
    }

    fn readExecResultMode(conn: *Conn, allocator: std.mem.Allocator, extended: bool, expect_ready: bool) ![]const u8 {
        var command_tag: ?[]const u8 = null;
        defer if (command_tag) |tag| allocator.free(tag);
        var failed = false;
        var saw_extended_control = !extended;
        while (true) {
            const msg = try conn.readMessage();
            switch (msg.tag) {
                '1', '2' => saw_extended_control = true,
                'C' => {
                    if (command_tag) |tag| allocator.free(tag);
                    command_tag = try allocator.dupe(u8, try parseCommandComplete(msg.payload));
                    if (!expect_ready) {
                        if (!saw_extended_control) return conn.protocolError();
                        if (failed) return error.PgServer;
                        const tag = command_tag.?;
                        command_tag = null;
                        return tag;
                    }
                },
                'T', 'D', '3', 'n', 'S', 'N', 't', 'A', 's' => {},
                'I' => {
                    if (!expect_ready) {
                        if (!saw_extended_control) return conn.protocolError();
                        if (failed) return error.PgServer;
                        if (command_tag) |tag| {
                            command_tag = null;
                            return tag;
                        }
                        return try allocator.dupe(u8, "");
                    }
                },
                'E' => {
                    failed = true;
                    conn.setLastError(msg.payload) catch return conn.protocolError();
                },
                'Z' => {
                    try conn.finishReady(msg.payload);
                    if (!saw_extended_control) return conn.protocolError();
                    if (failed) return error.PgServer;
                    if (command_tag) |tag| {
                        command_tag = null;
                        return tag;
                    }
                    return try allocator.dupe(u8, "");
                },
                else => return conn.protocolError(),
            }
        }
    }

    fn discardPipelineExtendedResult(conn: *Conn, expect_ready: bool) !void {
        var failed = false;
        var saw_extended_control = false;
        while (true) {
            const msg = try conn.readMessageHeader();
            switch (msg.tag) {
                '1', '2' => {
                    saw_extended_control = true;
                    try conn.skipMessagePayload(msg.payload_len);
                },
                'T', 'D', '3', 'n', 'S', 'N', 't', 'A', 's' => try conn.skipMessagePayload(msg.payload_len),
                'C', 'I' => {
                    try conn.skipMessagePayload(msg.payload_len);
                    if (!expect_ready) {
                        if (!saw_extended_control) return conn.protocolError();
                        if (failed) return error.PgServer;
                        return;
                    }
                },
                'E' => {
                    const payload = try conn.readMessagePayload(msg.payload_len);
                    failed = true;
                    conn.setLastError(payload) catch return conn.protocolError();
                },
                'Z' => {
                    const payload = try conn.readMessagePayload(msg.payload_len);
                    try conn.finishReady(payload);
                    if (!saw_extended_control) return conn.protocolError();
                    if (failed) return error.PgServer;
                    return;
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

    fn clearUnnamedStatementCache(conn: *Conn) void {
        if (conn.unnamed_statement_sql.len != 0) {
            conn.allocator.free(conn.unnamed_statement_sql);
            conn.unnamed_statement_sql = &.{};
        }
        if (conn.unnamed_statement_param_types.len != 0) {
            conn.allocator.free(conn.unnamed_statement_param_types);
            conn.unnamed_statement_param_types = &.{};
        }
    }

    fn clearPipelineStatementCache(conn: *Conn) void {
        if (conn.pipeline_statement_sql.len != 0) {
            conn.allocator.free(conn.pipeline_statement_sql);
            conn.pipeline_statement_sql = &.{};
        }
        if (conn.pipeline_statement_param_types.len != 0) {
            conn.allocator.free(conn.pipeline_statement_param_types);
            conn.pipeline_statement_param_types = &.{};
        }
    }

    fn matchesUnnamedStatement(conn: *const Conn, sql: []const u8, param_types: []const u32) bool {
        return std.mem.eql(u8, conn.unnamed_statement_sql, sql) and
            std.mem.eql(u32, conn.unnamed_statement_param_types, param_types);
    }

    fn matchesPipelineStatement(conn: *const Conn, sql: []const u8, param_types: []const u32) bool {
        return std.mem.eql(u8, conn.pipeline_statement_sql, sql) and
            std.mem.eql(u32, conn.pipeline_statement_param_types, param_types);
    }

    fn matchesUnnamedStatementValues(conn: *const Conn, sql: []const u8, values: []const Value) bool {
        if (!std.mem.eql(u8, conn.unnamed_statement_sql, sql)) return false;
        if (conn.unnamed_statement_param_types.len != values.len) return false;
        for (values, conn.unnamed_statement_param_types) |value, expected_oid| {
            if (expected_oid != runtimeValueTypeOid(value)) return false;
        }
        return true;
    }

    fn matchesPipelineStatementValues(conn: *const Conn, sql: []const u8, values: []const Value) bool {
        if (!std.mem.eql(u8, conn.pipeline_statement_sql, sql)) return false;
        if (conn.pipeline_statement_param_types.len != values.len) return false;
        for (values, conn.pipeline_statement_param_types) |value, expected_oid| {
            if (expected_oid != runtimeValueTypeOid(value)) return false;
        }
        return true;
    }

    fn updateUnnamedStatementCache(conn: *Conn, sql: []const u8, param_types: []const u32) !void {
        conn.clearUnnamedStatementCache();
        conn.unnamed_statement_sql = try conn.allocator.dupe(u8, sql);
        errdefer {
            conn.allocator.free(conn.unnamed_statement_sql);
            conn.unnamed_statement_sql = &.{};
        }
        conn.unnamed_statement_param_types = try conn.allocator.dupe(u32, param_types);
    }

    fn updatePipelineStatementCache(conn: *Conn, sql: []const u8, param_types: []const u32) !void {
        conn.clearPipelineStatementCache();
        conn.pipeline_statement_sql = try conn.allocator.dupe(u8, sql);
        errdefer {
            conn.allocator.free(conn.pipeline_statement_sql);
            conn.pipeline_statement_sql = &.{};
        }
        conn.pipeline_statement_param_types = try conn.allocator.dupe(u32, param_types);
    }

    fn updateUnnamedStatementCacheFromValues(conn: *Conn, sql: []const u8, values: []const Value) !void {
        conn.clearUnnamedStatementCache();
        conn.unnamed_statement_sql = try conn.allocator.dupe(u8, sql);
        errdefer {
            conn.allocator.free(conn.unnamed_statement_sql);
            conn.unnamed_statement_sql = &.{};
        }
        conn.unnamed_statement_param_types = try conn.allocator.alloc(u32, values.len);
        for (values, conn.unnamed_statement_param_types) |value, *oid| oid.* = runtimeValueTypeOid(value);
    }

    fn updatePipelineStatementCacheFromValues(conn: *Conn, sql: []const u8, values: []const Value) !void {
        conn.clearPipelineStatementCache();
        conn.pipeline_statement_sql = try conn.allocator.dupe(u8, sql);
        errdefer {
            conn.allocator.free(conn.pipeline_statement_sql);
            conn.pipeline_statement_sql = &.{};
        }
        conn.pipeline_statement_param_types = try conn.allocator.alloc(u32, values.len);
        for (values, conn.pipeline_statement_param_types) |value, *oid| oid.* = runtimeValueTypeOid(value);
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

    const MessageHeader = struct {
        tag: u8,
        payload_len: u32,
    };

    fn readMessageHeader(conn: *Conn) !MessageHeader {
        const reader = conn.currentReader();
        const tag = try reader.takeByte();
        const len = try reader.takeInt(u32, .big);
        if (len < 4 or len - 4 > conn.config.max_message_len) return error.InvalidMessageLength;
        return .{
            .tag = tag,
            .payload_len = len - 4,
        };
    }

    fn readMessagePayload(conn: *Conn, payload_len: u32) ![]u8 {
        if (conn.message_buffer.len < payload_len) {
            if (conn.message_buffer.len != 0) conn.allocator.free(conn.message_buffer);
            conn.message_buffer = try conn.allocator.alloc(u8, payload_len);
        }
        const payload = conn.message_buffer[0..payload_len];
        try conn.currentReader().readSliceAll(payload);
        return payload;
    }

    fn skipMessagePayload(conn: *Conn, payload_len: u32) !void {
        var remaining: usize = payload_len;
        var scratch: [256]u8 = undefined;
        while (remaining != 0) {
            const chunk_len = @min(remaining, scratch.len);
            try conn.currentReader().readSliceAll(scratch[0..chunk_len]);
            remaining -= chunk_len;
        }
    }

    fn currentReader(conn: *Conn) *std.Io.Reader {
        if (conn.reader_override) |*reader| return reader;
        if (conn.tls) |tls| return &tls.client.reader;
        return &conn.reader.interface;
    }

    fn writeMessages(conn: *Conn, bytes: []const u8) !void {
        try conn.queued_writes.appendSlice(conn.allocator, bytes);
        try conn.flushWrites();
    }

    fn writeSimpleQueryBuffered(conn: *Conn, sql: []const u8) !void {
        var header: [5]u8 = undefined;
        header[0] = 'Q';
        std.mem.writeInt(u32, header[1..5], @as(u32, @intCast(sql.len + 5)), .big);
        const zero = [_]u8{0};

        if (conn.tls) |tls| {
            try tls.client.writer.writeAll(&header);
            try tls.client.writer.writeAll(sql);
            try tls.client.writer.writeAll(&zero);
            return;
        }

        try conn.writer.interface.writeAll(&header);
        try conn.writer.interface.writeAll(sql);
        try conn.writer.interface.writeAll(&zero);
    }

    fn flushWrites(conn: *Conn) !void {
        if (conn.queued_writes.items.len != 0) {
            if (conn.tls) |tls| {
                try tls.client.writer.writeAll(conn.queued_writes.items);
            } else {
                try conn.writer.interface.writeAll(conn.queued_writes.items);
            }
            conn.queued_writes.clearRetainingCapacity();
        }
        if (conn.tls) |tls| {
            try tls.client.writer.flush();
            return;
        }
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
            'S' => {
                // Start TLS from a fresh stream reader/writer state after the
                // one-byte PostgreSQL SSL negotiation response.
                conn.reader = conn.stream.reader(conn.io, conn.read_buffer);
                conn.writer = conn.stream.writer(conn.io, conn.write_buffer);
                try conn.enableTls();
            },
            'N' => switch (conn.config.ssl_mode) {
                .prefer => {},
                else => return error.TlsUnsupported,
            },
            else => return error.ProtocolViolation,
        }
    }

    fn enableTls(conn: *Conn) !void {
        const tls = try conn.allocator.create(TlsState);
        tls.* = .{
            .base_reader = undefined,
            .base_writer = undefined,
            .client = undefined,
            .read_buffer = &.{},
            .write_buffer = &.{},
            .ca_bundle = null,
        };
        var tls_initialized = false;
        errdefer if (tls_initialized) {
            tls.deinit(conn.allocator);
        } else {
            if (tls.ca_bundle) |*bundle| bundle.deinit(conn.allocator);
            if (tls.read_buffer.len != 0) conn.allocator.free(tls.read_buffer);
            if (tls.write_buffer.len != 0) conn.allocator.free(tls.write_buffer);
            conn.allocator.destroy(tls);
        };

        tls.read_buffer = try conn.allocator.alloc(u8, TlsClient.min_buffer_len);
        tls.write_buffer = try conn.allocator.alloc(u8, TlsClient.min_buffer_len);

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

        tls.base_reader = conn.stream.reader(conn.io, conn.read_buffer);
        tls.base_writer = conn.stream.writer(conn.io, conn.write_buffer);
        var random_buffer: [TlsClient.Options.entropy_len]u8 = undefined;
        conn.io.random(&random_buffer);
        tls.client = TlsClient.init(
            &tls.base_reader.interface,
            &tls.base_writer.interface,
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
            error.WriteFailed => return tls.base_writer.err.?,
            error.ReadFailed => return tls.base_reader.err.?,
            else => return err,
        };
        tls_initialized = true;
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

fn appendAssumeCapacity(out: *std.ArrayList(u8), byte: u8) void {
    out.appendAssumeCapacity(byte);
}

fn appendSliceAssumeCapacity(out: *std.ArrayList(u8), bytes: []const u8) void {
    out.appendSliceAssumeCapacity(bytes);
}

fn appendIntAssumeCapacity(out: *std.ArrayList(u8), comptime T: type, value: T) void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .big);
    appendSliceAssumeCapacity(out, &bytes);
}

fn appendTaggedHeaderAssumeCapacity(out: *std.ArrayList(u8), tag: u8, payload_len: usize) void {
    appendAssumeCapacity(out, tag);
    appendIntAssumeCapacity(out, u32, @as(u32, @intCast(payload_len + 4)));
}

fn appendCStringAssumeCapacity(out: *std.ArrayList(u8), text: []const u8) void {
    appendSliceAssumeCapacity(out, text);
    appendAssumeCapacity(out, 0);
}

fn appendParseStaticTypesAssumeCapacity(out: *std.ArrayList(u8), statement_name: []const u8, sql: []const u8, param_types: []const u32) void {
    const payload_len = statement_name.len + 1 + sql.len + 1 + 2 + (4 * param_types.len);
    appendTaggedHeaderAssumeCapacity(out, 'P', payload_len);
    appendCStringAssumeCapacity(out, statement_name);
    appendCStringAssumeCapacity(out, sql);
    appendIntAssumeCapacity(out, u16, @as(u16, @intCast(param_types.len)));
    for (param_types) |oid| appendIntAssumeCapacity(out, u32, oid);
}

fn appendParseValuesAssumeCapacity(out: *std.ArrayList(u8), statement_name: []const u8, sql: []const u8, values: []const Value) void {
    const payload_len = statement_name.len + 1 + sql.len + 1 + 2 + (4 * values.len);
    appendTaggedHeaderAssumeCapacity(out, 'P', payload_len);
    appendCStringAssumeCapacity(out, statement_name);
    appendCStringAssumeCapacity(out, sql);
    appendIntAssumeCapacity(out, u16, @as(u16, @intCast(values.len)));
    for (values) |value| appendIntAssumeCapacity(out, u32, runtimeValueTypeOid(value));
}

fn appendExecuteAssumeCapacity(out: *std.ArrayList(u8), portal_name: []const u8, max_rows: u32) void {
    appendTaggedHeaderAssumeCapacity(out, 'E', portal_name.len + 1 + 4);
    appendCStringAssumeCapacity(out, portal_name);
    appendIntAssumeCapacity(out, u32, max_rows);
}

fn appendSyncAssumeCapacity(out: *std.ArrayList(u8)) void {
    appendTaggedHeaderAssumeCapacity(out, 'S', 0);
}

fn appendCloseAssumeCapacity(out: *std.ArrayList(u8), target: proto.CloseTarget, name: []const u8) void {
    appendTaggedHeaderAssumeCapacity(out, 'C', 1 + name.len + 1);
    appendAssumeCapacity(out, @intFromEnum(target));
    appendCStringAssumeCapacity(out, name);
}

fn runtimeValueTypeOid(value: Value) u32 {
    return switch (value) {
        .null => 0,
        .typed_null => |oid| oid,
        .bool => 16,
        .int2, .uint2 => 21,
        .int4, .uint4 => 23,
        .int8, .uint8 => 20,
        .float4 => 700,
        .float8 => 701,
        .text => 25,
        .bytea => 17,
        .raw_text => 0,
        .raw_binary => |v| v.type_oid,
    };
}

fn runtimeValueFormat(value: Value) proto.FormatCode {
    return switch (value) {
        .bool,
        .int2,
        .int4,
        .int8,
        .uint2,
        .uint4,
        .uint8,
        .float4,
        .float8,
        .bytea,
        .raw_binary,
        => .binary,
        else => .text,
    };
}

fn runtimeValueEncodedLen(value: Value) !?usize {
    return switch (value) {
        .null, .typed_null => null,
        .bool => 1,
        .int2, .uint2 => 2,
        .int4, .uint4, .float4 => 4,
        .int8, .uint8, .float8 => 8,
        .text => |v| v.len,
        .bytea => |v| v.len,
        .raw_text => |v| v.len,
        .raw_binary => |v| v.bytes.len,
    };
}

fn appendRuntimeValueBytesAssumeCapacity(out: *std.ArrayList(u8), value: Value) !void {
    switch (value) {
        .null, .typed_null => {},
        .bool => |v| appendAssumeCapacity(out, if (v) 1 else 0),
        .int2 => |v| appendIntAssumeCapacity(out, i16, v),
        .int4 => |v| appendIntAssumeCapacity(out, i32, v),
        .int8 => |v| appendIntAssumeCapacity(out, i64, v),
        .uint2 => |v| appendIntAssumeCapacity(out, i16, std.math.cast(i16, v) orelse return error.ValueOverflow),
        .uint4 => |v| appendIntAssumeCapacity(out, i32, std.math.cast(i32, v) orelse return error.ValueOverflow),
        .uint8 => |v| appendIntAssumeCapacity(out, i64, std.math.cast(i64, v) orelse return error.ValueOverflow),
        .float4 => |v| appendIntAssumeCapacity(out, u32, @bitCast(v)),
        .float8 => |v| appendIntAssumeCapacity(out, u64, @bitCast(v)),
        .text => |v| appendSliceAssumeCapacity(out, v),
        .bytea => |v| appendSliceAssumeCapacity(out, v),
        .raw_text => |v| appendSliceAssumeCapacity(out, v),
        .raw_binary => |v| appendSliceAssumeCapacity(out, v.bytes),
    }
}

fn valueBindMessageLen(portal_name: []const u8, statement_name: []const u8, values: []const Value) !usize {
    var payload_len: usize = portal_name.len + 1 + statement_name.len + 1;
    payload_len += 2 + (2 * values.len);
    payload_len += 2;
    for (values) |value| {
        payload_len += 4;
        if (try runtimeValueEncodedLen(value)) |len| payload_len += len;
    }
    payload_len += 2 + 2;
    return taggedMessageLen(payload_len);
}

fn valueExtendedQueryMessageLenFor(portal_name: []const u8, statement_name: []const u8, sql: []const u8, values: []const Value, parse_param_type_count: usize, include_parse: bool, include_sync: bool) !usize {
    var len = try valueExtendedExecuteMessageLenFor(portal_name, statement_name, values, include_sync);
    if (include_parse) len += taggedMessageLen(statement_name.len + 1 + sql.len + 1 + 2 + (4 * parse_param_type_count));
    return len;
}

fn valueExtendedExecuteMessageLenFor(portal_name: []const u8, statement_name: []const u8, values: []const Value, include_sync: bool) !usize {
    var len = try valueBindMessageLen(portal_name, statement_name, values) + executeMessageLen(portal_name);
    if (include_sync) len += syncMessageLen();
    return len;
}

fn appendBindValuesAssumeCapacity(out: *std.ArrayList(u8), portal_name: []const u8, statement_name: []const u8, values: []const Value, result_format: proto.FormatCode) !void {
    const payload_len = (try valueBindMessageLen(portal_name, statement_name, values)) - 5;
    appendTaggedHeaderAssumeCapacity(out, 'B', payload_len);
    appendCStringAssumeCapacity(out, portal_name);
    appendCStringAssumeCapacity(out, statement_name);
    appendIntAssumeCapacity(out, u16, @as(u16, @intCast(values.len)));
    for (values) |value| appendIntAssumeCapacity(out, u16, @intFromEnum(runtimeValueFormat(value)));
    appendIntAssumeCapacity(out, u16, @as(u16, @intCast(values.len)));
    for (values) |value| {
        if (try runtimeValueEncodedLen(value)) |len| {
            appendIntAssumeCapacity(out, i32, @as(i32, @intCast(len)));
            try appendRuntimeValueBytesAssumeCapacity(out, value);
        } else {
            appendIntAssumeCapacity(out, i32, -1);
        }
    }
    appendIntAssumeCapacity(out, u16, 1);
    appendIntAssumeCapacity(out, u16, @intFromEnum(result_format));
}

fn compiledArgFormat(comptime T: type) proto.FormatCode {
    if (T == [16]u8 or T == Date or T == Time or T == Timestamp) return .binary;
    return switch (@typeInfo(T)) {
        .optional => |opt| compiledArgFormat(opt.child),
        .bool, .int, .float => .binary,
        .pointer => .text,
        .array => .text,
        else => @compileError("unsupported compiled query argument type"),
    };
}

fn compiledArgEncodedLen(arg: anytype) !?usize {
    const T = @TypeOf(arg);
    if (T == [16]u8) return 16;
    if (T == Date) {
        try validateDate(arg.year, arg.month, arg.day);
        return 4;
    }
    if (T == Time) {
        try validateTime(arg.hour, arg.minute, arg.second);
        if (arg.microsecond >= std.time.us_per_s) return error.BadValue;
        return 8;
    }
    if (T == Timestamp) {
        try validateDate(arg.date.year, arg.date.month, arg.date.day);
        try validateTime(arg.time.hour, arg.time.minute, arg.time.second);
        if (arg.time.microsecond >= std.time.us_per_s) return error.BadValue;
        return 8;
    }

    return switch (@typeInfo(T)) {
        .optional => if (arg) |value| try compiledArgEncodedLen(value) else null,
        .bool => 1,
        .int => |info| switch (info.bits) {
            0...16 => 2,
            17...32 => 4,
            33...64 => 8,
            else => @compileError("compiled query integer arguments must fit within 64 bits"),
        },
        .float => |info| switch (info.bits) {
            32 => 4,
            64 => 8,
            else => @compileError("compiled query float arguments must be f32 or f64"),
        },
        .pointer => |info| switch (info.size) {
            .slice => if (info.child == u8) arg.len else @compileError("compiled query slice arguments must be []const u8 or []u8"),
            else => @compileError("compiled query pointer arguments must be slices"),
        },
        .array => |info| if (info.child == u8) arg.len else @compileError("compiled query array arguments must be [N]u8"),
        else => @compileError("unsupported compiled query argument type"),
    };
}

fn appendCompiledArgBytesAssumeCapacity(out: *std.ArrayList(u8), arg: anytype) !void {
    const T = @TypeOf(arg);
    if (T == [16]u8) {
        appendSliceAssumeCapacity(out, arg[0..]);
        return;
    }
    if (T == Date) {
        try validateDate(arg.year, arg.month, arg.day);
        appendIntAssumeCapacity(out, i32, @intCast(daysFromCivil(arg.year, arg.month, arg.day) - pg_unix_epoch_days));
        return;
    }
    if (T == Time) {
        try validateTime(arg.hour, arg.minute, arg.second);
        if (arg.microsecond >= std.time.us_per_s) return error.BadValue;
        const micros =
            @as(i64, arg.hour) * std.time.us_per_hour +
            @as(i64, arg.minute) * std.time.us_per_min +
            @as(i64, arg.second) * std.time.us_per_s +
            @as(i64, arg.microsecond);
        appendIntAssumeCapacity(out, i64, micros);
        return;
    }
    if (T == Timestamp) {
        try validateDate(arg.date.year, arg.date.month, arg.date.day);
        try validateTime(arg.time.hour, arg.time.minute, arg.time.second);
        if (arg.time.microsecond >= std.time.us_per_s) return error.BadValue;
        appendIntAssumeCapacity(out, i64, unixMicrosFromTimestamp(arg) - pg_unix_epoch_micros);
        return;
    }

    switch (@typeInfo(T)) {
        .optional => if (arg) |value| try appendCompiledArgBytesAssumeCapacity(out, value),
        .bool => appendAssumeCapacity(out, if (arg) 1 else 0),
        .int => |info| switch (info.bits) {
            0...16 => if (info.signedness == .signed)
                appendIntAssumeCapacity(out, i16, @as(i16, @intCast(arg)))
            else
                appendIntAssumeCapacity(out, i16, std.math.cast(i16, arg) orelse return error.ValueOverflow),
            17...32 => if (info.signedness == .signed)
                appendIntAssumeCapacity(out, i32, @as(i32, @intCast(arg)))
            else
                appendIntAssumeCapacity(out, i32, std.math.cast(i32, arg) orelse return error.ValueOverflow),
            33...64 => if (info.signedness == .signed)
                appendIntAssumeCapacity(out, i64, @as(i64, @intCast(arg)))
            else
                appendIntAssumeCapacity(out, i64, std.math.cast(i64, arg) orelse return error.ValueOverflow),
            else => @compileError("compiled query integer arguments must fit within 64 bits"),
        },
        .float => |info| switch (info.bits) {
            32 => appendIntAssumeCapacity(out, u32, @bitCast(arg)),
            64 => appendIntAssumeCapacity(out, u64, @bitCast(arg)),
            else => @compileError("compiled query float arguments must be f32 or f64"),
        },
        .pointer => |info| switch (info.size) {
            .slice => if (info.child == u8)
                appendSliceAssumeCapacity(out, arg)
            else
                @compileError("compiled query slice arguments must be []const u8 or []u8"),
            else => @compileError("compiled query pointer arguments must be slices"),
        },
        .array => |info| if (info.child == u8)
            appendSliceAssumeCapacity(out, arg[0..])
        else
            @compileError("compiled query array arguments must be [N]u8"),
        else => @compileError("unsupported compiled query argument type"),
    }
}

fn compiledBindMessageLen(portal_name: []const u8, statement_name: []const u8, args: anytype) !usize {
    const fields = std.meta.fields(@TypeOf(args));
    var payload_len: usize = portal_name.len + 1 + statement_name.len + 1;
    payload_len += 2 + (2 * fields.len);
    payload_len += 2;
    inline for (fields) |field| {
        payload_len += 4;
        if (try compiledArgEncodedLen(@field(args, field.name))) |len| payload_len += len;
    }
    payload_len += 2 + 2;
    return taggedMessageLen(payload_len);
}

fn compiledExtendedQueryMessageLenFor(portal_name: []const u8, statement_name: []const u8, sql: []const u8, args: anytype, parse_param_type_count: usize, include_parse: bool, include_sync: bool) !usize {
    var len = try compiledExtendedExecuteMessageLenFor(portal_name, statement_name, args, include_sync);
    if (include_parse) len += taggedMessageLen(statement_name.len + 1 + sql.len + 1 + 2 + (4 * parse_param_type_count));
    return len;
}

fn compiledExtendedExecuteMessageLenFor(portal_name: []const u8, statement_name: []const u8, args: anytype, include_sync: bool) !usize {
    var len = try compiledBindMessageLen(portal_name, statement_name, args) + executeMessageLen(portal_name);
    if (include_sync) len += syncMessageLen();
    return len;
}

fn appendBindCompiledArgsAssumeCapacity(out: *std.ArrayList(u8), portal_name: []const u8, statement_name: []const u8, args: anytype, result_format: proto.FormatCode) !void {
    const fields = std.meta.fields(@TypeOf(args));
    const payload_len = (try compiledBindMessageLen(portal_name, statement_name, args)) - 5;
    appendTaggedHeaderAssumeCapacity(out, 'B', payload_len);
    appendCStringAssumeCapacity(out, portal_name);
    appendCStringAssumeCapacity(out, statement_name);
    appendIntAssumeCapacity(out, u16, @as(u16, @intCast(fields.len)));
    inline for (fields) |field| {
        appendIntAssumeCapacity(out, u16, @intFromEnum(compiledArgFormat(field.type)));
    }
    appendIntAssumeCapacity(out, u16, @as(u16, @intCast(fields.len)));
    inline for (fields) |field| {
        if (try compiledArgEncodedLen(@field(args, field.name))) |len| {
            appendIntAssumeCapacity(out, i32, @as(i32, @intCast(len)));
            try appendCompiledArgBytesAssumeCapacity(out, @field(args, field.name));
        } else {
            appendIntAssumeCapacity(out, i32, -1);
        }
    }
    appendIntAssumeCapacity(out, u16, 1);
    appendIntAssumeCapacity(out, u16, @intFromEnum(result_format));
}

fn validateCompiledQuerySpec(comptime sql_text: []const u8, comptime Args: type, comptime RowT: type) void {
    comptime {
        const expected_params = sqlPlaceholderCount(sql_text);
        const actual_params = compiledArgCount(Args);
        if (expected_params != actual_params) {
            @compileError(std.fmt.comptimePrint(
                "compiled query placeholder count mismatch for `{s}`: expected {d}, got {d}",
                .{ sql_text, expected_params, actual_params },
            ));
        }
        switch (@typeInfo(RowT)) {
            .@"struct" => {},
            else => @compileError("compiled query row type must be a struct"),
        }
    }
}

fn inferCompiledProtocol(comptime sql_text: []const u8, comptime Args: type) QueryProtocol {
    _ = Args;
    return if (sqlPlaceholderCount(sql_text) == 0) .simple else .extended;
}

fn compiledArgCount(comptime Args: type) usize {
    return switch (@typeInfo(Args)) {
        .@"struct" => |info| info.fields.len,
        else => @compileError("compiled query args must be a tuple or struct"),
    };
}

fn compiledArgsHaveStaticTypes(comptime Args: type) bool {
    const fields = std.meta.fields(Args);
    inline for (fields) |field| {
        if (compiledStaticArgTypeOid(field.type) == null) return false;
    }
    return true;
}

fn compiledStaticParamTypes(comptime Args: type) [compiledArgCount(Args)]u32 {
    const fields = std.meta.fields(Args);
    var param_types: [fields.len]u32 = undefined;
    inline for (fields, 0..) |field, i| {
        param_types[i] = compiledStaticArgTypeOid(field.type) orelse 0;
    }
    return param_types;
}

fn compiledStaticArgTypeOid(comptime T: type) ?u32 {
    if (T == Value) return null;
    if (T == Date) return 1082;
    if (T == Time) return 1083;
    if (T == Timestamp) return 1114;
    if (T == [16]u8) return 2950;
    return switch (@typeInfo(T)) {
        .optional => |opt| compiledStaticArgTypeOid(opt.child),
        .bool => 16,
        .int => |info| switch (info.bits) {
            0...16 => 21,
            17...32 => 23,
            33...64 => 20,
            else => @compileError("compiled query integer arguments must fit within 64 bits"),
        },
        .float => |info| switch (info.bits) {
            32 => 700,
            64 => 701,
            else => @compileError("compiled query float arguments must be f32 or f64"),
        },
        .pointer => |info| switch (info.size) {
            .slice => if (info.child == u8) 25 else @compileError("compiled query slice arguments must be []const u8 or []u8"),
            else => @compileError("compiled query pointer arguments must be slices"),
        },
        .array => |info| if (info.child == u8) 25 else @compileError("compiled query array arguments must be [N]u8"),
        else => @compileError("unsupported compiled query argument type; use scalars, temporal/uuid types, byte/text slices, optionals, or zpg.Value"),
    };
}

fn sqlPlaceholderCount(comptime sql_text: []const u8) usize {
    var i: usize = 0;
    var max_index: usize = 0;
    while (i < sql_text.len) : (i += 1) {
        switch (sql_text[i]) {
            '\'' => i = skipSingleQuoted(sql_text, i),
            '"' => i = skipDoubleQuoted(sql_text, i),
            '-' => if (i + 1 < sql_text.len and sql_text[i + 1] == '-') {
                i = skipLineComment(sql_text, i);
            },
            '/' => if (i + 1 < sql_text.len and sql_text[i + 1] == '*') {
                i = skipBlockComment(sql_text, i);
            },
            '$' => {
                if (i + 1 < sql_text.len and std.ascii.isDigit(sql_text[i + 1])) {
                    var j = i + 1;
                    var value: usize = 0;
                    while (j < sql_text.len and std.ascii.isDigit(sql_text[j])) : (j += 1) {
                        value = value * 10 + (sql_text[j] - '0');
                    }
                    if (value > max_index) max_index = value;
                    i = j - 1;
                    continue;
                }
                if (dollarQuoteTagLen(sql_text, i)) |tag_len| {
                    i = skipDollarQuoted(sql_text, i, tag_len);
                }
            },
            else => {},
        }
    }
    return max_index;
}

fn skipSingleQuoted(comptime text: []const u8, start: usize) usize {
    var i = start + 1;
    while (i < text.len) : (i += 1) {
        if (text[i] != '\'') continue;
        if (i + 1 < text.len and text[i + 1] == '\'') {
            i += 1;
            continue;
        }
        return i;
    }
    return text.len;
}

fn skipDoubleQuoted(comptime text: []const u8, start: usize) usize {
    var i = start + 1;
    while (i < text.len) : (i += 1) {
        if (text[i] != '"') continue;
        if (i + 1 < text.len and text[i + 1] == '"') {
            i += 1;
            continue;
        }
        return i;
    }
    return text.len;
}

fn skipLineComment(comptime text: []const u8, start: usize) usize {
    var i = start + 2;
    while (i < text.len and text[i] != '\n' and text[i] != '\r') : (i += 1) {}
    return if (i == text.len) text.len else i - 1;
}

fn skipBlockComment(comptime text: []const u8, start: usize) usize {
    var depth: usize = 1;
    var i = start + 2;
    while (i + 1 < text.len) : (i += 1) {
        if (text[i] == '/' and text[i + 1] == '*') {
            depth += 1;
            i += 1;
            continue;
        }
        if (text[i] == '*' and text[i + 1] == '/') {
            depth -= 1;
            if (depth == 0) return i + 1;
            i += 1;
        }
    }
    return text.len;
}

fn dollarQuoteTagLen(comptime text: []const u8, start: usize) ?usize {
    if (text[start] != '$') return null;
    var i = start + 1;
    while (i < text.len and text[i] != '$') : (i += 1) {
        if (!(std.ascii.isAlphanumeric(text[i]) or text[i] == '_')) return null;
    }
    if (i >= text.len or text[i] != '$') return null;
    return i - start + 1;
}

fn skipDollarQuoted(comptime text: []const u8, start: usize, tag_len: usize) usize {
    const tag = text[start .. start + tag_len];
    var i = start + tag_len;
    while (i + tag_len <= text.len) : (i += 1) {
        if (std.mem.eql(u8, text[i .. i + tag_len], tag)) {
            return i + tag_len - 1;
        }
    }
    return text.len;
}

fn encodeCompiledParams(allocator: std.mem.Allocator, args: anytype) ![]proto.Param {
    const Args = @TypeOf(args);
    const fields = std.meta.fields(Args);
    const params = try allocator.alloc(proto.Param, fields.len);
    inline for (fields, 0..) |field, i| {
        params[i] = try compiledArgToParam(allocator, @field(args, field.name));
    }
    return params;
}

fn compiledArgToParam(allocator: std.mem.Allocator, arg: anytype) !proto.Param {
    const T = @TypeOf(arg);
    if (T == Value) return switch (arg) {
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
    if (T == [16]u8) return .{ .type_oid = 2950, .format = .binary, .value = try allocator.dupe(u8, arg[0..]) };
    if (T == Date) {
        try validateDate(arg.year, arg.month, arg.day);
        var bytes = try allocator.alloc(u8, 4);
        std.mem.writeInt(i32, bytes[0..4], @intCast(daysFromCivil(arg.year, arg.month, arg.day) - pg_unix_epoch_days), .big);
        return .{ .type_oid = 1082, .format = .binary, .value = bytes };
    }
    if (T == Time) {
        try validateTime(arg.hour, arg.minute, arg.second);
        if (arg.microsecond >= std.time.us_per_s) return error.BadValue;
        var bytes = try allocator.alloc(u8, 8);
        const micros =
            @as(i64, arg.hour) * std.time.us_per_hour +
            @as(i64, arg.minute) * std.time.us_per_min +
            @as(i64, arg.second) * std.time.us_per_s +
            @as(i64, arg.microsecond);
        std.mem.writeInt(i64, bytes[0..8], micros, .big);
        return .{ .type_oid = 1083, .format = .binary, .value = bytes };
    }
    if (T == Timestamp) {
        try validateDate(arg.date.year, arg.date.month, arg.date.day);
        try validateTime(arg.time.hour, arg.time.minute, arg.time.second);
        if (arg.time.microsecond >= std.time.us_per_s) return error.BadValue;
        var bytes = try allocator.alloc(u8, 8);
        std.mem.writeInt(i64, bytes[0..8], unixMicrosFromTimestamp(arg) - pg_unix_epoch_micros, .big);
        return .{ .type_oid = 1114, .format = .binary, .value = bytes };
    }

    return switch (@typeInfo(T)) {
        .optional => if (arg) |value|
            try compiledArgToParam(allocator, value)
        else
            .{ .value = null },
        .bool => .{ .type_oid = 16, .value = if (arg) "t" else "f" },
        .int => |info| switch (info.bits) {
            0...16 => if (info.signedness == .signed)
                .{ .type_oid = 21, .value = try std.fmt.allocPrint(allocator, "{d}", .{@as(i16, @intCast(arg))}) }
            else
                .{ .type_oid = 21, .value = try std.fmt.allocPrint(allocator, "{d}", .{@as(u16, @intCast(arg))}) },
            17...32 => if (info.signedness == .signed)
                .{ .type_oid = 23, .value = try std.fmt.allocPrint(allocator, "{d}", .{@as(i32, @intCast(arg))}) }
            else
                .{ .type_oid = 23, .value = try std.fmt.allocPrint(allocator, "{d}", .{@as(u32, @intCast(arg))}) },
            33...64 => if (info.signedness == .signed)
                .{ .type_oid = 20, .value = try std.fmt.allocPrint(allocator, "{d}", .{@as(i64, @intCast(arg))}) }
            else
                .{ .type_oid = 20, .value = try std.fmt.allocPrint(allocator, "{d}", .{@as(u64, @intCast(arg))}) },
            else => @compileError("compiled query integer arguments must fit within 64 bits"),
        },
        .float => |info| switch (info.bits) {
            32 => .{ .type_oid = 700, .value = try std.fmt.allocPrint(allocator, "{}", .{arg}) },
            64 => .{ .type_oid = 701, .value = try std.fmt.allocPrint(allocator, "{}", .{arg}) },
            else => @compileError("compiled query float arguments must be f32 or f64"),
        },
        .pointer => |info| switch (info.size) {
            .slice => if (info.child == u8) .{ .type_oid = 25, .value = arg } else @compileError("compiled query slice arguments must be []const u8, []u8, or zpg.Value"),
            else => @compileError("compiled query pointer arguments must be slices"),
        },
        .array => |info| if (info.child == u8) .{ .type_oid = 25, .value = arg[0..] } else @compileError("compiled query array arguments must be [N]u8"),
        else => @compileError("unsupported compiled query argument type; use scalars, byte/text slices, optionals, or zpg.Value"),
    };
}

fn materializeCompiledResult(comptime RowT: type, raw: Result) !CompiledResult(RowT) {
    var owned_raw = raw;
    const fields = std.meta.fields(RowT);
    if (owned_raw.columns.len != 0 and owned_raw.columns.len != fields.len) return error.UnexpectedColumnCount;
    if (owned_raw.columns.len == 0 and owned_raw.rows.len != 0 and owned_raw.rows[0].values.len != fields.len) {
        return error.UnexpectedColumnCount;
    }

    const arena = owned_raw.arena.allocator();
    const typed_rows = try arena.alloc(RowT, owned_raw.rows.len);
    for (typed_rows, 0..) |*typed_row, row_index| {
        var out: RowT = undefined;
        inline for (fields, 0..) |field, column_index| {
            @field(out, field.name) = try decodeCompiledField(owned_raw, row_index, column_index, field.type);
        }
        typed_row.* = out;
    }

    return .{
        .raw = owned_raw,
        .rows = typed_rows,
    };
}

fn decodeCompiledField(result: Result, row_index: usize, column_index: usize, comptime T: type) !T {
    if (result.columns.len == 0) {
        return decodeCompiledFieldWithoutColumns(result.rows[row_index], column_index, T);
    }
    return switch (@typeInfo(T)) {
        .optional => |opt| if (try result.decode(row_index, column_index, opt.child)) |value|
            value
        else
            null,
        else => (try result.decode(row_index, column_index, T)).? ,
    };
}

fn decodeCompiledFieldWithoutColumns(row: Row, column_index: usize, comptime T: type) !T {
    if (column_index >= row.values.len) return error.ColumnOutOfBounds;
    return switch (@typeInfo(T)) {
        .optional => |opt| if (row.values[column_index]) |raw|
            try decodeTextValue(opt.child, raw)
        else
            null,
        else => if (row.values[column_index]) |raw|
            try decodeTextValue(T, raw)
        else
            error.BadValue,
    };
}

fn collectParamTypes(allocator: std.mem.Allocator, params: []const proto.Param) ![]u32 {
    if (params.len == 0) return allocator.dupe(u32, &.{});
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
    const storage = try arena.dupe(u8, payload);
    var i: usize = 0;
    const count = try readIntAt(u16, storage, &i);
    i += 2;
    const values = try arena.alloc(?[]const u8, count);
    for (0..count) |idx| {
        const len = try readIntAt(i32, storage, &i);
        i += 4;
        if (len == -1) {
            values[idx] = null;
            continue;
        }
        if (len < 0) return error.InvalidDataRow;
        if (storage.len - i < @as(usize, @intCast(len))) return error.InvalidDataRow;
        values[idx] = storage[i..][0..@as(usize, @intCast(len))];
        i += @as(usize, @intCast(len));
    }
    return .{ .values = values };
}

fn extendedQueryMessageLen(statement_name: []const u8, sql: []const u8, params: []const proto.Param, include_parse: bool) usize {
    return extendedQueryMessageLenFor("", statement_name, sql, params, params.len, include_parse, true);
}

fn extendedExecuteMessageLen(statement_name: []const u8, params: []const proto.Param) usize {
    return extendedExecuteMessageLenFor("", statement_name, params, true);
}

fn extendedQueryMessageLenFor(portal_name: []const u8, statement_name: []const u8, sql: []const u8, params: []const proto.Param, parse_param_type_count: usize, include_parse: bool, include_sync: bool) usize {
    var len = extendedExecuteMessageLenFor(portal_name, statement_name, params, include_sync);
    if (include_parse) {
        len += taggedMessageLen(statement_name.len + 1 + sql.len + 1 + 2 + (4 * parse_param_type_count));
    }
    return len;
}

fn extendedExecuteMessageLenFor(portal_name: []const u8, statement_name: []const u8, params: []const proto.Param, include_sync: bool) usize {
    var len = bindMessageLen(portal_name, statement_name, params) +
        executeMessageLen(portal_name);
    if (include_sync) len += syncMessageLen();
    return len;
}

fn closeMessageLen(name: []const u8, include: bool) usize {
    if (!include) return 0;
    return taggedMessageLen(1 + name.len + 1);
}

fn bindMessageLen(portal_name: []const u8, statement_name: []const u8, params: []const proto.Param) usize {
    var payload_len: usize = portal_name.len + 1 + statement_name.len + 1;
    payload_len += 2 + (2 * params.len);
    payload_len += 2;
    for (params) |param| {
        payload_len += 4;
        if (param.value) |value| payload_len += value.len;
    }
    payload_len += 2 + 2;
    return taggedMessageLen(payload_len);
}

fn executeMessageLen(portal_name: []const u8) usize {
    return taggedMessageLen(portal_name.len + 1 + 4);
}

fn syncMessageLen() usize {
    return taggedMessageLen(0);
}

fn taggedMessageLen(payload_len: usize) usize {
    return 1 + 4 + payload_len;
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
    if (T == Date) return parseDateText(raw);
    if (T == Time) {
        const parsed = try parseTimeText(raw);
        if (parsed.offset_seconds != null or parsed.consumed != raw.len) return error.BadValue;
        return parsed.time;
    }
    if (T == Timestamp) return parseTimestampText(raw);
    if (T == [16]u8) return parseUuidText(raw);

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
    if (T == Date and type_oid == 1082) return decodeBinaryDate(raw);
    if (T == Time and type_oid == 1083) return decodeBinaryTime(raw);
    if (T == Timestamp and (type_oid == 1114 or type_oid == 1184)) return decodeBinaryTimestamp(raw);
    if (T == [16]u8 and type_oid == 2950) return decodeBinaryUuid(raw);
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
        17, 25, 1043, 114 => if (T == []const u8)
            raw
        else
            error.TypeMismatch,
        3802 => decodeBinaryJsonb(T, raw),
        2950 => if (T == []const u8)
            raw
        else
            error.TypeMismatch,
        else => error.UnsupportedBinaryType,
    };
}

fn decodeBinaryJsonb(comptime T: type, raw: []const u8) !T {
    if (T != []const u8) return error.TypeMismatch;
    if (raw.len == 0 or raw[0] != 1) return error.InvalidBinaryValue;
    return raw[1..];
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

const ParsedTimeText = struct {
    time: Time,
    offset_seconds: ?i32 = null,
    consumed: usize,
};

const pg_unix_epoch_days: i64 = daysFromCivil(2000, 1, 1);
const pg_unix_epoch_micros: i64 = pg_unix_epoch_days * std.time.us_per_day;

fn parseDateText(raw: []const u8) !Date {
    var index: usize = 0;
    const year = try parseSignedIntComponent(i32, raw, &index);
    try expectSeparator(raw, &index, '-');
    const month = try parseFixedWidthUnsigned(u8, raw, &index, 2);
    try expectSeparator(raw, &index, '-');
    const day = try parseFixedWidthUnsigned(u8, raw, &index, 2);
    if (index != raw.len) return error.BadValue;
    validateDate(year, month, day) catch return error.BadValue;
    return .{
        .year = year,
        .month = month,
        .day = day,
    };
}

fn parseTimeText(raw: []const u8) !ParsedTimeText {
    var index: usize = 0;
    const hour = try parseFixedWidthUnsigned(u8, raw, &index, 2);
    try expectSeparator(raw, &index, ':');
    const minute = try parseFixedWidthUnsigned(u8, raw, &index, 2);
    try expectSeparator(raw, &index, ':');
    const second = try parseFixedWidthUnsigned(u8, raw, &index, 2);
    var microsecond: u32 = 0;
    if (index < raw.len and raw[index] == '.') {
        index += 1;
        microsecond = try parseFractionMicros(raw, &index);
    }
    validateTime(hour, minute, second) catch return error.BadValue;
    const offset_seconds = if (index < raw.len) try parseOffsetSeconds(raw, &index) else null;
    return .{
        .time = .{
            .hour = hour,
            .minute = minute,
            .second = second,
            .microsecond = microsecond,
        },
        .offset_seconds = offset_seconds,
        .consumed = index,
    };
}

fn parseTimestampText(raw: []const u8) !Timestamp {
    var index: usize = 0;
    const year = try parseSignedIntComponent(i32, raw, &index);
    try expectSeparator(raw, &index, '-');
    const month = try parseFixedWidthUnsigned(u8, raw, &index, 2);
    try expectSeparator(raw, &index, '-');
    const day = try parseFixedWidthUnsigned(u8, raw, &index, 2);
    if (index >= raw.len or (raw[index] != ' ' and raw[index] != 'T')) return error.BadValue;
    index += 1;

    const parsed_time = try parseTimeText(raw[index..]);
    if (index + parsed_time.consumed != raw.len) return error.BadValue;
    validateDate(year, month, day) catch return error.BadValue;

    var timestamp = Timestamp{
        .date = .{
            .year = year,
            .month = month,
            .day = day,
        },
        .time = parsed_time.time,
    };
    if (parsed_time.offset_seconds) |offset| {
        timestamp = try timestampWithOffsetToUtc(timestamp, offset);
    }
    return timestamp;
}

fn decodeBinaryUuid(raw: []const u8) ![16]u8 {
    if (raw.len != 16) return error.InvalidBinaryValue;
    return raw[0..16].*;
}

fn decodeBinaryDate(raw: []const u8) !Date {
    if (raw.len != 4) return error.InvalidBinaryValue;
    const pg_days = std.mem.readInt(i32, raw[0..4], .big);
    return dateFromUnixDays(@as(i64, pg_days) + pg_unix_epoch_days);
}

fn decodeBinaryTime(raw: []const u8) !Time {
    if (raw.len != 8) return error.InvalidBinaryValue;
    const micros = std.mem.readInt(i64, raw[0..8], .big);
    if (micros < 0 or micros >= std.time.us_per_day) return error.InvalidBinaryValue;
    return timeFromDayMicros(micros);
}

fn decodeBinaryTimestamp(raw: []const u8) !Timestamp {
    if (raw.len != 8) return error.InvalidBinaryValue;
    const micros = std.mem.readInt(i64, raw[0..8], .big);
    return timestampFromUnixMicros(micros + pg_unix_epoch_micros);
}

fn parseUuidText(raw: []const u8) ![16]u8 {
    if (raw.len != 36) return error.BadValue;
    var out: [16]u8 = undefined;
    var raw_index: usize = 0;
    var out_index: usize = 0;
    while (raw_index < raw.len) {
        if (raw_index == 8 or raw_index == 13 or raw_index == 18 or raw_index == 23) {
            if (raw[raw_index] != '-') return error.BadValue;
            raw_index += 1;
            continue;
        }
        out[out_index] = (try parseHexNibble(raw[raw_index]) << 4) | try parseHexNibble(raw[raw_index + 1]);
        raw_index += 2;
        out_index += 1;
    }
    return out;
}

fn parseHexNibble(ch: u8) !u8 {
    return switch (ch) {
        '0'...'9' => ch - '0',
        'a'...'f' => ch - 'a' + 10,
        'A'...'F' => ch - 'A' + 10,
        else => error.BadValue,
    };
}

fn parseSignedIntComponent(comptime T: type, raw: []const u8, index: *usize) !T {
    const start = index.*;
    if (index.* < raw.len and (raw[index.*] == '-' or raw[index.*] == '+')) index.* += 1;
    const digit_start = index.*;
    while (index.* < raw.len and std.ascii.isDigit(raw[index.*])) : (index.* += 1) {}
    if (digit_start == index.*) return error.BadValue;
    return std.fmt.parseInt(T, raw[start..index.*], 10) catch error.BadValue;
}

fn parseFixedWidthUnsigned(comptime T: type, raw: []const u8, index: *usize, width: usize) !T {
    if (raw.len - index.* < width) return error.BadValue;
    for (raw[index.* .. index.* + width]) |ch| {
        if (!std.ascii.isDigit(ch)) return error.BadValue;
    }
    const value = std.fmt.parseInt(T, raw[index.* .. index.* + width], 10) catch return error.BadValue;
    index.* += width;
    return value;
}

fn parseFractionMicros(raw: []const u8, index: *usize) !u32 {
    var micros: u32 = 0;
    var digits: usize = 0;
    while (index.* < raw.len and std.ascii.isDigit(raw[index.*])) : (index.* += 1) {
        if (digits < 6) micros = micros * 10 + (raw[index.*] - '0');
        digits += 1;
    }
    if (digits == 0) return error.BadValue;
    if (digits < 6) {
        var remaining = 6 - digits;
        while (remaining != 0) : (remaining -= 1) micros *= 10;
    }
    return micros;
}

fn parseOffsetSeconds(raw: []const u8, index: *usize) !?i32 {
    if (index.* >= raw.len) return null;
    if (raw[index.*] == 'Z') {
        index.* += 1;
        return 0;
    }
    if (raw[index.*] != '+' and raw[index.*] != '-') return null;
    const sign: i32 = if (raw[index.*] == '-') -1 else 1;
    index.* += 1;
    const hours = try parseFixedWidthUnsigned(i32, raw, index, 2);
    var minutes: i32 = 0;
    var seconds: i32 = 0;
    if (index.* < raw.len and raw[index.*] == ':') {
        index.* += 1;
        minutes = try parseFixedWidthUnsigned(i32, raw, index, 2);
        if (index.* < raw.len and raw[index.*] == ':') {
            index.* += 1;
            seconds = try parseFixedWidthUnsigned(i32, raw, index, 2);
        }
    } else if (raw.len - index.* >= 2 and std.ascii.isDigit(raw[index.*]) and std.ascii.isDigit(raw[index.* + 1])) {
        minutes = try parseFixedWidthUnsigned(i32, raw, index, 2);
    }
    if (minutes >= 60 or seconds >= 60) return error.BadValue;
    return sign * (hours * 3600 + minutes * 60 + seconds);
}

fn expectSeparator(raw: []const u8, index: *usize, separator: u8) !void {
    if (index.* >= raw.len or raw[index.*] != separator) return error.BadValue;
    index.* += 1;
}

fn validateDate(year: i32, month: u8, day: u8) !void {
    if (month < 1 or month > 12 or day < 1) return error.BadValue;
    const days_in_month: u8 = switch (month) {
        1, 3, 5, 7, 8, 10, 12 => 31,
        4, 6, 9, 11 => 30,
        2 => if (isLeapYear(year)) @as(u8, 29) else @as(u8, 28),
        else => unreachable,
    };
    if (day > days_in_month) return error.BadValue;
}

fn validateTime(hour: u8, minute: u8, second: u8) !void {
    if (hour > 23 or minute > 59 or second > 59) return error.BadValue;
}

fn isLeapYear(year: i32) bool {
    if (@mod(year, 4) != 0) return false;
    if (@mod(year, 100) != 0) return true;
    return @mod(year, 400) == 0;
}

fn daysFromCivil(year: i32, month: u8, day: u8) i64 {
    const y = @as(i64, year) - @as(i64, if (month <= 2) 1 else 0);
    const era = @divFloor(y, 400);
    const yoe = y - era * 400;
    const m = @as(i64, month);
    const mp = m + (if (m > 2) @as(i64, -3) else @as(i64, 9));
    const doy = @divFloor(153 * mp + 2, 5) + @as(i64, day) - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146097 + doe - 719468;
}

fn dateFromUnixDays(days: i64) Date {
    const z = days + 719468;
    const era = @divFloor(z, 146097);
    const doe = z - era * 146097;
    const yoe = @divFloor(doe - @divFloor(doe, 1460) + @divFloor(doe, 36524) - @divFloor(doe, 146096), 365);
    var year = yoe + era * 400;
    const doy = doe - (365 * yoe + @divFloor(yoe, 4) - @divFloor(yoe, 100));
    const mp = @divFloor(5 * doy + 2, 153);
    const day = doy - @divFloor(153 * mp + 2, 5) + 1;
    const month = mp + (if (mp < 10) @as(i64, 3) else @as(i64, -9));
    year += if (month <= 2) 1 else 0;
    return .{
        .year = @intCast(year),
        .month = @intCast(month),
        .day = @intCast(day),
    };
}

fn timeFromDayMicros(day_micros: i64) Time {
    const hour = @divFloor(day_micros, std.time.us_per_hour);
    const minute = @divFloor(@mod(day_micros, std.time.us_per_hour), std.time.us_per_min);
    const second = @divFloor(@mod(day_micros, std.time.us_per_min), std.time.us_per_s);
    const microsecond = @mod(day_micros, std.time.us_per_s);
    return .{
        .hour = @intCast(hour),
        .minute = @intCast(minute),
        .second = @intCast(second),
        .microsecond = @intCast(microsecond),
    };
}

fn timestampFromUnixMicros(unix_micros: i64) !Timestamp {
    const unix_days = @divFloor(unix_micros, std.time.us_per_day);
    const day_micros = @mod(unix_micros, std.time.us_per_day);
    return .{
        .date = dateFromUnixDays(unix_days),
        .time = timeFromDayMicros(day_micros),
    };
}

fn timestampWithOffsetToUtc(timestamp: Timestamp, offset_seconds: i32) !Timestamp {
    const unix_micros = unixMicrosFromTimestamp(timestamp) - @as(i64, offset_seconds) * std.time.us_per_s;
    return timestampFromUnixMicros(unix_micros);
}

fn unixMicrosFromTimestamp(timestamp: Timestamp) i64 {
    const unix_days = daysFromCivil(timestamp.date.year, timestamp.date.month, timestamp.date.day);
    const day_micros =
        @as(i64, timestamp.time.hour) * std.time.us_per_hour +
        @as(i64, timestamp.time.minute) * std.time.us_per_min +
        @as(i64, timestamp.time.second) * std.time.us_per_s +
        @as(i64, timestamp.time.microsecond);
    return unix_days * std.time.us_per_day + day_micros;
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

test "binary decode supports uuid date time timestamp and jsonb" {
    try std.testing.expectEqualSlices(u8, &.{
        0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
        0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
    }, &(try decodeBinaryValue([16]u8, 2950, &.{
        0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
        0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
    })));

    var date_bytes: [4]u8 = undefined;
    std.mem.writeInt(i32, &date_bytes, @intCast(daysFromCivil(2026, 3, 18) - pg_unix_epoch_days), .big);
    try std.testing.expectEqual(Date{ .year = 2026, .month = 3, .day = 18 }, try decodeBinaryValue(Date, 1082, &date_bytes));

    var time_bytes: [8]u8 = undefined;
    std.mem.writeInt(i64, &time_bytes, 12 * std.time.us_per_hour + 34 * std.time.us_per_min + 56 * std.time.us_per_s + 789012, .big);
    try std.testing.expectEqual(Time{ .hour = 12, .minute = 34, .second = 56, .microsecond = 789012 }, try decodeBinaryValue(Time, 1083, &time_bytes));

    const ts = Timestamp{
        .date = .{ .year = 2026, .month = 3, .day = 18 },
        .time = .{ .hour = 12, .minute = 34, .second = 56, .microsecond = 789012 },
    };
    var ts_bytes: [8]u8 = undefined;
    std.mem.writeInt(i64, &ts_bytes, unixMicrosFromTimestamp(ts) - pg_unix_epoch_micros, .big);
    try std.testing.expectEqual(ts, try decodeBinaryValue(Timestamp, 1114, &ts_bytes));

    try std.testing.expectEqualStrings("{\"a\":1}", try decodeBinaryValue([]const u8, 3802, &.{ 1, '{', '"', 'a', '"', ':', '1', '}' }));
}

test "text decode supports bool int float and bytes" {
    try std.testing.expectEqual(@as(i32, 42), try decodeTextValue(i32, "42"));
    try std.testing.expectEqual(true, try decodeTextValue(bool, "t"));
    try std.testing.expectApproxEqAbs(@as(f64, 1.25), try decodeTextValue(f64, "1.25"), 0.000001);
    try std.testing.expectEqualStrings("hello", try decodeTextValue([]const u8, "hello"));
}

test "text decode supports uuid date time and timestamp types" {
    try std.testing.expectEqualSlices(u8, &.{
        0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
        0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
    }, &(try decodeTextValue([16]u8, "550e8400-e29b-41d4-a716-446655440000")));

    try std.testing.expectEqual(Date{ .year = 2026, .month = 3, .day = 18 }, try decodeTextValue(Date, "2026-03-18"));
    try std.testing.expectEqual(Time{ .hour = 12, .minute = 34, .second = 56, .microsecond = 789012 }, try decodeTextValue(Time, "12:34:56.789012"));
    try std.testing.expectEqual(Timestamp{
        .date = .{ .year = 2026, .month = 3, .day = 18 },
        .time = .{ .hour = 12, .minute = 34, .second = 56, .microsecond = 789012 },
    }, try decodeTextValue(Timestamp, "2026-03-18 12:34:56.789012"));
    try std.testing.expectEqual(Timestamp{
        .date = .{ .year = 2026, .month = 3, .day = 18 },
        .time = .{ .hour = 7, .minute = 4, .second = 56, .microsecond = 789012 },
    }, try decodeTextValue(Timestamp, "2026-03-18 12:34:56.789012+05:30"));
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

test "sql placeholder count ignores quoted and dollar quoted text" {
    try std.testing.expectEqual(@as(usize, 2), comptime sqlPlaceholderCount(
        "select '$1', $$ $2 $$, col from t where a = $1 and b = $2",
    ));
    try std.testing.expectEqual(@as(usize, 1), comptime sqlPlaceholderCount(
        "select \"$2\", $tag$ $9 $tag$, $1",
    ));
    try std.testing.expectEqual(@as(usize, 0), comptime sqlPlaceholderCount(
        "select '$1''$2', \"x$3\", $$ $4 $$",
    ));
}

test "sql placeholder count ignores line and nested block comments" {
    try std.testing.expectEqual(@as(usize, 1), comptime sqlPlaceholderCount(
        "select $1 -- $2 stays in a comment\n",
    ));
    try std.testing.expectEqual(@as(usize, 2), comptime sqlPlaceholderCount(
        "select /* $9 /* $8 */ still comment */ $2",
    ));
    try std.testing.expectEqual(@as(usize, 0), comptime sqlPlaceholderCount(
        "select /* $1",
    ));
}

test "compiled query infers protocol from placeholder usage" {
    const SimpleQuery = CompiledQuery("select 1", struct {}, struct { n: i32 });
    const ExtendedQuery = CompiledQuery("select $1::int4", struct { i32 }, struct { n: i32 });

    try std.testing.expectEqual(QueryProtocol.simple, SimpleQuery.protocol);
    try std.testing.expectEqual(QueryProtocol.extended, ExtendedQuery.protocol);
}

test "compiled arg encoder covers scalars optionals arrays and raw binary" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const params = try encodeCompiledParams(arena.allocator(), .{
        true,
        @as(i32, -7),
        @as(u64, 9),
        @as(f32, 1.5),
        @as([]const u8, "abc"),
        @as(?i32, null),
        @as(Value, .{ .raw_binary = .{ .type_oid = 17, .bytes = &.{ 1, 2, 3 } } }),
    });

    try std.testing.expectEqual(@as(usize, 7), params.len);
    try std.testing.expectEqual(@as(u32, 16), params[0].type_oid);
    try std.testing.expectEqualStrings("-7", params[1].value.?);
    try std.testing.expectEqual(@as(u32, 20), params[2].type_oid);
    try std.testing.expectEqual(@as(u32, 700), params[3].type_oid);
    try std.testing.expectEqualStrings("abc", params[4].value.?);
    try std.testing.expect(params[5].value == null);
    try std.testing.expectEqual(proto.FormatCode.binary, params[6].format);
    try std.testing.expectEqual(@as(u32, 17), params[6].type_oid);
}

test "collect param types preserves order and empty input" {
    const empty = try collectParamTypes(std.testing.allocator, &.{});
    defer std.testing.allocator.free(empty);
    try std.testing.expectEqual(@as(usize, 0), empty.len);

    const values = try collectParamTypes(std.testing.allocator, &.{
        .{ .type_oid = 23, .value = "1" },
        .{ .type_oid = 25, .value = "x" },
        .{ .type_oid = 17, .format = .binary, .value = &.{ 0xaa } },
    });
    defer std.testing.allocator.free(values);
    try std.testing.expectEqualSlices(u32, &.{ 23, 25, 17 }, values);
}

test "materialize compiled result decodes optional and positional rows" {
    const RowT = struct {
        a: []const u8,
        b: ?i32,
    };

    var with_columns = Result{ .arena = std.heap.ArenaAllocator.init(std.testing.allocator) };
    const arena1 = with_columns.arena.allocator();
    with_columns.columns = try arena1.dupe(Column, &.{
        .{ .name = "a", .type_oid = 25, .format = .text },
        .{ .name = "b", .type_oid = 23, .format = .text },
    });
    with_columns.rows = try arena1.dupe(Row, &.{
        .{ .values = try arena1.dupe(?[]const u8, &.{ "ok", null }) },
    });
    var typed1 = try materializeCompiledResult(RowT, with_columns);
    defer typed1.deinit();
    try std.testing.expectEqualStrings("ok", typed1.rows[0].a);
    try std.testing.expectEqual(@as(?i32, null), typed1.rows[0].b);

    var no_columns = Result{ .arena = std.heap.ArenaAllocator.init(std.testing.allocator) };
    const arena2 = no_columns.arena.allocator();
    no_columns.rows = try arena2.dupe(Row, &.{
        .{ .values = try arena2.dupe(?[]const u8, &.{ "hello", "42" }) },
    });
    var typed2 = try materializeCompiledResult(RowT, no_columns);
    defer typed2.deinit();
    try std.testing.expectEqualStrings("hello", typed2.rows[0].a);
    try std.testing.expectEqual(@as(?i32, 42), typed2.rows[0].b);
}

test "materialize compiled result rejects mismatched shape" {
    const RowT = struct { a: []const u8, b: i32 };

    var wrong_columns = Result{ .arena = std.heap.ArenaAllocator.init(std.testing.allocator) };
    defer wrong_columns.deinit();
    const arena1 = wrong_columns.arena.allocator();
    wrong_columns.columns = try arena1.dupe(Column, &.{
        .{ .name = "a", .type_oid = 25, .format = .text },
    });
    try std.testing.expectError(error.UnexpectedColumnCount, materializeCompiledResult(RowT, wrong_columns));

    var wrong_row = Result{ .arena = std.heap.ArenaAllocator.init(std.testing.allocator) };
    defer wrong_row.deinit();
    const arena2 = wrong_row.arena.allocator();
    wrong_row.rows = try arena2.dupe(Row, &.{
        .{ .values = try arena2.dupe(?[]const u8, &.{ "only-one" }) },
    });
    try std.testing.expectError(error.UnexpectedColumnCount, materializeCompiledResult(RowT, wrong_row));
}

test "decode compiled field without columns validates bounds and nullability" {
    const row = Row{ .values = &.{ "7", null } };
    try std.testing.expectEqual(@as(i32, 7), try decodeCompiledFieldWithoutColumns(row, 0, i32));
    try std.testing.expectEqual(@as(?i32, null), try decodeCompiledFieldWithoutColumns(row, 1, ?i32));
    try std.testing.expectError(error.BadValue, decodeCompiledFieldWithoutColumns(row, 1, i32));
    try std.testing.expectError(error.ColumnOutOfBounds, decodeCompiledFieldWithoutColumns(row, 2, i32));
}

test "parse format code and command complete validate inputs" {
    try std.testing.expectEqual(proto.FormatCode.text, try parseFormatCode(0));
    try std.testing.expectEqual(proto.FormatCode.binary, try parseFormatCode(1));
    try std.testing.expectError(error.UnsupportedColumnFormat, parseFormatCode(9));

    try std.testing.expectEqualStrings("SELECT 1", try parseCommandComplete("SELECT 1\x00"));
    try std.testing.expectError(error.InvalidField, parseCommandComplete("missing-zero"));
}

test "read int at validates bounds" {
    var index: usize = 0;
    try std.testing.expectEqual(@as(u16, 0x1234), try readIntAt(u16, &.{ 0x12, 0x34 }, &index));
    try std.testing.expectError(error.InvalidMessage, readIntAt(u32, &.{ 0x12, 0x34 }, &index));
}

test "binary and text decode reject invalid shapes" {
    try std.testing.expectError(error.TypeMismatch, decodeBinaryValue(i32, 16, &.{1}));
    try std.testing.expectError(error.InvalidBinaryValue, decodeBinaryValue([16]u8, 2950, &.{ 1, 2, 3 }));
    try std.testing.expectError(error.BadValue, decodeTextValue(bool, "not-bool"));
    try std.testing.expectError(error.UnsupportedDecode, decodeTextValue([]u8, "x"));
}

test "finish ready updates transaction state and validates payload size" {
    var conn: Conn = undefined;
    conn.tx_status = 'I';
    conn.healthy = true;

    try conn.finishReady(&.{ 'I' });
    try std.testing.expectEqual(@as(u8, 'I'), conn.tx_status);
    try std.testing.expect(conn.healthy);

    conn.healthy = true;
    try conn.finishReady(&.{ 'T' });
    try std.testing.expectEqual(@as(u8, 'T'), conn.tx_status);
    try std.testing.expect(!conn.healthy);

    conn.healthy = true;
    try std.testing.expectError(error.ProtocolViolation, conn.finishReady(&.{ 'I', 'x' }));
    try std.testing.expect(!conn.healthy);
}

test "queue pipeline simple query appends statements" {
    var conn: Conn = undefined;
    conn.allocator = std.testing.allocator;
    conn.queued_simple_pipeline_sql = .empty;
    conn.last_error = null;
    conn.unnamed_statement_sql = &.{};
    conn.unnamed_statement_param_types = &.{};
    defer {
        conn.queued_simple_pipeline_sql.deinit(std.testing.allocator);
    }

    try conn.queuePipelineSimpleQuery("select 1");
    try std.testing.expectEqualStrings("select 1;", conn.queued_simple_pipeline_sql.items);

    try conn.queuePipelineSimpleQuery("select 2");
    try std.testing.expectEqualStrings("select 1;select 2;", conn.queued_simple_pipeline_sql.items);
}

test "simple query paths clear unnamed statement cache" {
    var conn: Conn = undefined;
    conn.allocator = std.testing.allocator;
    conn.last_error = null;
    conn.unnamed_statement_sql = try std.testing.allocator.dupe(u8, "select $1");
    conn.unnamed_statement_param_types = try std.testing.allocator.dupe(u32, &.{23});
    conn.queued_simple_pipeline_sql = .empty;
    defer {
        conn.clearUnnamedStatementCache();
        conn.queued_simple_pipeline_sql.deinit(std.testing.allocator);
    }

    try conn.queuePipelineSimpleQuery("select 1");
    try std.testing.expectEqual(@as(usize, 0), conn.unnamed_statement_sql.len);
    try std.testing.expectEqual(@as(usize, 0), conn.unnamed_statement_param_types.len);
}

test "parse data row rejects negative non null length" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    try std.testing.expectError(error.InvalidDataRow, parseDataRow(arena.allocator(), &.{
        0, 1,
        0xff, 0xff, 0xff, 0xfe,
    }));
}

test "parse row description rejects unsupported format code" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var columns: std.ArrayList(Column) = .empty;
    defer columns.deinit(arena.allocator());

    try std.testing.expectError(error.UnsupportedColumnFormat, parseRowDescription(arena.allocator(), &columns, &.{
        0, 1,
        'x', 0,
        0, 0, 0, 0,
        0, 0,
        0, 0, 0, 23,
        0, 4,
        0, 0, 0, 0xff,
        0, 2,
    }));
}

test "message length helpers match encoded wire sizes" {
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(std.testing.allocator);

    const params = [_]proto.Param{
        .{ .type_oid = 23, .value = "7" },
        .{ .type_oid = 25, .value = "hi" },
    };

    try proto.appendBind(&out, std.testing.allocator, "portal", "stmt", &params, .text);
    try std.testing.expectEqual(bindMessageLen("portal", "stmt", &params), out.items.len);

    out.clearRetainingCapacity();
    try proto.appendExecute(&out, std.testing.allocator, "portal", 0);
    try std.testing.expectEqual(executeMessageLen("portal"), out.items.len);

    out.clearRetainingCapacity();
    try proto.appendSync(&out, std.testing.allocator);
    try std.testing.expectEqual(syncMessageLen(), out.items.len);

    out.clearRetainingCapacity();
    try proto.appendParse(&out, std.testing.allocator, "stmt", "select $1, $2", &.{ 23, 25 });
    try proto.appendBind(&out, std.testing.allocator, "", "stmt", &params, .text);
    try proto.appendExecute(&out, std.testing.allocator, "", 0);
    try proto.appendSync(&out, std.testing.allocator);
    try std.testing.expectEqual(extendedQueryMessageLen("stmt", "select $1, $2", &params, true), out.items.len);
}

test "direct value bind encodes binary scalars without text formatting" {
    const values = [_]Value{
        .{ .int4 = 7 },
        .{ .text = "hi" },
        .{ .float8 = 1.25 },
    };

    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(std.testing.allocator);
    try out.ensureUnusedCapacity(std.testing.allocator, try valueBindMessageLen("p0", "s0", &values));
    try appendBindValuesAssumeCapacity(&out, "p0", "s0", &values, .text);

    try std.testing.expectEqual(@as(u8, 'B'), out.items[0]);

    var index: usize = 5;
    try std.testing.expectEqualStrings("p0", try proto.fieldCString(out.items, &index));
    try std.testing.expectEqualStrings("s0", try proto.fieldCString(out.items, &index));
    try std.testing.expectEqual(@as(u16, 3), try readIntAt(u16, out.items, &index));
    index += 2;
    try std.testing.expectEqual(@intFromEnum(proto.FormatCode.binary), try readIntAt(u16, out.items, &index));
    index += 2;
    try std.testing.expectEqual(@intFromEnum(proto.FormatCode.text), try readIntAt(u16, out.items, &index));
    index += 2;
    try std.testing.expectEqual(@intFromEnum(proto.FormatCode.binary), try readIntAt(u16, out.items, &index));
    index += 2;
    try std.testing.expectEqual(@as(u16, 3), try readIntAt(u16, out.items, &index));
    index += 2;

    try std.testing.expectEqual(@as(i32, 4), try readIntAt(i32, out.items, &index));
    index += 4;
    var value_index: usize = 0;
    try std.testing.expectEqual(@as(i32, 7), try readIntAt(i32, out.items[index .. index + 4], &value_index));
    index += 4;

    try std.testing.expectEqual(@as(i32, 2), try readIntAt(i32, out.items, &index));
    index += 4;
    try std.testing.expectEqualStrings("hi", out.items[index .. index + 2]);
    index += 2;

    try std.testing.expectEqual(@as(i32, 8), try readIntAt(i32, out.items, &index));
    index += 4;
    value_index = 0;
    try std.testing.expectEqual(@as(f64, 1.25), @as(f64, @bitCast(try readIntAt(u64, out.items[index .. index + 8], &value_index))));
}

test "pipeline statement cache keys include parameter types" {
    var conn: Conn = undefined;
    conn.allocator = std.testing.allocator;
    conn.queued_writes = .empty;
    conn.pipeline_statement_sql = try std.testing.allocator.dupe(u8, "select $1");
    conn.pipeline_statement_param_types = try std.testing.allocator.dupe(u32, &.{23});
    defer {
        conn.clearPipelineStatementCache();
        conn.queued_writes.deinit(std.testing.allocator);
    }

    try conn.queuePipelineParameterizedQuery("select $1", &.{.{ .type_oid = 23, .value = "7" }}, &.{23}, .text, 1);
    try std.testing.expectEqual(@as(u8, 'B'), conn.queued_writes.items[0]);

    conn.queued_writes.clearRetainingCapacity();
    try conn.queuePipelineParameterizedQuery("select $1", &.{.{ .type_oid = 25, .value = "x" }}, &.{25}, .text, 2);
    try std.testing.expectEqual(@as(u8, 'C'), conn.queued_writes.items[0]);
    try std.testing.expect(std.mem.indexOfScalar(u8, conn.queued_writes.items, 'P') != null);
}

test "read query result mode rejects extended sequence without control message" {
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(std.testing.allocator);
    try appendTestMessage(&bytes, 'T', &.{
        0, 1,
        'n', 0,
        0, 0, 0, 0,
        0, 0,
        0, 0, 0, 23,
        0, 4,
        0, 0, 0, 0xff,
        0, 0,
    });
    try appendTestMessage(&bytes, 'D', &.{
        0, 1,
        0, 0, 0, 1,
        '7',
    });
    try appendTestMessage(&bytes, 'C', "SELECT 1\x00");
    try appendTestMessage(&bytes, 'Z', &.{ 'I' });

    var conn = testConnWithFixedReader(bytes.items);
    defer if (conn.message_buffer.len != 0) std.testing.allocator.free(conn.message_buffer);
    try std.testing.expectError(error.ProtocolViolation, conn.readQueryResultMode(std.testing.allocator, true, true));
    try std.testing.expect(!conn.healthy);
}

test "read exec result mode accepts extended command without ready when control message seen" {
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(std.testing.allocator);
    try appendTestMessage(&bytes, '1', &.{});
    try appendTestMessage(&bytes, 'C', "UPDATE 3\x00");

    var conn = testConnWithFixedReader(bytes.items);
    defer if (conn.message_buffer.len != 0) std.testing.allocator.free(conn.message_buffer);
    const tag = try conn.readExecResultMode(std.testing.allocator, true, false);
    defer std.testing.allocator.free(tag);
    try std.testing.expectEqualStrings("UPDATE 3", tag);
}

test "read exec result mode keeps only the last command tag without leaking" {
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(std.testing.allocator);
    try appendTestMessage(&bytes, 'C', "SELECT 1\x00");
    try appendTestMessage(&bytes, 'C', "SELECT 2\x00");
    try appendTestMessage(&bytes, 'Z', &.{ 'I' });

    var conn = testConnWithFixedReader(bytes.items);
    defer if (conn.message_buffer.len != 0) std.testing.allocator.free(conn.message_buffer);
    const tag = try conn.readExecResultMode(std.testing.allocator, false, true);
    defer std.testing.allocator.free(tag);
    try std.testing.expectEqualStrings("SELECT 2", tag);
}

test "discard pipeline extended result rejects missing control message" {
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(std.testing.allocator);
    try appendTestMessage(&bytes, 'C', "SELECT 1\x00");

    var conn = testConnWithFixedReader(bytes.items);
    defer if (conn.message_buffer.len != 0) std.testing.allocator.free(conn.message_buffer);
    try std.testing.expectError(error.ProtocolViolation, conn.discardPipelineExtendedResult(false));
    try std.testing.expect(!conn.healthy);
}

test "read simple query result marks protocol violation on bad ready payload" {
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(std.testing.allocator);
    try appendTestMessage(&bytes, 'T', &.{
        0, 1,
        'n', 0,
        0, 0, 0, 0,
        0, 0,
        0, 0, 0, 23,
        0, 4,
        0, 0, 0, 0xff,
        0, 0,
    });
    try appendTestMessage(&bytes, 'D', &.{
        0, 1,
        0, 0, 0, 1,
        '7',
    });
    try appendTestMessage(&bytes, 'C', "SELECT 1\x00");
    try appendTestMessage(&bytes, 'Z', &.{ 'I', 'x' });

    var conn = testConnWithFixedReader(bytes.items);
    defer if (conn.message_buffer.len != 0) std.testing.allocator.free(conn.message_buffer);
    try std.testing.expectError(error.ProtocolViolation, conn.readQueryResultMode(std.testing.allocator, false, true));
    try std.testing.expect(!conn.healthy);
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

fn appendTestMessage(out: *std.ArrayList(u8), tag: u8, payload: []const u8) !void {
    try out.append(std.testing.allocator, tag);
    var len_bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_bytes, @as(u32, @intCast(payload.len + 4)), .big);
    try out.appendSlice(std.testing.allocator, &len_bytes);
    try out.appendSlice(std.testing.allocator, payload);
}

fn testConnWithFixedReader(bytes: []const u8) Conn {
    return .{
        .allocator = std.testing.allocator,
        .io = undefined,
        .config = &test_config,
        .stream = undefined,
        .reader = undefined,
        .writer = undefined,
        .reader_override = std.Io.Reader.fixed(bytes),
        .read_buffer = &.{},
        .write_buffer = &.{},
        .healthy = true,
        .tx_status = 'I',
    };
}

const test_config: Config = .{
    .host = @constCast("localhost"),
    .port = 5432,
    .user = @constCast("postgres"),
    .password = null,
    .database = @constCast("postgres"),
    .application_name = @constCast("zpg-test"),
    .ssl_mode = .disable,
    .ssl_root_cert = null,
    .connect_timeout_ms = 0,
    .max_message_len = 1024 * 1024,
};

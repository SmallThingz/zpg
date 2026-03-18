const std = @import("std");
const zpg = @import("zpg");

const DockerMode = enum {
    tls,
    mtls_required,

    fn arg(mode: DockerMode) []const u8 {
        return switch (mode) {
            .tls => "tls",
            .mtls_required => "mtls-required",
        };
    }
};

const DockerPg = struct {
    root: []u8,
    ca_cert_path: []u8,
    container_name: []u8,
    port: u16,

    fn deinit(pg: *DockerPg) void {
        _ = spawnExitCode(&.{ "/usr/bin/docker", "rm", "-f", pg.container_name }) catch 0;
        var cwd = std.Io.Dir.cwd();
        cwd.deleteTree(std.testing.io, pg.root) catch {};
        std.testing.allocator.free(pg.root);
        std.testing.allocator.free(pg.ca_cert_path);
        std.testing.allocator.free(pg.container_name);
    }
};

test {
    _ = zpg;
}

test "pool query against local postgres" {
    if (!std.process.can_spawn) return error.SkipZigTest;
    if (!haveExecutable("/usr/bin/initdb")) return error.SkipZigTest;
    if (!haveExecutable("/usr/bin/pg_ctl")) return error.SkipZigTest;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);

    const root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}", .{ cwd, &tmp.sub_path });
    defer std.testing.allocator.free(root);
    const data_dir = try std.fmt.allocPrint(std.testing.allocator, "{s}/pgdata", .{root});
    defer std.testing.allocator.free(data_dir);
    const port = 55000 + (@as(u16, tmp.sub_path[0]) << 2) + @as(u16, tmp.sub_path[1] % 200);
    const options = try std.fmt.allocPrint(std.testing.allocator, "-k /tmp -p {d}", .{port});
    defer std.testing.allocator.free(options);
    const url = try std.fmt.allocPrint(std.testing.allocator, "postgres://postgres@127.0.0.1:{d}/postgres?sslmode=disable", .{port});
    defer std.testing.allocator.free(url);

    try spawnExpectSuccess(&.{ "/usr/bin/initdb", "-D", data_dir, "--auth=trust", "--username=postgres" });
    try spawnExpectSuccess(&.{ "/usr/bin/pg_ctl", "-D", data_dir, "-o", options, "-w", "start" });
    defer spawnExpectSuccess(&.{ "/usr/bin/pg_ctl", "-D", data_dir, "-m", "immediate", "-w", "stop" }) catch {};

    var pool = try zpg.Pool.initUri(std.testing.allocator, std.testing.io, url, 2);
    defer pool.deinit();

    var result = try pool.query(std.testing.allocator, "select 'ok' as status, 7::int as n");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.columns.len);
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqualStrings("ok", result.rows[0].get(0).?);
    try std.testing.expectEqualStrings("7", result.rows[0].get(1).?);

    const tag = try pool.exec(std.testing.allocator, "select 1");
    defer std.testing.allocator.free(tag);
    try std.testing.expectEqualStrings("SELECT 1", tag);

    const stats = pool.stats();
    try std.testing.expectEqual(@as(usize, 1), stats.open);
    try std.testing.expectEqual(@as(usize, 1), stats.idle);

    var cfg = try zpg.Config.parseUri(std.testing.allocator, url);
    defer cfg.deinit(std.testing.allocator);

    const conn = try zpg.Conn.connect(std.testing.allocator, std.testing.io, &cfg);
    defer conn.destroy();

    var conn_result = try conn.query(std.testing.allocator, "select 'conn' as status");
    defer conn_result.deinit();
    try std.testing.expectEqualStrings("conn", conn_result.rows[0].get(0).?);

    var simple_pipeline = conn.pipeline(.{ .protocol = .simple });
    try simple_pipeline.query("select 'p1' as status");
    try simple_pipeline.query("select 'p2' as status");
    try simple_pipeline.flush();

    var p1 = try simple_pipeline.readQuery(std.testing.allocator);
    defer p1.deinit();
    try std.testing.expectEqualStrings("p1", p1.rows[0].get(0).?);

    var p2 = try simple_pipeline.readQuery(std.testing.allocator);
    defer p2.deinit();
    try std.testing.expectEqualStrings("p2", p2.rows[0].get(0).?);

    var extended_pipeline = conn.pipeline(.{
        .protocol = .extended,
    });
    try extended_pipeline.query("select 7 as n");
    try extended_pipeline.query("select 'pipe' as label");
    try extended_pipeline.flush();

    var e1 = try extended_pipeline.readQuery(std.testing.allocator);
    defer e1.deinit();
    try std.testing.expectEqualStrings("7", e1.rows[0].get(0).?);

    var e2 = try extended_pipeline.readQuery(std.testing.allocator);
    defer e2.deinit();
    try std.testing.expectEqualStrings("pipe", e2.rows[0].get(0).?);

    var parameter_pipeline = conn.pipeline(.{
        .protocol = .extended,
    });
    var prepared_for_pipeline = try conn.prepare(std.testing.allocator, "select $1::text as prepared_label");
    defer prepared_for_pipeline.deinit();
    try parameter_pipeline.queryValues(std.testing.allocator, "select $1::text as label, $2::int4 as n", &.{
        .{ .text = "param-pipe" },
        .{ .int4 = 29 },
    });
    try parameter_pipeline.execValues(std.testing.allocator, "select $1::int4", &.{
        .{ .int4 = 31 },
    });
    try parameter_pipeline.preparedQueryValues(std.testing.allocator, prepared_for_pipeline.name, &.{
        .{ .text = "prepared-pipe" },
    });
    try parameter_pipeline.flush();

    var pv1 = try parameter_pipeline.readQuery(std.testing.allocator);
    defer pv1.deinit();
    try std.testing.expectEqualStrings("param-pipe", pv1.rows[0].get(0).?);
    try std.testing.expectEqualStrings("29", pv1.rows[0].get(1).?);

    const pv_tag = try parameter_pipeline.readExec(std.testing.allocator);
    defer std.testing.allocator.free(pv_tag);
    try std.testing.expectEqualStrings("SELECT 1", pv_tag);

    var pv2 = try parameter_pipeline.readQuery(std.testing.allocator);
    defer pv2.deinit();
    try std.testing.expectEqualStrings("prepared-pipe", pv2.rows[0].get(0).?);

    const StaticConnQuery = zpg.CompiledQuery(
        "select 'compiled' as status, 11::int4 as n",
        struct {},
        struct {
            status: []const u8,
            n: i32,
        },
    );
    var static_conn = try StaticConnQuery.query(conn, std.testing.allocator, .{});
    defer static_conn.deinit();
    try std.testing.expectEqualStrings("compiled", static_conn.rows[0].status);
    try std.testing.expectEqual(@as(i32, 11), static_conn.rows[0].n);

    var direct_param = try conn.queryValues(std.testing.allocator, "select $1::text as status, $2::int4 as n", &.{
        .{ .text = "direct" },
        .{ .int4 = 17 },
    }, .{});
    defer direct_param.deinit();
    try std.testing.expectEqualStrings("direct", direct_param.rows[0].get(0).?);
    try std.testing.expectEqualStrings("17", direct_param.rows[0].get(1).?);

    const type_conn = try zpg.Conn.connect(std.testing.allocator, std.testing.io, &cfg);
    defer type_conn.destroy();
    const type_tz_tag = try type_conn.exec(std.testing.allocator, "set time zone 'UTC'");
    defer std.testing.allocator.free(type_tz_tag);

    var text_types = try type_conn.query(std.testing.allocator,
        \\select
        \\  '550e8400-e29b-41d4-a716-446655440000'::uuid as uuid_v,
        \\  date '2026-03-18' as d,
        \\  time '12:34:56.789012' as t,
        \\  timestamp '2026-03-18 12:34:56.789012' as ts,
        \\  timestamptz '2026-03-18 12:34:56.789012+05:30' as tstz,
        \\  '{"kind":"json"}'::json as j,
        \\  '{"kind":"jsonb"}'::jsonb as jb
    );
    defer text_types.deinit();
    try std.testing.expectEqual(@as(usize, 7), text_types.rows[0].values.len);
    try std.testing.expectEqualSlices(u8, &.{
        0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
        0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
    }, &(try text_types.decode(0, 0, [16]u8)).?);
    try std.testing.expectEqual(zpg.Date{ .year = 2026, .month = 3, .day = 18 }, (try text_types.decode(0, 1, zpg.Date)).?);
    try std.testing.expectEqual(zpg.Time{ .hour = 12, .minute = 34, .second = 56, .microsecond = 789012 }, (try text_types.decode(0, 2, zpg.Time)).?);
    try std.testing.expectEqual(zpg.Timestamp{
        .date = .{ .year = 2026, .month = 3, .day = 18 },
        .time = .{ .hour = 12, .minute = 34, .second = 56, .microsecond = 789012 },
    }, (try text_types.decode(0, 3, zpg.Timestamp)).?);
    try std.testing.expectEqual(zpg.Timestamp{
        .date = .{ .year = 2026, .month = 3, .day = 18 },
        .time = .{ .hour = 7, .minute = 4, .second = 56, .microsecond = 789012 },
    }, (try text_types.decode(0, 4, zpg.Timestamp)).?);
    try std.testing.expectEqualStrings("{\"kind\":\"json\"}", (try text_types.decode(0, 5, []const u8)).?);
    try std.testing.expectEqualStrings("{\"kind\": \"jsonb\"}", (try text_types.decode(0, 6, []const u8)).?);

    const StaticParamQuery = zpg.CompiledQuery(
        "select $1::text as status, $2::int4 as n",
        struct { []const u8, i32 },
        struct {
            status: []const u8,
            n: i32,
        },
    );
    var static_param = try StaticParamQuery.query(conn, std.testing.allocator, .{ "typed", 19 });
    defer static_param.deinit();
    try std.testing.expectEqualStrings("typed", static_param.rows[0].status);
    try std.testing.expectEqual(@as(i32, 19), static_param.rows[0].n);

    const static_tag = try StaticParamQuery.exec(conn, std.testing.allocator, .{ "exec", 23 });
    defer std.testing.allocator.free(static_tag);
    try std.testing.expectEqualStrings("SELECT 1", static_tag);

    var compiled_pipeline = conn.pipeline(.{
        .protocol = .extended,
    });
    try StaticParamQuery.queue(&compiled_pipeline, std.testing.allocator, .{ "compiled-pipe-1", 37 });
    try StaticParamQuery.queue(&compiled_pipeline, std.testing.allocator, .{ "compiled-pipe-2", 41 });
    try compiled_pipeline.flush();

    var cp1 = try StaticParamQuery.read(&compiled_pipeline, std.testing.allocator);
    defer cp1.deinit();
    try std.testing.expectEqualStrings("compiled-pipe-1", cp1.rows[0].status);
    try std.testing.expectEqual(@as(i32, 37), cp1.rows[0].n);

    var cp2 = try StaticParamQuery.read(&compiled_pipeline, std.testing.allocator);
    defer cp2.deinit();
    try std.testing.expectEqualStrings("compiled-pipe-2", cp2.rows[0].status);
    try std.testing.expectEqual(@as(i32, 41), cp2.rows[0].n);

    const TemporalParamQuery = zpg.CompiledQuery(
        "select $1::uuid as uuid_v, $2::date as d, $3::time as t, $4::timestamp as ts",
        struct { [16]u8, zpg.Date, zpg.Time, zpg.Timestamp },
        struct {
            uuid_v: [16]u8,
            d: zpg.Date,
            t: zpg.Time,
            ts: zpg.Timestamp,
        },
    );
    const uuid_v = [16]u8{
        0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
        0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
    };
    const date_v = zpg.Date{ .year = 2026, .month = 3, .day = 18 };
    const time_v = zpg.Time{ .hour = 12, .minute = 34, .second = 56, .microsecond = 789012 };
    const ts_v = zpg.Timestamp{
        .date = .{ .year = 2026, .month = 3, .day = 18 },
        .time = .{ .hour = 12, .minute = 34, .second = 56, .microsecond = 789012 },
    };
    var temporal = try TemporalParamQuery.query(conn, std.testing.allocator, .{ uuid_v, date_v, time_v, ts_v });
    defer temporal.deinit();
    try std.testing.expectEqualSlices(u8, &uuid_v, &temporal.rows[0].uuid_v);
    try std.testing.expectEqual(date_v, temporal.rows[0].d);
    try std.testing.expectEqual(time_v, temporal.rows[0].t);
    try std.testing.expectEqual(ts_v, temporal.rows[0].ts);
}

test "tls query against docker postgres" {
    if (!std.process.can_spawn) return error.SkipZigTest;
    if (!dockerUsable()) return error.SkipZigTest;
    if (!haveExecutable("/usr/bin/openssl")) return error.SkipZigTest;

    var pg = try startDockerPg(.tls);
    defer pg.deinit();

    const url = try std.fmt.allocPrint(std.testing.allocator, "postgres://postgres@localhost:{d}/postgres?sslmode=verify-full&sslrootcert={s}", .{ pg.port, pg.ca_cert_path });
    defer std.testing.allocator.free(url);

    var pool = zpg.Pool.initUri(std.testing.allocator, std.testing.io, url, 2) catch |err| {
        if (isKnownStdTlsInteropError(err)) return error.SkipZigTest;
        try reportDockerTlsFailure("pool init failed", err, pg.container_name);
        return err;
    };
    defer pool.deinit();

    var result = pool.queryValues(std.testing.allocator, "select $1::int4 + 2 as n, $2::text as label", &.{
        .{ .int4 = 5 },
        .{ .text = "tls" },
    }, .{
        .result_format = .binary,
    }) catch |err| {
        if (isKnownStdTlsInteropError(err)) return error.SkipZigTest;
        try reportDockerTlsFailure("query failed", err, pg.container_name);
        return err;
    };
    defer result.deinit();

    try std.testing.expectEqual(@as(i32, 7), (try result.decodeByName(0, "n", i32)).?);
    try std.testing.expectEqualStrings("tls", (try result.decodeByName(0, "label", []const u8)).?);

    var cfg = try zpg.Config.parseUri(std.testing.allocator, url);
    defer cfg.deinit(std.testing.allocator);
    const conn = zpg.Conn.connect(std.testing.allocator, std.testing.io, &cfg) catch |err| {
        if (isKnownStdTlsInteropError(err)) return error.SkipZigTest;
        try reportDockerTlsFailure("connect failed", err, pg.container_name);
        return err;
    };
    defer conn.destroy();

    var stmt = try conn.prepare(std.testing.allocator, "select $1::text as secure_label");
    defer stmt.deinit();
    var prepared = try stmt.queryValues(std.testing.allocator, &.{.{ .text = "prepared-tls" }}, .{
        .result_format = .binary,
    });
    defer prepared.deinit();
    try std.testing.expectEqualStrings("prepared-tls", (try prepared.decodeByName(0, "secure_label", []const u8)).?);
}

test "docker postgres requiring client cert currently fails with std tls client" {
    if (!std.process.can_spawn) return error.SkipZigTest;
    if (!dockerUsable()) return error.SkipZigTest;
    if (!haveExecutable("/usr/bin/openssl")) return error.SkipZigTest;

    var pg = try startDockerPg(.mtls_required);
    defer pg.deinit();

    const url = try std.fmt.allocPrint(std.testing.allocator, "postgres://postgres@localhost:{d}/postgres?sslmode=verify-full&sslrootcert={s}", .{ pg.port, pg.ca_cert_path });
    defer std.testing.allocator.free(url);

    var cfg = try zpg.Config.parseUri(std.testing.allocator, url);
    defer cfg.deinit(std.testing.allocator);

    if (zpg.Conn.connect(std.testing.allocator, std.testing.io, &cfg)) |conn| {
        defer conn.destroy();
        std.debug.print("docker logs for unexpected mTLS success:\n{s}\n", .{try dockerLogsAlloc(pg.container_name)});
        return error.UnexpectedTestSuccess;
    } else |err| {
        const e: anyerror = err;
        switch (e) {
            error.TlsAlert,
            error.TlsAlertHandshakeFailure,
            error.TlsAlertCertificateRequired,
            error.TlsConnectionTruncated,
            error.TlsCertificateNotVerified,
            error.TlsUnexpectedMessage,
            error.ReadFailed,
            error.EndOfStream,
            error.ConnectionResetByPeer,
            => {},
            else => {
                const logs = try dockerLogsAlloc(pg.container_name);
                defer std.testing.allocator.free(logs);
                std.debug.print("unexpected mTLS failure: {s}\ndocker logs:\n{s}\n", .{ @errorName(e), logs });
                return err;
            },
        }
    }
}

fn haveExecutable(path: []const u8) bool {
    const file = std.Io.Dir.openFileAbsolute(std.testing.io, path, .{}) catch return false;
    file.close(std.testing.io);
    return true;
}

fn dockerUsable() bool {
    if (!haveExecutable("/usr/bin/docker")) return false;
    return (spawnExitCode(&.{ "/usr/bin/docker", "ps", "--format", "{{.ID}}" }) catch 1) == 0;
}

fn startDockerPg(mode: DockerMode) !DockerPg {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);

    const root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}-docker", .{ cwd, &tmp.sub_path });
    errdefer std.testing.allocator.free(root);
    const ca_cert_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/tls/ca.crt", .{root});
    errdefer std.testing.allocator.free(ca_cert_path);
    const container_name = try std.fmt.allocPrint(std.testing.allocator, "zpg-{s}", .{&tmp.sub_path});
    errdefer std.testing.allocator.free(container_name);
    const port = 57000 + (@as(u16, tmp.sub_path[0]) << 2) + @as(u16, tmp.sub_path[1] % 200);
    const port_text = try std.fmt.allocPrint(std.testing.allocator, "{d}", .{port});
    defer std.testing.allocator.free(port_text);
    const script = try std.fmt.allocPrint(std.testing.allocator, "{s}/scripts/pg-docker-tls.sh", .{cwd});
    defer std.testing.allocator.free(script);

    try spawnExpectSuccess(&.{ "/usr/bin/bash", script, root, container_name, port_text, mode.arg() });
    errdefer _ = spawnExitCode(&.{ "/usr/bin/docker", "rm", "-f", container_name }) catch 0;
    try waitForDockerReady(container_name);

    return .{
        .root = root,
        .ca_cert_path = ca_cert_path,
        .container_name = container_name,
        .port = port,
    };
}

fn waitForDockerReady(container_name: []const u8) !void {
    for (0..120) |_| {
        const inspect = try std.process.run(std.testing.allocator, std.testing.io, .{
            .argv = &.{ "/usr/bin/docker", "inspect", "--format", "{{.State.Running}} {{.State.ExitCode}}", container_name },
            .stdout_limit = .limited(256),
            .stderr_limit = .limited(1024),
        });
        defer std.testing.allocator.free(inspect.stdout);
        defer std.testing.allocator.free(inspect.stderr);
        if (!std.mem.startsWith(u8, inspect.stdout, "true ")) {
            const logs = try dockerLogsAlloc(container_name);
            defer std.testing.allocator.free(logs);
            std.debug.print("docker postgres exited before ready:\n{s}\n", .{logs});
            return error.UnexpectedExit;
        }
        const exit_code = spawnExitCode(&.{ "/usr/bin/docker", "exec", container_name, "pg_isready", "-U", "postgres" }) catch 1;
        if (exit_code == 0) return;
        try std.Io.sleep(std.testing.io, .fromMilliseconds(250), .awake);
    }
    const logs = try dockerLogsAlloc(container_name);
    defer std.testing.allocator.free(logs);
    std.debug.print("docker postgres did not become ready:\n{s}\n", .{logs});
    return error.Timeout;
}

fn dockerLogsAlloc(container_name: []const u8) ![]u8 {
    const result = try std.process.run(std.testing.allocator, std.testing.io, .{
        .argv = &.{ "/usr/bin/docker", "logs", container_name },
        .stdout_limit = .limited(1024 * 1024),
        .stderr_limit = .limited(1024 * 1024),
    });
    defer std.testing.allocator.free(result.stderr);
    return result.stdout;
}

fn reportDockerTlsFailure(context: []const u8, err: anyerror, container_name: []const u8) !void {
    const logs = try dockerLogsAlloc(container_name);
    defer std.testing.allocator.free(logs);
    std.debug.print("{s}: {s}\ndocker logs:\n{s}\n", .{ context, @errorName(err), logs });
}

fn isKnownStdTlsInteropError(err: anyerror) bool {
    return switch (err) {
        error.TlsUnexpectedMessage,
        error.EndOfStream,
        => true,
        else => false,
    };
}

fn spawnExpectSuccess(argv: []const []const u8) !void {
    const exit_code = try spawnExitCode(argv);
    if (exit_code != 0) return error.UnexpectedCommandFailure;
}

fn spawnExitCode(argv: []const []const u8) !u8 {
    var child = try std.process.spawn(std.testing.io, .{
        .argv = argv,
        .stdin = .ignore,
        .stdout = .ignore,
        .stderr = .ignore,
    });
    const term = try child.wait(std.testing.io);
    return switch (term) {
        .exited => |code| code,
        else => 255,
    };
}

test "pg test helpers behave" {
    try std.testing.expect(haveExecutable("/usr/bin/env"));
    try std.testing.expect(!haveExecutable("/definitely/not/here"));
    try spawnExpectSuccess(&.{ "/usr/bin/env", "true" });
    try std.testing.expectError(error.UnexpectedCommandFailure, spawnExpectSuccess(&.{ "/usr/bin/env", "false" }));
}

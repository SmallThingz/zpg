const std = @import("std");
const zpg = @import("zpg");

const DockerMode = enum {
    plain,
    tls,
    mtls_required,

    fn arg(mode: DockerMode) []const u8 {
        return switch (mode) {
            .plain => "plain",
            .tls => "tls",
            .mtls_required => "mtls-required",
        };
    }
};

const DockerPg = struct {
    root: []u8,
    ca_cert_path: []u8,
    client_cert_path: []u8,
    client_key_path: []u8,
    container_name: []u8,
    port: u16,

    fn deinit(pg: *DockerPg) void {
        _ = spawnExitCode(&.{ "/usr/bin/docker", "rm", "-f", pg.container_name }) catch 0;
        var cwd = std.Io.Dir.cwd();
        cwd.deleteTree(std.testing.io, pg.root) catch {};
        std.testing.allocator.free(pg.root);
        std.testing.allocator.free(pg.ca_cert_path);
        std.testing.allocator.free(pg.client_cert_path);
        std.testing.allocator.free(pg.client_key_path);
        std.testing.allocator.free(pg.container_name);
    }
};

test {
    _ = zpg;
}

test "pool query against local postgres" {
    if (!std.process.can_spawn) return error.SkipZigTest;
    if (!dockerUsable()) return error.SkipZigTest;

    var pg = try startDockerPg(.plain);
    defer pg.deinit();

    const url = try std.fmt.allocPrint(std.testing.allocator, "postgres://postgres@localhost:{d}/postgres?sslmode=disable", .{pg.port});
    defer std.testing.allocator.free(url);

    var pool = try zpg.Pool.initUri(std.testing.allocator, std.testing.io, url, 2);
    defer pool.deinit();

    const PoolDb = zpg.Statements(.{
        .pool_query = zpg.statement(
            "pool_query",
            "select 'ok' as status, 7::int as n",
            struct {},
            struct {
                status: []const u8,
                n: i32,
            },
        ),
        .pool_exec = zpg.statement("pool_exec", "select 1", struct {}, struct {}),
    });
    const pooled_conn = try pool.acquire();

    var result = try PoolDb.stmt.pool_query.query(pooled_conn, std.testing.allocator, .{});
    defer result.deinit();
    try std.testing.expectEqualStrings("ok", result.rows[0].status);
    try std.testing.expectEqual(@as(i32, 7), result.rows[0].n);

    const tag = try PoolDb.stmt.pool_exec.exec(pooled_conn, std.testing.allocator, .{});
    defer std.testing.allocator.free(tag);
    try std.testing.expectEqualStrings("SELECT 1", tag);
    pool.release(pooled_conn);

    const stats = pool.stats();
    try std.testing.expectEqual(@as(usize, 1), stats.open);
    try std.testing.expectEqual(@as(usize, 1), stats.idle);

    var cfg = try zpg.Config.parseUri(std.testing.allocator, url);
    defer cfg.deinit(std.testing.allocator);

    const Db = zpg.Statements(.{
        .conn_status = zpg.statement("conn_status", "select 'conn' as status", struct {}, struct { status: []const u8 }),
        .pipeline_number = zpg.statement("pipeline_number", "select 7 as n", struct {}, struct { n: i32 }),
        .pipeline_label = zpg.statement("pipeline_label", "select 'pipe' as label", struct {}, struct { label: []const u8 }),
        .parameter_pipeline = zpg.statement(
            "parameter_pipeline",
            "select $1::text as label, $2::int4 as n",
            struct { []const u8, i32 },
            struct {
                label: []const u8,
                n: i32,
            },
        ),
        .parameter_pipeline_exec = zpg.statement("parameter_pipeline_exec", "select $1::int4", struct { i32 }, struct {}),
        .prepared_pipeline_label = zpg.statement(
            "prepared_pipeline_label",
            "select $1::text as prepared_label",
            struct { []const u8 },
            struct { prepared_label: []const u8 },
        ),
        .static_conn = zpg.statement(
            "static_conn",
            "select 'compiled' as status, 11::int4 as n",
            struct {},
            struct {
                status: []const u8,
                n: i32,
            },
        ),
        .static_param = zpg.statement(
            "static_param",
            "select $1::text as status, $2::int4 as n",
            struct { []const u8, i32 },
            struct {
                status: []const u8,
                n: i32,
            },
        ),
        .temporal_param = zpg.statement(
            "temporal_param",
            "select $1::uuid as uuid_v, $2::date as d, $3::time as t, $4::timestamp as ts",
            struct { [16]u8, zpg.Date, zpg.Time, zpg.Timestamp },
            struct {
                uuid_v: [16]u8,
                d: zpg.Date,
                t: zpg.Time,
                ts: zpg.Timestamp,
            },
        ),
    });
    const conn = try Db.connect(std.testing.allocator, std.testing.io, &cfg);
    defer conn.destroy();

    var conn_result = try Db.stmt.conn_status.query(conn, std.testing.allocator, .{});
    defer conn_result.deinit();
    try std.testing.expectEqualStrings("conn", conn_result.rows[0].status);

    var extended_pipeline = conn.pipeline();
    try Db.stmt.pipeline_number.queue(&extended_pipeline, .{});
    try Db.stmt.pipeline_label.queue(&extended_pipeline, .{});
    try extended_pipeline.flush();

    var e1 = try extended_pipeline.readQuery(std.testing.allocator);
    defer e1.deinit();
    try std.testing.expectEqualStrings("7", e1.rows[0].get(0).?);

    var e2 = try extended_pipeline.readQuery(std.testing.allocator);
    defer e2.deinit();
    try std.testing.expectEqualStrings("pipe", e2.rows[0].get(0).?);

    var parameter_pipeline = conn.pipeline();
    try Db.stmt.parameter_pipeline.queue(&parameter_pipeline, .{ "param-pipe", @as(i32, 29) });
    try Db.stmt.parameter_pipeline_exec.queue(&parameter_pipeline, .{@as(i32, 31)});
    try Db.stmt.prepared_pipeline_label.queue(&parameter_pipeline, .{"prepared-pipe"});
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

    var static_conn = try Db.stmt.static_conn.query(conn, std.testing.allocator, .{});
    defer static_conn.deinit();
    try std.testing.expectEqualStrings("compiled", static_conn.rows[0].status);
    try std.testing.expectEqual(@as(i32, 11), static_conn.rows[0].n);

    const TypesDb = zpg.Statements(.{
        .set_tz = zpg.statement("set_tz", "set time zone 'UTC'", struct {}, struct {}),
        .text_types = zpg.statement(
            "text_types",
            \\select
            \\  '550e8400-e29b-41d4-a716-446655440000'::uuid as uuid_v,
            \\  date '2026-03-18' as d,
            \\  time '12:34:56.789012' as t,
            \\  timestamp '2026-03-18 12:34:56.789012' as ts,
            \\  timestamptz '2026-03-18 12:34:56.789012+05:30' as tstz,
            \\  '{"kind":"json"}'::json as j,
            \\  '{"kind":"jsonb"}'::jsonb as jb
            ,
            struct {},
            struct {
                uuid_v: [16]u8,
                d: zpg.Date,
                t: zpg.Time,
                ts: zpg.Timestamp,
                tstz: zpg.Timestamp,
                j: []const u8,
                jb: []const u8,
            },
        ),
    });
    const type_conn = try TypesDb.connect(std.testing.allocator, std.testing.io, &cfg);
    defer type_conn.destroy();
    const type_tz_tag = try TypesDb.stmt.set_tz.exec(type_conn, std.testing.allocator, .{});
    defer std.testing.allocator.free(type_tz_tag);

    var text_types = try TypesDb.stmt.text_types.query(type_conn, std.testing.allocator, .{});
    defer text_types.deinit();
    try std.testing.expectEqualSlices(u8, &.{
        0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
        0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
    }, &text_types.rows[0].uuid_v);
    try std.testing.expectEqual(zpg.Date{ .year = 2026, .month = 3, .day = 18 }, text_types.rows[0].d);
    try std.testing.expectEqual(zpg.Time{ .hour = 12, .minute = 34, .second = 56, .microsecond = 789012 }, text_types.rows[0].t);
    try std.testing.expectEqual(zpg.Timestamp{
        .date = .{ .year = 2026, .month = 3, .day = 18 },
        .time = .{ .hour = 12, .minute = 34, .second = 56, .microsecond = 789012 },
    }, text_types.rows[0].ts);
    try std.testing.expectEqual(zpg.Timestamp{
        .date = .{ .year = 2026, .month = 3, .day = 18 },
        .time = .{ .hour = 7, .minute = 4, .second = 56, .microsecond = 789012 },
    }, text_types.rows[0].tstz);
    try std.testing.expectEqualStrings("{\"kind\":\"json\"}", text_types.rows[0].j);
    try std.testing.expectEqualStrings("{\"kind\": \"jsonb\"}", text_types.rows[0].jb);

    var static_param = try Db.stmt.static_param.query(conn, std.testing.allocator, .{ "typed", 19 });
    defer static_param.deinit();
    try std.testing.expectEqualStrings("typed", static_param.rows[0].status);
    try std.testing.expectEqual(@as(i32, 19), static_param.rows[0].n);

    const static_tag = try Db.stmt.static_param.exec(conn, std.testing.allocator, .{ "exec", 23 });
    defer std.testing.allocator.free(static_tag);
    try std.testing.expectEqualStrings("SELECT 1", static_tag);

    var compiled_pipeline = conn.pipeline();
    try Db.stmt.static_param.queue(&compiled_pipeline, .{ "compiled-pipe-1", 37 });
    try Db.stmt.static_param.queue(&compiled_pipeline, .{ "compiled-pipe-2", 41 });
    try compiled_pipeline.flush();

    var cp1 = try Db.stmt.static_param.read(&compiled_pipeline, std.testing.allocator);
    defer cp1.deinit();
    try std.testing.expectEqualStrings("compiled-pipe-1", cp1.rows[0].status);
    try std.testing.expectEqual(@as(i32, 37), cp1.rows[0].n);

    var cp2 = try Db.stmt.static_param.read(&compiled_pipeline, std.testing.allocator);
    defer cp2.deinit();
    try std.testing.expectEqualStrings("compiled-pipe-2", cp2.rows[0].status);
    try std.testing.expectEqual(@as(i32, 41), cp2.rows[0].n);

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
    var temporal = try Db.stmt.temporal_param.query(conn, std.testing.allocator, .{ uuid_v, date_v, time_v, ts_v });
    defer temporal.deinit();
    try std.testing.expectEqualSlices(u8, &uuid_v, &temporal.rows[0].uuid_v);
    try std.testing.expectEqual(date_v, temporal.rows[0].d);
    try std.testing.expectEqual(time_v, temporal.rows[0].t);
    try std.testing.expectEqual(ts_v, temporal.rows[0].ts);

    const prepared_conn = try Db.connect(std.testing.allocator, std.testing.io, &cfg);
    defer prepared_conn.destroy();
    var prepared_compiled = try Db.stmt.static_param.query(prepared_conn, std.testing.allocator, .{ "prewarmed", 73 });
    defer prepared_compiled.deinit();
    try std.testing.expectEqualStrings("prewarmed", prepared_compiled.rows[0].status);
    try std.testing.expectEqual(@as(i32, 73), prepared_compiled.rows[0].n);
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
        try reportDockerTlsFailure("pool init failed", err, pg.container_name);
        return err;
    };
    defer pool.deinit();

    const TlsDb = zpg.Statements(.{
        .tls_query = zpg.statement(
            "tls_query",
            "select $1::int4 + 2 as n, $2::text as label",
            struct { i32, []const u8 },
            struct { n: i32, label: []const u8 },
        ),
        .secure_label = zpg.statement(
            "secure_label",
            "select $1::text as secure_label",
            struct { []const u8 },
            struct { secure_label: []const u8 },
        ),
    });
    const tls_conn = pool.acquire() catch |err| {
        try reportDockerTlsFailure("acquire failed", err, pg.container_name);
        return err;
    };
    defer pool.release(tls_conn);
    try TlsDb.prepare(tls_conn);
    var result = TlsDb.stmt.tls_query.query(tls_conn, std.testing.allocator, .{ @as(i32, 5), "tls" }) catch |err| {
        try reportDockerTlsFailure("query failed", err, pg.container_name);
        return err;
    };
    defer result.deinit();
    try std.testing.expectEqual(@as(i32, 7), result.rows[0].n);
    try std.testing.expectEqualStrings("tls", result.rows[0].label);

    var cfg = try zpg.Config.parseUri(std.testing.allocator, url);
    defer cfg.deinit(std.testing.allocator);
    const conn = zpg.Conn.connect(std.testing.allocator, std.testing.io, &cfg) catch |err| {
        try reportDockerTlsFailure("connect failed", err, pg.container_name);
        return err;
    };
    defer conn.destroy();

    try TlsDb.prepare(conn);
    var prepared = try TlsDb.stmt.secure_label.query(conn, std.testing.allocator, .{"prepared-tls"});
    defer prepared.deinit();
    try std.testing.expectEqualStrings("prepared-tls", prepared.rows[0].secure_label);
    
    const mtls_url = try std.fmt.allocPrint(
        std.testing.allocator,
        "postgres://postgres@localhost:{d}/postgres?sslmode=verify-full&sslrootcert={s}&sslcert={s}&sslkey={s}",
        .{ pg.port, pg.ca_cert_path, pg.client_cert_path, pg.client_key_path },
    );
    defer std.testing.allocator.free(mtls_url);

    var mtls_cfg = try zpg.Config.parseUri(std.testing.allocator, mtls_url);
    defer mtls_cfg.deinit(std.testing.allocator);
    const mtls_conn = zpg.Conn.connect(std.testing.allocator, std.testing.io, &mtls_cfg) catch |err| {
        try reportDockerTlsFailure("mTLS connect failed", err, pg.container_name);
        return err;
    };
    defer mtls_conn.destroy();

    try TlsDb.prepare(mtls_conn);
    var mtls_result = try TlsDb.stmt.secure_label.query(mtls_conn, std.testing.allocator, .{"mutual-tls"});
    defer mtls_result.deinit();
    try std.testing.expectEqualStrings("mutual-tls", mtls_result.rows[0].secure_label);
}

test "docker postgres requiring client cert succeeds with configured client auth" {
    if (!std.process.can_spawn) return error.SkipZigTest;
    if (!dockerUsable()) return error.SkipZigTest;
    if (!haveExecutable("/usr/bin/openssl")) return error.SkipZigTest;

    var pg = try startDockerPg(.mtls_required);
    defer pg.deinit();

    const url = try std.fmt.allocPrint(
        std.testing.allocator,
        "postgres://postgres@localhost:{d}/postgres?sslmode=verify-full&sslrootcert={s}&sslcert={s}&sslkey={s}",
        .{ pg.port, pg.ca_cert_path, pg.client_cert_path, pg.client_key_path },
    );
    defer std.testing.allocator.free(url);

    var cfg = try zpg.Config.parseUri(std.testing.allocator, url);
    defer cfg.deinit(std.testing.allocator);

    const Db = zpg.Statements(.{
        .whoami = zpg.statement(
            "whoami",
            "select current_user as whoami",
            struct {},
            struct { whoami: []const u8 },
        ),
    });

    const conn = zpg.Conn.connect(std.testing.allocator, std.testing.io, &cfg) catch |err| {
        try reportDockerTlsFailure("mTLS connect failed", err, pg.container_name);
        return err;
    };
    defer conn.destroy();

    try Db.prepare(conn);
    var result = try Db.stmt.whoami.query(conn, std.testing.allocator, .{});
    defer result.deinit();
    try std.testing.expectEqualStrings("postgres", result.rows[0].whoami);
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
    const client_cert_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/tls/client.crt", .{root});
    errdefer std.testing.allocator.free(client_cert_path);
    const client_key_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/tls/client.key", .{root});
    errdefer std.testing.allocator.free(client_key_path);
    const container_name = try std.fmt.allocPrint(std.testing.allocator, "zpg-{s}", .{&tmp.sub_path});
    errdefer std.testing.allocator.free(container_name);
    const script = try std.fmt.allocPrint(std.testing.allocator, "{s}/scripts/pg-docker-tls.sh", .{cwd});
    defer std.testing.allocator.free(script);
    const seed = std.hash.Wyhash.hash(0, &tmp.sub_path);
    for (0..32) |attempt| {
        const port_offset: u16 = @intCast((seed +% attempt) % (65535 - 49152));
        const port: u16 = 49152 + port_offset;
        const port_text = try std.fmt.allocPrint(std.testing.allocator, "{d}", .{port});
        defer std.testing.allocator.free(port_text);

        const exit_code = try spawnExitCode(&.{ "/usr/bin/bash", script, root, container_name, port_text, mode.arg() });
        if (exit_code != 0) continue;
        errdefer _ = spawnExitCode(&.{ "/usr/bin/docker", "rm", "-f", container_name }) catch 0;
        waitForDockerReady(container_name, port) catch |err| switch (err) {
            error.UnexpectedExit, error.Timeout => {
                _ = spawnExitCode(&.{ "/usr/bin/docker", "rm", "-f", container_name }) catch 0;
                continue;
            },
            else => return err,
        };

        return .{
            .root = root,
            .ca_cert_path = ca_cert_path,
            .client_cert_path = client_cert_path,
            .client_key_path = client_key_path,
            .container_name = container_name,
            .port = port,
        };
    }
    return error.NoAvailableDockerPort;
}

fn waitForDockerReady(container_name: []const u8, port: u16) !void {
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
        const socket_ready = (spawnExitCode(&.{ "/usr/bin/docker", "exec", container_name, "pg_isready", "-U", "postgres" }) catch 1) == 0;
        const tcp_ready = (spawnExitCode(&.{ "/usr/bin/docker", "exec", container_name, "pg_isready", "-h", "127.0.0.1", "-U", "postgres" }) catch 1) == 0;
        if (socket_ready and tcp_ready and hostPortAcceptingConnections(port)) {
            try std.Io.sleep(std.testing.io, .fromMilliseconds(100), .awake);
            if ((spawnExitCode(&.{ "/usr/bin/docker", "exec", container_name, "pg_isready", "-h", "127.0.0.1", "-U", "postgres" }) catch 1) == 0) return;
        }
        try std.Io.sleep(std.testing.io, .fromMilliseconds(250), .awake);
    }
    const logs = try dockerLogsAlloc(container_name);
    defer std.testing.allocator.free(logs);
    std.debug.print("docker postgres did not become ready:\n{s}\n", .{logs});
    return error.Timeout;
}

fn hostPortAcceptingConnections(port: u16) bool {
    const cmd = std.fmt.allocPrint(std.testing.allocator, "exec 3<>/dev/tcp/127.0.0.1/{d}", .{port}) catch return false;
    defer std.testing.allocator.free(cmd);
    return (spawnExitCode(&.{ "/usr/bin/bash", "-lc", cmd }) catch 1) == 0;
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

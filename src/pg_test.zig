const std = @import("std");
const zpg = @import("zpg");

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

    var result = try pool.queryAlloc(std.testing.allocator, "select 'ok' as status, 7::int as n");
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
}

fn haveExecutable(path: []const u8) bool {
    const file = std.Io.Dir.openFileAbsolute(std.testing.io, path, .{}) catch return false;
    file.close(std.testing.io);
    return true;
}

fn spawnExpectSuccess(argv: []const []const u8) !void {
    var child = try std.process.spawn(std.testing.io, .{
        .argv = argv,
        .stdin = .ignore,
        .stdout = .ignore,
        .stderr = .ignore,
    });
    const term = try child.wait(std.testing.io);
    switch (term) {
        .exited => |code| if (code == 0) return,
        else => {},
    }
    return error.UnexpectedCommandFailure;
}

test "pg test helpers behave" {
    try std.testing.expect(haveExecutable("/usr/bin/env"));
    try std.testing.expect(!haveExecutable("/definitely/not/here"));
    try spawnExpectSuccess(&.{ "/usr/bin/env", "true" });
    try std.testing.expectError(error.UnexpectedCommandFailure, spawnExpectSuccess(&.{ "/usr/bin/env", "false" }));
}

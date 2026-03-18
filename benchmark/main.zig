const std = @import("std");
const zpg = @import("zpg");

const WorkerStats = struct {
    count: usize = 0,
    total_ns: i128 = 0,
    max_ns: i128 = 0,
};

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.smp_allocator;
    var args = init.minimal.args.iterate();
    _ = args.next();
    const workers = parseArg(args.next(), @max(std.Thread.getCpuCount() catch 4, 1));
    const per_worker = parseArg(args.next(), 10_000);
    const max_pool = parseArg(args.next(), workers);
    const sql = args.next() orelse "select 1";

    var tmp = try makeTempRoot(init.io, allocator);
    defer tmp.deinit(init.io, allocator);

    try spawnExpectSuccess(init.io, &.{ "/usr/bin/initdb", "-D", tmp.data_dir, "--auth=trust", "--username=postgres" });
    try spawnExpectSuccess(init.io, &.{ "/usr/bin/pg_ctl", "-D", tmp.data_dir, "-o", tmp.options, "-w", "start" });
    defer spawnExpectSuccess(init.io, &.{ "/usr/bin/pg_ctl", "-D", tmp.data_dir, "-m", "immediate", "-w", "stop" }) catch {};

    var pool = try zpg.Pool.initUri(allocator, init.io, tmp.url, max_pool);
    defer pool.deinit();

    const stats = try allocator.alloc(WorkerStats, workers);
    defer allocator.free(stats);
    @memset(stats, .{});

    const threads = try allocator.alloc(std.Thread, workers);
    defer allocator.free(threads);

    const started = std.Io.Timestamp.now(init.io, .real);
    for (threads, stats) |*thread, *stat| {
        thread.* = try std.Thread.spawn(.{}, workerMain, .{ init.io, &pool, per_worker, sql, stat });
    }
    for (threads) |thread| thread.join();
    const elapsed = started.untilNow(init.io, .real).toNanoseconds();

    var total_count: usize = 0;
    var total_ns: i128 = 0;
    var max_ns: i128 = 0;
    for (stats) |stat| {
        total_count += stat.count;
        total_ns += stat.total_ns;
        max_ns = @max(max_ns, stat.max_ns);
    }

    const avg_ns = if (total_count == 0) 0 else @divTrunc(total_ns, @as(i128, @intCast(total_count)));
    const qps = if (elapsed <= 0) 0 else (@as(f64, @floatFromInt(total_count)) * @as(f64, @floatFromInt(std.time.ns_per_s))) / @as(f64, @floatFromInt(elapsed));

    var stdout_buffer: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    try stdout.interface.print(
        "workers={d} per_worker={d} pool={d} total={d} wall_ms={d} qps={d:.2} avg_us={d} max_us={d}\n",
        .{
            workers,
            per_worker,
            max_pool,
            total_count,
            @divTrunc(elapsed, std.time.ns_per_ms),
            qps,
            @divTrunc(avg_ns, std.time.ns_per_us),
            @divTrunc(max_ns, std.time.ns_per_us),
        },
    );
    try stdout.interface.flush();
}

fn workerMain(io: std.Io, pool: *zpg.Pool, per_worker: usize, sql: []const u8, stat: *WorkerStats) !void {
    for (0..per_worker) |_| {
        const started = std.Io.Timestamp.now(io, .real);
        var result = try pool.query(std.heap.smp_allocator, sql);
        result.deinit();
        const elapsed = started.untilNow(io, .real).toNanoseconds();
        stat.count += 1;
        stat.total_ns += elapsed;
        stat.max_ns = @max(stat.max_ns, elapsed);
    }
}

const TempRoot = struct {
    root: []u8,
    data_dir: []u8,
    options: []u8,
    url: []u8,

    fn deinit(tmp: *TempRoot, io: std.Io, allocator: std.mem.Allocator) void {
        var cwd = std.Io.Dir.cwd();
        cwd.deleteTree(io, tmp.root) catch {};
        allocator.free(tmp.root);
        allocator.free(tmp.data_dir);
        allocator.free(tmp.options);
        allocator.free(tmp.url);
    }
};

fn makeTempRoot(io: std.Io, allocator: std.mem.Allocator) !TempRoot {
    var random_bytes: [8]u8 = undefined;
    try io.randomSecure(&random_bytes);
    const suffix = std.fmt.bytesToHex(random_bytes, .lower);
    const root = try std.fmt.allocPrint(allocator, "/tmp/cloud-zig-pg-bench-{s}", .{suffix});
    var cwd = std.Io.Dir.cwd();
    var root_dir = try cwd.createDirPathOpen(io, root, .{});
    root_dir.close(io);
    const data_dir = try std.fmt.allocPrint(allocator, "{s}/data", .{root});
    const port = 56000 + @as(u16, random_bytes[0]);
    const options = try std.fmt.allocPrint(allocator, "-k /tmp -p {d}", .{port});
    const url = try std.fmt.allocPrint(allocator, "postgres://postgres@127.0.0.1:{d}/postgres?sslmode=disable", .{port});
    return .{
        .root = root,
        .data_dir = data_dir,
        .options = options,
        .url = url,
    };
}

fn spawnExpectSuccess(io: std.Io, argv: []const []const u8) !void {
    var child = try std.process.spawn(io, .{
        .argv = argv,
        .stdin = .ignore,
        .stdout = .ignore,
        .stderr = .ignore,
    });
    const term = try child.wait(io);
    switch (term) {
        .exited => |code| if (code == 0) return,
        else => {},
    }
    return error.UnexpectedCommandFailure;
}

fn parseArg(text: ?[]const u8, fallback: usize) usize {
    const s = text orelse return fallback;
    return std.fmt.parseInt(usize, s, 10) catch fallback;
}

const std = @import("std");

pub const BenchmarkMode = enum {
    compare,
    latency,
    throughput,
};

pub const ZpgVariant = enum {
    simple,
    extended,
};

pub const SummaryMeta = struct {
    warmup: usize = 0,
    workers: usize = 1,
    pool: usize = 0,
    connections: usize = 0,
    pipeline_depth: usize = 1,
};

pub const Summary = struct {
    driver: []const u8,
    mode: BenchmarkMode,
    variant: []const u8,
    requests: usize,
    warmup: usize,
    workers: usize,
    pool: usize,
    connections: usize,
    pipeline_depth: usize,
    wall_ns: u64,
    qps: f64,
    min_ns: u64,
    avg_ns: u64,
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    max_ns: u64,

    pub fn print(summary: Summary) void {
        std.debug.print(
            "bench driver={s} mode={s} variant={s} requests={d} warmup={d} workers={d} pool={d} connections={d} pipeline_depth={d} wall_ms={d} qps={d:.2} min_us={d} avg_us={d} p50_us={d} p95_us={d} p99_us={d} max_us={d}\n",
            .{
                summary.driver,
                modeName(summary.mode),
                summary.variant,
                summary.requests,
                summary.warmup,
                summary.workers,
                summary.pool,
                summary.connections,
                summary.pipeline_depth,
                @divTrunc(summary.wall_ns, std.time.ns_per_ms),
                summary.qps,
                @divTrunc(summary.min_ns, std.time.ns_per_us),
                @divTrunc(summary.avg_ns, std.time.ns_per_us),
                @divTrunc(summary.p50_ns, std.time.ns_per_us),
                @divTrunc(summary.p95_ns, std.time.ns_per_us),
                @divTrunc(summary.p99_ns, std.time.ns_per_us),
                @divTrunc(summary.max_ns, std.time.ns_per_us),
            },
        );
    }
};

pub const TempPostgres = struct {
    root: []u8,
    data_dir: []u8,
    options: []u8,
    url: []u8,

    pub fn init(io: std.Io, allocator: std.mem.Allocator) !TempPostgres {
        var random_bytes: [8]u8 = undefined;
        try io.randomSecure(&random_bytes);
        const suffix = std.fmt.bytesToHex(random_bytes, .lower);
        const root = try std.fmt.allocPrint(allocator, "/tmp/zpg-bench-{s}", .{suffix});
        errdefer allocator.free(root);

        var cwd = std.Io.Dir.cwd();
        var root_dir = try cwd.createDirPathOpen(io, root, .{});
        root_dir.close(io);

        const data_dir = try std.fmt.allocPrint(allocator, "{s}/data", .{root});
        errdefer allocator.free(data_dir);

        const port = 56000 + @as(u16, random_bytes[0]);
        const options = try std.fmt.allocPrint(allocator, "-k /tmp -p {d}", .{port});
        errdefer allocator.free(options);

        const url = try std.fmt.allocPrint(allocator, "postgres://postgres@127.0.0.1:{d}/postgres?sslmode=disable&connect_timeout=0", .{port});
        errdefer allocator.free(url);

        try spawnExpectSuccess(io, &.{ "/usr/bin/initdb", "-D", data_dir, "--auth=trust", "--username=postgres" });
        errdefer cwd.deleteTree(io, root) catch {};

        try spawnExpectSuccess(io, &.{ "/usr/bin/pg_ctl", "-D", data_dir, "-o", options, "-w", "start" });

        return .{
            .root = root,
            .data_dir = data_dir,
            .options = options,
            .url = url,
        };
    }

    pub fn deinit(tmp: *TempPostgres, io: std.Io, allocator: std.mem.Allocator) void {
        spawnExpectSuccess(io, &.{ "/usr/bin/pg_ctl", "-D", tmp.data_dir, "-m", "immediate", "-w", "stop" }) catch {};
        var cwd = std.Io.Dir.cwd();
        cwd.deleteTree(io, tmp.root) catch {};
        allocator.free(tmp.root);
        allocator.free(tmp.data_dir);
        allocator.free(tmp.options);
        allocator.free(tmp.url);
        tmp.* = undefined;
    }
};

pub fn modeName(mode: BenchmarkMode) []const u8 {
    return switch (mode) {
        .compare => "compare",
        .latency => "latency",
        .throughput => "throughput",
    };
}

pub fn variantName(variant: ZpgVariant) []const u8 {
    return switch (variant) {
        .simple => "simple",
        .extended => "extended",
    };
}

pub fn summarize(samples: []u64, wall_ns: u64, driver: []const u8, mode: BenchmarkMode, variant: []const u8, meta: SummaryMeta) Summary {
    if (samples.len == 0) {
        return .{
            .driver = driver,
            .mode = mode,
            .variant = variant,
            .requests = 0,
            .warmup = meta.warmup,
            .workers = meta.workers,
            .pool = meta.pool,
            .connections = meta.connections,
            .pipeline_depth = meta.pipeline_depth,
            .wall_ns = wall_ns,
            .qps = 0,
            .min_ns = 0,
            .avg_ns = 0,
            .p50_ns = 0,
            .p95_ns = 0,
            .p99_ns = 0,
            .max_ns = 0,
        };
    }

    std.mem.sortUnstable(u64, samples, {}, std.sort.asc(u64));

    var total_ns: u128 = 0;
    for (samples) |sample| total_ns += sample;

    return .{
        .driver = driver,
        .mode = mode,
        .variant = variant,
        .requests = samples.len,
        .warmup = meta.warmup,
        .workers = meta.workers,
        .pool = meta.pool,
        .connections = meta.connections,
        .pipeline_depth = meta.pipeline_depth,
        .wall_ns = wall_ns,
        .qps = if (wall_ns == 0) 0 else (@as(f64, @floatFromInt(samples.len)) * @as(f64, @floatFromInt(std.time.ns_per_s))) / @as(f64, @floatFromInt(wall_ns)),
        .min_ns = samples[0],
        .avg_ns = @intCast(total_ns / samples.len),
        .p50_ns = percentile(samples, 50, 100),
        .p95_ns = percentile(samples, 95, 100),
        .p99_ns = percentile(samples, 99, 100),
        .max_ns = samples[samples.len - 1],
    };
}

pub fn printComparison(mode: BenchmarkMode, baseline: Summary, contender: Summary) void {
    std.debug.print(
        "compare mode={s} baseline={s}/{s} contender={s}/{s} qps_ratio={d:.2} avg_latency_speedup={d:.2} p99_latency_speedup={d:.2}\n",
        .{
            modeName(mode),
            baseline.driver,
            baseline.variant,
            contender.driver,
            contender.variant,
            ratio(contender.qps, baseline.qps),
            inverseRatio(contender.avg_ns, baseline.avg_ns),
            inverseRatio(contender.p99_ns, baseline.p99_ns),
        },
    );
}

pub fn parseSummaryLine(line: []const u8, mode: BenchmarkMode, driver: []const u8, variant: []const u8) !Summary {
    var summary = Summary{
        .driver = driver,
        .mode = mode,
        .variant = variant,
        .requests = 0,
        .warmup = 0,
        .workers = 0,
        .pool = 0,
        .connections = 0,
        .pipeline_depth = 0,
        .wall_ns = 0,
        .qps = 0,
        .min_ns = 0,
        .avg_ns = 0,
        .p50_ns = 0,
        .p95_ns = 0,
        .p99_ns = 0,
        .max_ns = 0,
    };

    var tokens = std.mem.tokenizeScalar(u8, std.mem.trim(u8, line, " \t\r\n"), ' ');
    while (tokens.next()) |token| {
        if (std.mem.eql(u8, token, "bench")) continue;

        const eq = std.mem.indexOfScalar(u8, token, '=') orelse continue;
        const key = token[0..eq];
        const value = token[eq + 1 ..];

        if (std.mem.eql(u8, key, "requests")) {
            summary.requests = try std.fmt.parseInt(usize, value, 10);
        } else if (std.mem.eql(u8, key, "warmup")) {
            summary.warmup = try std.fmt.parseInt(usize, value, 10);
        } else if (std.mem.eql(u8, key, "workers")) {
            summary.workers = try std.fmt.parseInt(usize, value, 10);
        } else if (std.mem.eql(u8, key, "pool")) {
            summary.pool = try std.fmt.parseInt(usize, value, 10);
        } else if (std.mem.eql(u8, key, "connections")) {
            summary.connections = try std.fmt.parseInt(usize, value, 10);
        } else if (std.mem.eql(u8, key, "pipeline_depth")) {
            summary.pipeline_depth = try std.fmt.parseInt(usize, value, 10);
        } else if (std.mem.eql(u8, key, "wall_ms")) {
            const wall_ms = try std.fmt.parseInt(u64, value, 10);
            summary.wall_ns = wall_ms * std.time.ns_per_ms;
        } else if (std.mem.eql(u8, key, "qps")) {
            summary.qps = try std.fmt.parseFloat(f64, value);
        } else if (std.mem.eql(u8, key, "min_us")) {
            summary.min_ns = try parseScaledNs(value);
        } else if (std.mem.eql(u8, key, "avg_us")) {
            summary.avg_ns = try parseScaledNs(value);
        } else if (std.mem.eql(u8, key, "p50_us")) {
            summary.p50_ns = try parseScaledNs(value);
        } else if (std.mem.eql(u8, key, "p95_us")) {
            summary.p95_ns = try parseScaledNs(value);
        } else if (std.mem.eql(u8, key, "p99_us")) {
            summary.p99_ns = try parseScaledNs(value);
        } else if (std.mem.eql(u8, key, "max_us")) {
            summary.max_ns = try parseScaledNs(value);
        }
    }

    return summary;
}

pub fn spawnExpectSuccess(io: std.Io, argv: []const []const u8) !void {
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

fn percentile(sorted: []const u64, numerator: usize, denominator: usize) u64 {
    const index = ((sorted.len - 1) * numerator) / denominator;
    return sorted[index];
}

fn ratio(numerator: f64, denominator: f64) f64 {
    if (denominator == 0) return 0;
    return numerator / denominator;
}

fn inverseRatio(latency_ns: u64, baseline_ns: u64) f64 {
    if (latency_ns == 0) return 0;
    return @as(f64, @floatFromInt(baseline_ns)) / @as(f64, @floatFromInt(latency_ns));
}

fn parseScaledNs(text: []const u8) !u64 {
    return (try std.fmt.parseInt(u64, text, 10)) * std.time.ns_per_us;
}

const std = @import("std");
const zpg = @import("zpg");
const common = @import("common.zig");

const Options = struct {
    mode: common.BenchmarkMode,
    latency_iterations: usize,
    latency_warmup: usize,
    throughput_workers: usize,
    throughput_per_worker: usize,
    throughput_pool: usize,
    throughput_warmup: usize,
    throughput_pipeline_depth: usize,
    throughput_shim_connections: usize,
    shim_binary: ?[]const u8 = null,
    sql: []const u8 = "select 1",
    help: bool = false,

    fn defaults() Options {
        const cpu_count = std.Thread.getCpuCount() catch 4;
        const workers = @max(cpu_count, 1);
        return .{
            .mode = .compare,
            .latency_iterations = 10_000,
            .latency_warmup = 1_000,
            .throughput_workers = workers,
            .throughput_per_worker = 10_000,
            .throughput_pool = workers,
            .throughput_warmup = 512,
            .throughput_pipeline_depth = 128,
            .throughput_shim_connections = workers,
        };
    }

    fn normalize(opts: *Options) void {
        opts.throughput_workers = @max(opts.throughput_workers, 1);
        opts.throughput_pool = @max(opts.throughput_pool, 1);
        opts.throughput_pipeline_depth = @max(opts.throughput_pipeline_depth, 1);
        opts.throughput_shim_connections = @max(opts.throughput_shim_connections, 1);
    }
};

const ThroughputGate = struct {
    ready: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    start: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
};

const ThroughputWorkerArgs = struct {
    io: std.Io,
    pool: *zpg.Pool,
    sql: []const u8,
    variant: common.ZpgVariant,
    warmup: usize,
    samples: []u64,
    gate: *ThroughputGate,
};

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.smp_allocator;
    var options = try parseArgs(allocator, init);
    defer if (options.shim_binary) |path| allocator.free(path);

    if (options.help) {
        printUsage();
        return;
    }

    const shim_binary = options.shim_binary orelse return error.MissingShimBinary;

    var tmp = try common.TempPostgres.init(init.io, allocator);
    defer tmp.deinit(init.io, allocator);

    switch (options.mode) {
        .compare => {
            try runLatencySuite(allocator, init.io, tmp.url, shim_binary, options);
            std.debug.print("\n", .{});
            try runThroughputSuite(allocator, init.io, tmp.url, shim_binary, options);
        },
        .latency => try runLatencySuite(allocator, init.io, tmp.url, shim_binary, options),
        .throughput => try runThroughputSuite(allocator, init.io, tmp.url, shim_binary, options),
    }
}

fn parseArgs(allocator: std.mem.Allocator, init: std.process.Init) !Options {
    var options = Options.defaults();
    var args = init.minimal.args.iterate();
    _ = args.next();

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help")) {
            options.help = true;
        } else if (std.mem.eql(u8, arg, "--mode")) {
            options.mode = try parseMode(args.next() orelse return error.MissingArgumentValue);
        } else if (std.mem.eql(u8, arg, "--latency-iterations")) {
            options.latency_iterations = try parseUnsigned(args.next() orelse return error.MissingArgumentValue);
        } else if (std.mem.eql(u8, arg, "--latency-warmup")) {
            options.latency_warmup = try parseUnsigned(args.next() orelse return error.MissingArgumentValue);
        } else if (std.mem.eql(u8, arg, "--throughput-workers")) {
            options.throughput_workers = try parseUnsigned(args.next() orelse return error.MissingArgumentValue);
        } else if (std.mem.eql(u8, arg, "--throughput-per-worker")) {
            options.throughput_per_worker = try parseUnsigned(args.next() orelse return error.MissingArgumentValue);
        } else if (std.mem.eql(u8, arg, "--throughput-pool")) {
            options.throughput_pool = try parseUnsigned(args.next() orelse return error.MissingArgumentValue);
        } else if (std.mem.eql(u8, arg, "--throughput-warmup")) {
            options.throughput_warmup = try parseUnsigned(args.next() orelse return error.MissingArgumentValue);
        } else if (std.mem.eql(u8, arg, "--throughput-pipeline-depth")) {
            options.throughput_pipeline_depth = try parseUnsigned(args.next() orelse return error.MissingArgumentValue);
        } else if (std.mem.eql(u8, arg, "--throughput-shim-connections")) {
            options.throughput_shim_connections = try parseUnsigned(args.next() orelse return error.MissingArgumentValue);
        } else if (std.mem.eql(u8, arg, "--shim-binary")) {
            const value = args.next() orelse return error.MissingArgumentValue;
            if (options.shim_binary) |existing| allocator.free(existing);
            options.shim_binary = try allocator.dupe(u8, value);
        } else if (std.mem.eql(u8, arg, "--sql")) {
            options.sql = args.next() orelse return error.MissingArgumentValue;
        } else {
            return error.UnknownArgument;
        }
    }

    options.normalize();
    return options;
}

fn runLatencySuite(allocator: std.mem.Allocator, io: std.Io, url: []const u8, shim_binary: []const u8, options: Options) !void {
    std.debug.print("suite mode=latency sql={s}\n", .{options.sql});

    const simple = try runZpgLatency(allocator, io, url, options.sql, options.latency_warmup, options.latency_iterations, .simple);
    simple.print();

    const extended = try runZpgLatency(allocator, io, url, options.sql, options.latency_warmup, options.latency_iterations, .extended);
    extended.print();

    const shim = try runShimBenchmark(allocator, io, shim_binary, .latency, url, options);
    shim.print();

    common.printComparison(.latency, simple, shim);
    common.printComparison(.latency, extended, shim);
}

fn runThroughputSuite(allocator: std.mem.Allocator, io: std.Io, url: []const u8, shim_binary: []const u8, options: Options) !void {
    std.debug.print("suite mode=throughput sql={s}\n", .{options.sql});

    const simple = try runZpgThroughput(
        allocator,
        io,
        url,
        options.sql,
        options.throughput_warmup,
        options.throughput_workers,
        options.throughput_per_worker,
        options.throughput_pool,
        .simple,
    );
    simple.print();

    const extended = try runZpgThroughput(
        allocator,
        io,
        url,
        options.sql,
        options.throughput_warmup,
        options.throughput_workers,
        options.throughput_per_worker,
        options.throughput_pool,
        .extended,
    );
    extended.print();

    const shim = try runShimBenchmark(allocator, io, shim_binary, .throughput, url, options);
    shim.print();

    common.printComparison(.throughput, simple, shim);
    common.printComparison(.throughput, extended, shim);
}

fn runZpgLatency(
    allocator: std.mem.Allocator,
    io: std.Io,
    url: []const u8,
    sql: []const u8,
    warmup: usize,
    iterations: usize,
    variant: common.ZpgVariant,
) !common.Summary {
    var config = try zpg.Config.parseUri(allocator, url);
    const conn = try zpg.Conn.connect(allocator, io, &config);

    const query_options = queryOptions(variant);
    for (0..warmup) |_| {
        var result = try conn.queryOpts(allocator, sql, query_options);
        result.deinit();
    }

    const samples = try allocator.alloc(u64, iterations);
    defer allocator.free(samples);

    const started = std.Io.Timestamp.now(io, .real);
    for (samples) |*sample| {
        const op_started = std.Io.Timestamp.now(io, .real);
        var result = try conn.queryOpts(allocator, sql, query_options);
        result.deinit();
        sample.* = nanosSince(op_started, io);
    }

    return common.summarize(samples, nanosSince(started, io), "zpg", .latency, common.variantName(variant), .{
        .warmup = warmup,
        .workers = 1,
        .connections = 1,
    });
}

fn runZpgThroughput(
    allocator: std.mem.Allocator,
    io: std.Io,
    url: []const u8,
    sql: []const u8,
    warmup: usize,
    workers: usize,
    per_worker: usize,
    pool_size: usize,
    variant: common.ZpgVariant,
) !common.Summary {
    var pool = try zpg.Pool.initUri(allocator, io, url, pool_size);

    const total_requests = workers * per_worker;
    const samples = try allocator.alloc(u64, total_requests);
    defer allocator.free(samples);

    const threads = try allocator.alloc(std.Thread, workers);
    defer allocator.free(threads);

    var gate = ThroughputGate{};
    for (threads, 0..) |*thread, i| {
        const start_index = i * per_worker;
        thread.* = try std.Thread.spawn(.{}, throughputWorkerMain, .{ThroughputWorkerArgs{
            .io = io,
            .pool = &pool,
            .sql = sql,
            .variant = variant,
            .warmup = warmup,
            .samples = samples[start_index .. start_index + per_worker],
            .gate = &gate,
        }});
    }

    // Hold all workers at a single barrier so the wall clock reflects only the hot run.
    while (gate.ready.load(.acquire) != workers) std.atomic.spinLoopHint();

    const started = std.Io.Timestamp.now(io, .real);
    gate.start.store(true, .release);

    for (threads) |thread| thread.join();

    return common.summarize(samples, nanosSince(started, io), "zpg", .throughput, common.variantName(variant), .{
        .warmup = warmup,
        .workers = workers,
        .pool = pool_size,
        .connections = pool_size,
    });
}

fn throughputWorkerMain(args: ThroughputWorkerArgs) !void {
    const query_options = queryOptions(args.variant);
    for (0..args.warmup) |_| {
        var result = try args.pool.queryOpts(std.heap.smp_allocator, args.sql, query_options);
        result.deinit();
    }

    _ = args.gate.ready.fetchAdd(1, .acq_rel);
    while (!args.gate.start.load(.acquire)) std.atomic.spinLoopHint();

    for (args.samples) |*sample| {
        const op_started = std.Io.Timestamp.now(args.io, .real);
        var result = try args.pool.queryOpts(std.heap.smp_allocator, args.sql, query_options);
        result.deinit();
        sample.* = nanosSince(op_started, args.io);
    }
}

fn runShimBenchmark(
    allocator: std.mem.Allocator,
    io: std.Io,
    shim_binary: []const u8,
    mode: common.BenchmarkMode,
    url: []const u8,
    options: Options,
) !common.Summary {
    var argv: std.ArrayList([]const u8) = .empty;
    defer argv.deinit(allocator);

    var owned_args: std.ArrayList([]u8) = .empty;
    defer {
        for (owned_args.items) |item| allocator.free(item);
        owned_args.deinit(allocator);
    }

    try argv.append(allocator, shim_binary);
    try argv.append(allocator, "--mode");
    try argv.append(allocator, common.modeName(mode));
    try argv.append(allocator, "--url");
    try argv.append(allocator, url);
    try argv.append(allocator, "--sql");
    try argv.append(allocator, options.sql);

    switch (mode) {
        .latency => {
            try appendOwnedIntArg(allocator, &argv, &owned_args, "--iterations", options.latency_iterations);
            try appendOwnedIntArg(allocator, &argv, &owned_args, "--warmup", options.latency_warmup);
        },
        .throughput => {
            try appendOwnedIntArg(allocator, &argv, &owned_args, "--requests", options.throughput_workers * options.throughput_per_worker);
            try appendOwnedIntArg(allocator, &argv, &owned_args, "--warmup", options.throughput_warmup);
            try appendOwnedIntArg(allocator, &argv, &owned_args, "--connections", options.throughput_shim_connections);
            try appendOwnedIntArg(allocator, &argv, &owned_args, "--pipeline-depth", options.throughput_pipeline_depth);
        },
        .compare => return error.InvalidBenchmarkMode,
    }

    const result = try std.process.run(allocator, io, .{
        .argv = argv.items,
    });
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    switch (result.term) {
        .exited => |code| {
            if (code != 0) {
                if (result.stderr.len != 0) std.debug.print("{s}\n", .{result.stderr});
                if (result.stdout.len != 0) std.debug.print("{s}\n", .{result.stdout});
                return error.BenchmarkShimFailed;
            }
        },
        else => return error.BenchmarkShimFailed,
    }

    const line = firstSummaryLine(result.stdout) orelse {
        if (result.stderr.len != 0) std.debug.print("{s}\n", .{result.stderr});
        if (result.stdout.len != 0) std.debug.print("{s}\n", .{result.stdout});
        return error.InvalidBenchmarkOutput;
    };

    return common.parseSummaryLine(line, mode, "libpq-pipeline-shim", "pipeline");
}

fn appendOwnedIntArg(
    allocator: std.mem.Allocator,
    argv: *std.ArrayList([]const u8),
    owned_args: *std.ArrayList([]u8),
    key: []const u8,
    value: usize,
) !void {
    try argv.append(allocator, key);
    const text = try std.fmt.allocPrint(allocator, "{d}", .{value});
    try owned_args.append(allocator, text);
    try argv.append(allocator, text);
}

fn firstSummaryLine(output: []const u8) ?[]const u8 {
    var lines = std.mem.splitScalar(u8, output, '\n');
    while (lines.next()) |line| {
        const trimmed = std.mem.trim(u8, line, " \t\r");
        if (trimmed.len == 0) continue;
        if (std.mem.startsWith(u8, trimmed, "bench ")) return trimmed;
    }
    return null;
}

fn queryOptions(variant: common.ZpgVariant) zpg.QueryOptions {
    return .{
        .protocol = switch (variant) {
            .simple => .simple,
            .extended => .extended,
        },
    };
}

fn parseMode(text: []const u8) !common.BenchmarkMode {
    if (std.mem.eql(u8, text, "compare")) return .compare;
    if (std.mem.eql(u8, text, "latency")) return .latency;
    if (std.mem.eql(u8, text, "throughput")) return .throughput;
    return error.InvalidBenchmarkMode;
}

fn parseUnsigned(text: []const u8) !usize {
    return std.fmt.parseInt(usize, text, 10);
}

fn nanosSince(started: std.Io.Timestamp, io: std.Io) u64 {
    return @intCast(started.untilNow(io, .real).toNanoseconds());
}

fn printUsage() void {
    std.debug.print(
        \\usage: zpg-benchmark --shim-binary <path> [options]
        \\
        \\  --mode compare|latency|throughput
        \\  --sql <query>
        \\  --latency-iterations <n>
        \\  --latency-warmup <n>
        \\  --throughput-workers <n>
        \\  --throughput-per-worker <n>
        \\  --throughput-pool <n>
        \\  --throughput-warmup <n>
        \\  --throughput-pipeline-depth <n>
        \\  --throughput-shim-connections <n>
        \\
        \\`zig build benchmark` injects `--shim-binary` automatically.
        \\
        ,
        .{},
    );
}

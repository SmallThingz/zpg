const std = @import("std");
const builtin = @import("builtin");
const zpg = @import("zpg");
const common = @import("common.zig");

const benchmark_sql = "select 1";
const BenchDb = zpg.Statements(.{
    .ping = zpg.statement(
        "ping",
        benchmark_sql,
        struct {},
        struct { n: i32 },
    ),
});

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
    sql: []const u8 = benchmark_sql,
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

const PipelineWorkerArgs = struct {
    io: std.Io,
    url: []const u8,
    variant: common.ZpgVariant,
    warmup: usize,
    depth: usize,
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
    if (!std.mem.eql(u8, options.sql, benchmark_sql)) return error.UnsupportedDynamicBenchmarkSql;

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

    const prepared = try runZpgLatency(allocator, io, url, options.latency_warmup, options.latency_iterations);
    prepared.print();

    const shim = try runShimBenchmark(allocator, io, shim_binary, .latency, url, options);
    shim.print();

    common.printComparison(.latency, prepared, shim);
}

fn runThroughputSuite(allocator: std.mem.Allocator, io: std.Io, url: []const u8, shim_binary: []const u8, options: Options) !void {
    std.debug.print("suite mode=throughput sql={s}\n", .{options.sql});
    const total_requests = options.throughput_workers * options.throughput_per_worker;
    const pipeline_connections = @min(options.throughput_workers, options.throughput_pool);

    const prepared = try runZpgPipelinedThroughput(
        allocator,
        io,
        url,
        options.throughput_warmup,
        total_requests,
        pipeline_connections,
        options.throughput_pipeline_depth,
        .prepared_pipeline,
    );
    prepared.print();

    const shim = try runShimBenchmark(allocator, io, shim_binary, .throughput, url, options);
    shim.print();

    common.printComparison(.throughput, prepared, shim);
}

fn runZpgLatency(
    allocator: std.mem.Allocator,
    io: std.Io,
    url: []const u8,
    warmup: usize,
    iterations: usize,
) !common.Summary {
    var config = try zpg.Config.parseUri(allocator, url);
    defer config.deinit(allocator);
    const conn = try BenchDb.connect(allocator, io, &config);
    defer conn.destroy();

    for (0..warmup) |_| {
        var result = try BenchDb.stmt.ping.query(conn, allocator, .{});
        result.deinit();
    }

    const samples = try allocator.alloc(u64, iterations);
    defer allocator.free(samples);

    const started = monoNowNs();
    for (samples) |*sample| {
        const op_started = monoNowNs();
        var result = try BenchDb.stmt.ping.query(conn, allocator, .{});
        result.deinit();
        sample.* = monoNowNs() - op_started;
    }

    return common.summarize(samples, monoNowNs() - started, "zpg", .latency, common.variantName(.prepared), .{
        .warmup = warmup,
        .workers = 1,
        .connections = 1,
    });
}

fn runZpgPipelinedThroughput(
    allocator: std.mem.Allocator,
    io: std.Io,
    url: []const u8,
    warmup: usize,
    requests: usize,
    connections: usize,
    depth: usize,
    variant: common.ZpgVariant,
) !common.Summary {
    const samples = try allocator.alloc(u64, requests);
    defer allocator.free(samples);

    const threads = try allocator.alloc(std.Thread, connections);
    defer allocator.free(threads);

    var gate = ThroughputGate{};
    var assigned: usize = 0;
    for (threads, 0..) |*thread, i| {
        const remaining_threads = connections - i;
        const remaining_requests = requests - assigned;
        const worker_requests = @divFloor(remaining_requests + remaining_threads - 1, remaining_threads);
        thread.* = try std.Thread.spawn(.{}, pipelineWorkerMain, .{PipelineWorkerArgs{
            .io = io,
            .url = url,
            .variant = variant,
            .warmup = warmup,
            .depth = depth,
            .samples = samples[assigned .. assigned + worker_requests],
            .gate = &gate,
        }});
        assigned += worker_requests;
    }

    while (gate.ready.load(.acquire) != connections) std.atomic.spinLoopHint();

    const started = monoNowNs();
    gate.start.store(true, .release);

    for (threads) |thread| thread.join();

    return common.summarize(samples, monoNowNs() - started, "zpg", .throughput, common.variantName(variant), .{
        .warmup = warmup,
        .workers = connections,
        .connections = connections,
        .pipeline_depth = depth,
    });
}

fn pipelineWorkerMain(args: PipelineWorkerArgs) !void {
    const allocator = std.heap.page_allocator;
    var config = try zpg.Config.parseUri(allocator, args.url);
    defer config.deinit(allocator);
    const conn = try BenchDb.connect(allocator, args.io, &config);
    defer conn.destroy();

    try runPipeline(args.io, conn, args.variant, args.warmup, args.depth, null);

    _ = args.gate.ready.fetchAdd(1, .acq_rel);
    while (!args.gate.start.load(.acquire)) std.atomic.spinLoopHint();

    try runPipeline(args.io, conn, args.variant, args.samples.len, args.depth, args.samples);
}

fn runPipeline(io: std.Io, conn: *zpg.Conn, variant: common.ZpgVariant, count: usize, depth: usize, maybe_samples: ?[]u64) !void {
    _ = io;
    if (count == 0) return;

    _ = variant;
    var pipeline = conn.pipeline();
    var sent: usize = 0;
    var completed: usize = 0;

    while (completed < count) {
        const batch_end = @min(sent + depth, count);
        while (sent < batch_end) {
            if (maybe_samples) |samples| samples[sent] = monoNowNs();
            try BenchDb.stmt.ping.queue(&pipeline, .{});
            sent += 1;
        }
        try pipeline.flush();

        while (completed < batch_end) {
            try pipeline.discard();
            if (maybe_samples) |samples| {
                samples[completed] = monoNowNs() - samples[completed];
            }
            completed += 1;
        }
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

fn parseMode(text: []const u8) !common.BenchmarkMode {
    if (std.mem.eql(u8, text, "compare")) return .compare;
    if (std.mem.eql(u8, text, "latency")) return .latency;
    if (std.mem.eql(u8, text, "throughput")) return .throughput;
    return error.InvalidBenchmarkMode;
}

fn parseUnsigned(text: []const u8) !usize {
    return std.fmt.parseInt(usize, text, 10);
}

fn monoNowNs() u64 {
    if (builtin.os.tag == .linux) {
        var ts: std.os.linux.timespec = undefined;
        if (std.os.linux.clock_gettime(std.os.linux.CLOCK.MONOTONIC, &ts) == 0) {
            return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
        }
    }
    return 0;
}

fn printUsage() void {
    std.debug.print(
        \\usage: zpg-benchmark --shim-binary <path> [options]
        \\
        \\  --mode compare|latency|throughput
        \\  --sql <query>              currently only `select 1` is supported
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

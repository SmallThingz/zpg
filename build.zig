const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const tls_dep = b.dependency("tls", .{
        .target = target,
        .optimize = optimize,
    });
    const tls_mod = tls_dep.module("tls");

    const mod = b.addModule("zpg", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
    });
    mod.addImport("tls", tls_mod);

    const mod_tests = b.addTest(.{
        .root_module = mod,
        .test_runner = .{ .path = b.path("test_runner.zig"), .mode = .simple },
    });
    const run_mod_tests = b.addRunArtifact(mod_tests);

    const integration_tests = b.addTest(.{
        .name = "zpg-integration-tests",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/pg_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zpg", .module = mod },
            },
        }),
        .test_runner = .{ .path = b.path("test_runner.zig"), .mode = .simple },
    });
    const run_integration_tests = b.addRunArtifact(integration_tests);
    run_integration_tests.addArgs(&.{ "--jobs", "1" });
    run_integration_tests.step.dependOn(&run_mod_tests.step);

    integration_tests.step.dependOn(&mod_tests.step);
    const test_compile_step = b.step("test-compile", "Build test binaries without running them");
    test_compile_step.dependOn(&integration_tests.step);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_integration_tests.step);

    const smoke_exe = b.addExecutable(.{
        .name = "zpg-smoke",
        .root_module = b.createModule(.{
            .root_source_file = b.path("examples/smoke.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zpg", .module = mod },
            },
        }),
    });
    const example_smoke_step = b.step("example-smoke", "Build the smoke example");
    example_smoke_step.dependOn(&smoke_exe.step);

    const bench_exe = b.addExecutable(.{
        .name = "zpg-benchmark",
        .root_module = b.createModule(.{
            .root_source_file = b.path("benchmark/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zpg", .module = mod },
            },
        }),
    });
    bench_exe.root_module.addImport("common", b.createModule(.{
        .root_source_file = b.path("benchmark/common.zig"),
        .target = target,
        .optimize = optimize,
    }));

    const libpq_shim_module = b.createModule(.{
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .link_libcpp = true,
    });
    libpq_shim_module.addCSourceFile(.{
        .file = b.path("benchmark/libpq_pipeline_shim.cpp"),
        .flags = &.{ "-std=c++20", "-pthread" },
        .language = .cpp,
    });
    libpq_shim_module.linkSystemLibrary("pq", .{ .use_pkg_config = .yes });
    libpq_shim_module.linkSystemLibrary("pthread", .{});

    const libpq_shim_exe = b.addExecutable(.{
        .name = "zpg-libpq-pipeline-shim",
        .root_module = libpq_shim_module,
    });

    const bench_run = b.addRunArtifact(bench_exe);
    bench_run.addArg("--shim-binary");
    bench_run.addArtifactArg(libpq_shim_exe);
    if (b.args) |args| bench_run.addArgs(args);
    const bench_step = b.step("benchmark", "Run latency and throughput benchmarks against zpg and the libpq pipeline shim");
    bench_step.dependOn(&bench_run.step);

    const bench_latency_run = b.addRunArtifact(bench_exe);
    bench_latency_run.addArg("--shim-binary");
    bench_latency_run.addArtifactArg(libpq_shim_exe);
    bench_latency_run.addArgs(&.{ "--mode", "latency" });
    if (b.args) |args| bench_latency_run.addArgs(args);
    const bench_latency_step = b.step("benchmark-latency", "Run the latency benchmark suite");
    bench_latency_step.dependOn(&bench_latency_run.step);

    const bench_throughput_run = b.addRunArtifact(bench_exe);
    bench_throughput_run.addArg("--shim-binary");
    bench_throughput_run.addArtifactArg(libpq_shim_exe);
    bench_throughput_run.addArgs(&.{ "--mode", "throughput" });
    if (b.args) |args| bench_throughput_run.addArgs(args);
    const bench_throughput_step = b.step("benchmark-throughput", "Run the throughput benchmark suite");
    bench_throughput_step.dependOn(&bench_throughput_run.step);
}

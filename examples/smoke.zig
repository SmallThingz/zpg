const std = @import("std");
const zpg = @import("zpg");

const SmokeDb = zpg.Statements(.{
    .smoke = zpg.statement(
        "smoke",
        "select 1 as one",
        struct {},
        struct { one: i32 },
    ),
});

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.smp_allocator;
    const url = init.environ_map.get("POSTGRES_URL") orelse return error.MissingPostgresUrl;
    var cfg = try zpg.Config.parseUri(allocator, url);
    defer cfg.deinit(allocator);

    const conn = try SmokeDb.connect(allocator, init.io, &cfg);
    defer conn.destroy();

    var result = try SmokeDb.stmt.smoke.query(conn, allocator, .{});
    defer result.deinit();

    var stdout_buffer: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    try stdout.interface.print("rows={d} one={d}\n", .{ result.rows.len, result.rows[0].one });
    try stdout.interface.flush();
}

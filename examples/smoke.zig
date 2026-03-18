const std = @import("std");
const zpg = @import("zpg");

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.smp_allocator;
    const url = init.environ_map.get("POSTGRES_URL") orelse return error.MissingPostgresUrl;
    var args = init.minimal.args.iterate();
    _ = args.next();
    const sql = args.next() orelse "select 1 as one";

    var pool = try zpg.Pool.initUri(allocator, init.io, url, 2);
    defer pool.deinit();

    var result = try pool.query(allocator, sql);
    defer result.deinit();

    var stdout_buffer: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    try stdout.interface.print("command={s} cols={d} rows={d}\n", .{
        result.command_tag,
        result.columns.len,
        result.rows.len,
    });
    for (result.rows) |row| {
        for (row.values, 0..) |value, i| {
            if (i != 0) try stdout.interface.writeAll(",");
            try stdout.interface.writeAll(value orelse "NULL");
        }
        try stdout.interface.writeAll("\n");
    }
    try stdout.interface.flush();
}

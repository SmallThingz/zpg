const std = @import("std");

const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;
const Sha256 = std.crypto.hash.sha2.Sha256;

pub const Client = struct {
    nonce: [24]u8,
    client_first_bare: [29]u8 = undefined,
    client_first: [32]u8 = undefined,
    auth_message: [512]u8 = undefined,
    auth_len: usize = 0,
    password: []const u8,
    salted_password: [32]u8 = undefined,

    pub fn init(io: std.Io, password: []const u8) !Client {
        var random_bytes: [18]u8 = undefined;
        try io.randomSecure(&random_bytes);
        var nonce: [24]u8 = undefined;
        _ = std.base64.standard.Encoder.encode(&nonce, &random_bytes);
        var client: Client = .{
            .nonce = nonce,
            .password = password,
        };
        _ = try std.fmt.bufPrint(&client.client_first_bare, "n=,r={s}", .{&client.nonce});
        _ = try std.fmt.bufPrint(&client.client_first, "n,,n=,r={s}", .{&client.nonce});
        return client;
    }

    pub fn initialMessage(client: *const Client) []const u8 {
        return &client.client_first;
    }

    pub fn serverFirst(client: *Client, message: []const u8, out: *[256]u8) ![]const u8 {
        var nonce: ?[]const u8 = null;
        var salt_b64: ?[]const u8 = null;
        var iterations_text: ?[]const u8 = null;
        var it = std.mem.splitScalar(u8, message, ',');
        while (it.next()) |part| {
            if (part.len < 3) continue;
            switch (part[0]) {
                'r' => nonce = part[2..],
                's' => salt_b64 = part[2..],
                'i' => iterations_text = part[2..],
                else => {},
            }
        }
        const server_nonce = nonce orelse return error.InvalidScramServerFirst;
        if (!std.mem.startsWith(u8, server_nonce, &client.nonce)) return error.InvalidScramNonce;
        const salt_input = salt_b64 orelse return error.InvalidScramServerFirst;
        const iterations = std.fmt.parseInt(u32, iterations_text orelse return error.InvalidScramServerFirst, 10) catch {
            return error.InvalidScramServerFirst;
        };

        var salt_bytes: [128]u8 = undefined;
        const salt_len = try std.base64.standard.Decoder.calcSizeForSlice(salt_input);
        _ = try std.base64.standard.Decoder.decode(salt_bytes[0..salt_len], salt_input);
        try std.crypto.pwhash.pbkdf2(&client.salted_password, client.password, salt_bytes[0..salt_len], iterations, HmacSha256);

        var client_key: [32]u8 = undefined;
        HmacSha256.create(&client_key, "Client Key", &client.salted_password);
        var stored_key: [32]u8 = undefined;
        Sha256.hash(&client_key, &stored_key, .{});

        client.auth_len = (try std.fmt.bufPrint(
            &client.auth_message,
            "{s},{s},c=biws,r={s}",
            .{ &client.client_first_bare, message, server_nonce },
        )).len;

        var client_signature: [32]u8 = undefined;
        HmacSha256.create(&client_signature, client.auth_message[0..client.auth_len], &stored_key);
        for (&client_key, 0..) |*b, i| b.* ^= client_signature[i];

        var proof_b64: [44]u8 = undefined;
        const proof = std.base64.standard.Encoder.encode(&proof_b64, &client_key);
        return std.fmt.bufPrint(out, "c=biws,r={s},p={s}", .{ server_nonce, proof });
    }

    pub fn verifyServerFinal(client: *Client, message: []const u8) !void {
        if (!std.mem.startsWith(u8, message, "v=")) return error.InvalidScramServerFinal;
        const verifier_b64 = message[2..];
        var verifier: [64]u8 = undefined;
        const verifier_len = try std.base64.standard.Decoder.calcSizeForSlice(verifier_b64);
        _ = try std.base64.standard.Decoder.decode(verifier[0..verifier_len], verifier_b64);
        var server_key: [32]u8 = undefined;
        HmacSha256.create(&server_key, "Server Key", &client.salted_password);
        var expected: [32]u8 = undefined;
        HmacSha256.create(&expected, client.auth_message[0..client.auth_len], &server_key);
        if (!std.mem.eql(u8, verifier[0..verifier_len], &expected)) return error.InvalidScramServerFinal;
    }
};

test "scram roundtrip vector" {
    var client: Client = .{
        .nonce = "9IZ2O01zb9IgiIZ1WJ/zgpJB".*,
        .password = "foobar",
    };
    _ = try std.fmt.bufPrint(&client.client_first_bare, "n=,r={s}", .{&client.nonce});
    _ = try std.fmt.bufPrint(&client.client_first, "n,,n=,r={s}", .{&client.nonce});

    try std.testing.expectEqualStrings("n,,n=,r=9IZ2O01zb9IgiIZ1WJ/zgpJB", client.initialMessage());

    var out: [256]u8 = undefined;
    const client_final = try client.serverFirst("r=9IZ2O01zb9IgiIZ1WJ/zgpJBjx/oIRLs02gGSHcw1KEty3eY,s=fs3IXBy7U7+IvVjZ,i=4096", &out);
    try std.testing.expectEqualStrings(
        "c=biws,r=9IZ2O01zb9IgiIZ1WJ/zgpJBjx/oIRLs02gGSHcw1KEty3eY,p=AmNKosjJzS31NTlQYNs5BTeQjdHdk7lOflDo5re2an8=",
        client_final,
    );
    try client.verifyServerFinal("v=U+ppxD5XUKtradnv8e2MkeupiA8FU87Sg8CXzXHDAzw=");
}

test "scram fuzz invalid messages stay bounded" {
    const io = std.testing.io;
    for (0..1000) |seed| {
        var client = try Client.init(io, "secret");
        var prng = std.Random.DefaultPrng.init(seed);
        const random = prng.random();
        var msg: [96]u8 = undefined;
        const len = random.intRangeLessThan(usize, 0, msg.len);
        random.bytes(msg[0..len]);
        var out: [256]u8 = undefined;
        _ = client.serverFirst(msg[0..len], &out) catch {};
        _ = client.verifyServerFinal(msg[0..len]) catch {};
    }
}

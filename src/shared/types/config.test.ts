import assert from "node:assert/strict";
import { test } from "node:test";
import {
	normalizeNetworkProxyConfig,
	normalizeProxyBypass,
	normalizeProxyConfig,
	redactProxyConfig,
	redactProxySettings,
	redactProxyUrl,
} from "./config.ts";

test("migrates flattened P2P routes when a sync payload has a default nested profile", () => {
	const normalized = normalizeNetworkProxyConfig({
		proxy: normalizeProxyConfig({ http: { enable: false } }),
		"p2p-proxy": "socks5://tcp.example:1080",
		"p2p-no-proxy": "tcp.example",
		"p2p-udp-proxy": "socks5h://udp.example:1080",
		"p2p-udp-no-proxy": "udp.example",
	});

	assert.deepEqual(
		(normalized.proxy as ReturnType<typeof normalizeProxyConfig>).p2p,
		{
			enable: true,
			server: "socks5://tcp.example:1080",
			bypass: "tcp.example",
			udp: {
				server: "socks5h://udp.example:1080",
				bypass: "udp.example",
			},
		},
	);
});

test("keeps a configured nested P2P profile over flattened sync keys", () => {
	const normalized = normalizeNetworkProxyConfig({
		proxy: normalizeProxyConfig({
			p2p: { enable: true, server: "socks5://nested.example:1080" },
		}),
		"p2p-proxy": "socks5://legacy.example:1080",
	});

	assert.equal(
		(normalized.proxy as ReturnType<typeof normalizeProxyConfig>).p2p.server,
		"socks5://nested.example:1080",
	);
});

test("keeps an empty UDP override inheriting the TCP route during sync", () => {
	const tcpServer = "socks5://tcp.example:1080";
	const normalized = normalizeNetworkProxyConfig({
		proxy: normalizeProxyConfig({
			p2p: {
				enable: true,
				server: tcpServer,
				bypass: "tcp.example",
				udp: { server: "", bypass: "" },
			},
		}),
		"p2p-proxy": tcpServer,
		"p2p-no-proxy": "tcp.example",
		"p2p-udp-proxy": tcpServer,
		"p2p-udp-no-proxy": "tcp.example",
	});

	assert.deepEqual(
		(normalized.proxy as ReturnType<typeof normalizeProxyConfig>).p2p.udp,
		{ server: "", bypass: "" },
	);
});

test("normalizes legacy proxy settings into the HTTP profile", () => {
	assert.deepEqual(
		normalizeProxyConfig({
			enable: "true",
			server: " http://proxy.example:8080 ",
			bypass: " .Example.com, example.com ",
			scope: ["download", "download", "invalid"],
		}),
		{
			http: {
				enable: true,
				server: "http://proxy.example:8080",
				bypass: "example.com",
				scope: ["download"],
			},
			p2p: {
				enable: false,
				server: "",
				bypass: "",
				udp: { server: "", bypass: "" },
			},
		},
	);
});

test("fills nested defaults and normalizes profiles independently", () => {
	const value = normalizeProxyConfig({
		http: { scope: "DOWNLOAD, update-trackers", bypass: "EXAMPLE.com" },
		p2p: { enable: "on", server: "socks5h://proxy.example:1080" },
	});
	assert.deepEqual(value.http.scope, ["download", "update-trackers"]);
	assert.equal(value.http.bypass, "example.com");
	assert.equal(value.p2p.enable, true);
	assert.deepEqual(value.p2p, {
		enable: true,
		server: "socks5h://proxy.example:1080",
		bypass: "",
		udp: { server: "", bypass: "" },
	});
});

test("normalizes the optional UDP profile independently from the TCP profile", () => {
	const value = normalizeProxyConfig({
		p2p: {
			enable: true,
			server: "http://tcp.example:8080",
			bypass: "TCP.example,127.0.0.1",
			udp: {
				server: " socks5h://udp.example:1080 ",
				bypass: " UDP.example, udp.example:443 ",
			},
		},
	});
	assert.deepEqual(value.p2p, {
		enable: true,
		server: "http://tcp.example:8080",
		bypass: "tcp.example,127.0.0.1",
		udp: {
			server: "socks5h://udp.example:1080",
			bypass: "udp.example,udp.example:443",
		},
	});
});

test("fills UDP defaults for malformed or omitted nested profiles", () => {
	assert.deepEqual(normalizeProxyConfig({ p2p: { udp: "invalid" } }).p2p.udp, {
		server: "",
		bypass: "",
	});
	assert.deepEqual(normalizeProxyConfig({ p2p: { udp: null } }).p2p.udp, {
		server: "",
		bypass: "",
	});
});

test("splits delimited scope values inside arrays", () => {
	assert.deepEqual(
		normalizeProxyConfig({
			http: { scope: ["download, update-app", "update-trackers"] },
		}).http.scope,
		["download", "update-app", "update-trackers"],
	);
});

test("treats a null scope as omitted and rejects signed bypass ports", () => {
	const value = normalizeProxyConfig({
		http: { scope: null, bypass: "example.com:+80,example.org:8080" },
	});
	assert.deepEqual(value.http.scope, [
		"download",
		"update-app",
		"update-trackers",
	]);
	assert.equal(value.http.bypass, "example.org:8080");
});

test("migrates legacy HTTP fields even when a P2P profile already exists", () => {
	const value = normalizeProxyConfig({
		enable: true,
		server: "http://legacy.example:8080",
		p2p: { enable: true, server: "socks5://p2p.example:1080" },
	});
	assert.equal(value.http.enable, true);
	assert.equal(value.http.server, "http://legacy.example:8080");
	assert.equal(value.p2p.server, "socks5://p2p.example:1080");
});

test("merges partial nested HTTP fields over legacy values", () => {
	const value = normalizeProxyConfig({
		enable: true,
		server: "http://legacy.example:8080",
		bypass: "legacy.example",
		scope: ["download", "update-app"],
		http: { bypass: "nested.example" },
	});
	assert.deepEqual(value.http, {
		enable: true,
		server: "http://legacy.example:8080",
		bypass: "nested.example",
		scope: ["download", "update-app"],
	});
});

test("normalizes bypass entries and redacts proxy credentials", () => {
	assert.equal(
		normalizeProxyBypass(" .Example.com,EXAMPLE.com:443\n127.0.0.1 "),
		"example.com,example.com:443,127.0.0.1",
	);
	assert.equal(
		redactProxyUrl("http://alice:s3cret@proxy.example:8080"),
		"http://proxy.example:8080/",
	);
	assert.equal(
		redactProxyUrl("alice:s3cret@proxy.example:8080"),
		"<invalid proxy>",
	);
	const redacted = redactProxyConfig({
		http: { server: "http://alice:s3cret@proxy.example:8080" },
		p2p: {
			server: "socks5://bob:secret@proxy.example:1080",
			udp: { server: "socks5h://carol:udp-secret@udp.example:1080" },
		},
	});
	assert.equal(redacted.http.server.includes("s3cret"), false);
	assert.equal(redacted.p2p.server.includes("secret"), false);
	assert.equal(redacted.p2p.udp.server.includes("udp-secret"), false);
	const settings = redactProxySettings({
		"all-proxy": "http://alice:s3cret@proxy.example:8080",
		"p2p-proxy": "socks5://bob:secret@proxy.example:1080",
		"p2p-udp-proxy": "socks5h://carol:udp-secret@udp.example:1080",
	});
	assert.equal(settings["all-proxy"], "http://proxy.example:8080/");
	assert.equal(settings["p2p-proxy"], "socks5://proxy.example:1080");
	assert.equal(settings["p2p-udp-proxy"], "socks5h://udp.example:1080");
	const camelSettings = redactProxySettings({
		allProxy: "http://alice:s3cret@proxy.example:8080",
		p2pProxy: "socks5://bob:secret@proxy.example:1080",
		p2pUdpProxy: "socks5h://carol:udp-secret@udp.example:1080",
		pbhRpcSecret: "pbh-token-secret",
		rpcSecret: "rpc-token-secret",
	});
	assert.equal(camelSettings.allProxy, "http://proxy.example:8080/");
	assert.equal(camelSettings.p2pProxy, "socks5://proxy.example:1080");
	assert.equal(camelSettings.p2pUdpProxy, "socks5h://udp.example:1080");
	assert.equal(camelSettings.pbhRpcSecret, "[REDACTED]");
	assert.equal(camelSettings.rpcSecret, "[REDACTED]");
	assert.notEqual(
		JSON.stringify(camelSettings).includes("pbh-token-secret"),
		true,
	);
	const secrets = redactProxySettings({
		cloudSyncToken: "session-token",
		savedCredentials: [{ password: "hunter2" }],
		"ftp-passwd": "ftp-secret",
		sftpPrivateKeyPassphrase: "key-secret",
		header: "Cookie: sid=abc",
		p2PProxy: "socks5://bob:secret@proxy.example:1080",
		dir: "/downloads",
	});
	assert.equal(secrets.p2PProxy, "socks5://proxy.example:1080");
	assert.equal(secrets.dir, "/downloads");
	assert.equal(
		/session-token|hunter2|ftp-secret|key-secret|sid=abc/.test(
			JSON.stringify(secrets),
		),
		false,
	);
});

test("normalizes IP networks and rejects malformed bypass entries", () => {
	assert.equal(
		normalizeProxyBypass("10.4.3.2/8, 2001:db8:0:1::/36, [2001:db8::]/32:8443"),
		"10.0.0.0/8,2001:db8::/36,[2001:db8::]/32:8443",
	);
	assert.equal(
		normalizeProxyBypass(
			"bad host, *.example.com, example.com:0, 10.0.0.0/33, [not-a-host], [2001:db8::]/32:8443:extra, 001.002.003.004",
		),
		"",
	);
});

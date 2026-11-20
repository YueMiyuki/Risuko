import assert from "node:assert/strict";
import { test } from "node:test";
import {
	isPrivateTracker,
	isSecretOption,
	keepLocalSyncSecrets,
	remoteNeedsScrub,
	stripProxyCredentials,
	stripUrlCredentials,
	withoutSyncSecrets,
} from "./syncSecrets.ts";

test("user and password are stripped from a proxy URL", () => {
	assert.equal(
		stripProxyCredentials("http://user:pass@proxy.example:8080"),
		"http://proxy.example:8080",
	);
});

test("a user without a password is stripped and the path is kept", () => {
	assert.equal(
		stripProxyCredentials("socks5://user@10.0.0.1:1080/"),
		"socks5://10.0.0.1:1080/",
	);
});

test("credentials are stripped from an address without a scheme", () => {
	assert.equal(
		stripProxyCredentials("user:pass@proxy.example:8080"),
		"proxy.example:8080",
	);
});

test("everything up to the last @ is stripped", () => {
	assert.equal(stripProxyCredentials("http://u:p@ss@host:1"), "http://host:1");
});

test("a password containing a slash is stripped from a proxy address", () => {
	assert.equal(
		stripProxyCredentials("http://user:pa/ss@proxy.example:8080"),
		"http://proxy.example:8080",
	);
});

test("a proxy address with credentials is trimmed before stripping", () => {
	assert.equal(stripProxyCredentials("  socks5://u:p@h:1"), "socks5://h:1");
});

test("a proxy address without credentials is not trimmed", () => {
	assert.equal(stripProxyCredentials("  http://h:1 "), "  http://h:1 ");
});

test("an address without credentials is unchanged", () => {
	assert.equal(
		stripProxyCredentials("http://proxy.example:8080"),
		"http://proxy.example:8080",
	);
});

test("an @ anywhere in a proxy address marks credentials", () => {
	assert.equal(stripProxyCredentials("http://host/path@x"), "http://x");
});

test("an empty address is unchanged", () => {
	assert.equal(stripProxyCredentials(""), "");
});

test("an @ after the authority of a URL is not treated as credentials", () => {
	assert.equal(stripUrlCredentials("http://host/path@x"), "http://host/path@x");
});

test("credentials are stripped from the authority of a URL", () => {
	assert.equal(
		stripUrlCredentials("https://u:p@site.example/a@b"),
		"https://site.example/a@b",
	);
});

test("password-like and header options are secret", () => {
	for (const key of [
		"http-passwd",
		"all-proxy-passwd",
		"rpc-secret",
		"header",
		"load-cookies",
	]) {
		assert.equal(isSecretOption(key), true, key);
	}
	for (const key of ["split", "dir", "all-proxy", "user-agent"]) {
		assert.equal(isSecretOption(key), false, key);
	}
});

test("network data loses proxy credentials without being mutated", () => {
	const data = {
		"all-proxy": "http://u:p@h:1",
		"p2p-proxy": "socks5://u@h:2",
		"p2p-udp-proxy": "u:p@h:3",
		"no-proxy": "a@b",
		proxy: {
			server: "http://u:p@legacy:1",
			http: {
				enable: true,
				server: "http://u:p@web:1",
				bypass: "x@y",
				scope: ["download"],
			},
			p2p: {
				enable: true,
				server: "socks5://u:p@tcp:1",
				bypass: "",
				udp: { server: "socks5://u:p@udp:1", bypass: "" },
			},
		},
	};
	const before = structuredClone(data);
	assert.deepEqual(withoutSyncSecrets("network", data), {
		"all-proxy": "http://h:1",
		"p2p-proxy": "socks5://h:2",
		"p2p-udp-proxy": "h:3",
		"no-proxy": "a@b",
		proxy: {
			server: "http://legacy:1",
			http: {
				enable: true,
				server: "http://web:1",
				bypass: "x@y",
				scope: ["download"],
			},
			p2p: {
				enable: true,
				server: "socks5://tcp:1",
				bypass: "",
				udp: { server: "socks5://udp:1", bypass: "" },
			},
		},
	});
	assert.deepEqual(data, before);
});

test("engine overrides lose secret entries and proxy credentials", () => {
	assert.deepEqual(
		withoutSyncSecrets("engine", {
			"engine-overrides": {
				split: 8,
				"http-passwd": "x",
				header: "A: b",
				"all-proxy": "http://u:p@h:1",
			},
			"rpc-listen-port": 16800,
		}),
		{
			"engine-overrides": { split: 8, "all-proxy": "http://h:1" },
			"rpc-listen-port": 16800,
		},
	);
});

test("proxy overrides are stripped even without a scheme", () => {
	assert.deepEqual(
		withoutSyncSecrets("engine", {
			"engine-overrides": {
				"all-proxy": "user:pass@host:1",
				"http-proxy": "http://u:pa/ss@h:2",
			},
		}),
		{
			"engine-overrides": { "all-proxy": "host:1", "http-proxy": "http://h:2" },
		},
	);
});

test("other override URLs keep an @ after the authority", () => {
	assert.deepEqual(
		withoutSyncSecrets("engine", {
			"engine-overrides": {
				referer: "https://u:p@site.example/a@b",
				"user-agent": "me@example",
			},
		}),
		{
			"engine-overrides": {
				referer: "https://site.example/a@b",
				"user-agent": "me@example",
			},
		},
	);
});

test("other categories are returned unchanged", () => {
	const data = { split: 8, "user-agent": "x", referer: "http://u:p@h:1" };
	assert.deepEqual(withoutSyncSecrets("download", data), {
		split: 8,
		"user-agent": "x",
		referer: "http://u:p@h:1",
	});
});

test("a local proxy address is kept when it matches after stripping", () => {
	assert.deepEqual(
		keepLocalSyncSecrets(
			"network",
			{ "all-proxy": "http://u:p@h:1" },
			{ "all-proxy": "http://h:1" },
		),
		{ "all-proxy": "http://u:p@h:1" },
	);
});

test("a different incoming proxy address wins", () => {
	assert.deepEqual(
		keepLocalSyncSecrets(
			"network",
			{ "all-proxy": "http://u:p@h:1" },
			{ "all-proxy": "http://other:1" },
		),
		{ "all-proxy": "http://other:1" },
	);
});

test("a local proxy password with a slash is kept when the address matches", () => {
	assert.deepEqual(
		keepLocalSyncSecrets(
			"network",
			{ "all-proxy": "http://u:pa/ss@h:1" },
			{ "all-proxy": "http://h:1" },
		),
		{ "all-proxy": "http://u:pa/ss@h:1" },
	);
});

test("credentials uploaded by an older client are not imported", () => {
	assert.deepEqual(
		keepLocalSyncSecrets("network", {}, { "all-proxy": "http://x:y@h:1" }),
		{ "all-proxy": "http://h:1" },
	);
});

test("a nested local proxy server is kept when it matches after stripping", () => {
	const proxy = (udpServer: string) => ({
		http: { enable: false, server: "", bypass: "", scope: [] },
		p2p: {
			enable: true,
			server: "",
			bypass: "",
			udp: { server: udpServer, bypass: "" },
		},
	});
	assert.deepEqual(
		keepLocalSyncSecrets(
			"network",
			{ proxy: proxy("socks5://u:p@udp:1") },
			{ proxy: proxy("socks5://udp:1") },
		),
		{ proxy: proxy("socks5://u:p@udp:1") },
	);
});

test("local secret engine overrides are kept and incoming ones dropped", () => {
	assert.deepEqual(
		keepLocalSyncSecrets(
			"engine",
			{ "engine-overrides": { "http-passwd": "x", split: 4 } },
			{ "engine-overrides": { split: 8, "ftp-passwd": "leak" } },
		),
		{ "engine-overrides": { split: 8, "http-passwd": "x" } },
	);
});

test("a local engine override address is kept only when it matches after stripping", () => {
	const local = { "engine-overrides": { "all-proxy": "http://u:p@h:1" } };
	assert.deepEqual(
		keepLocalSyncSecrets("engine", local, {
			"engine-overrides": { "all-proxy": "http://h:1" },
		}),
		{ "engine-overrides": { "all-proxy": "http://u:p@h:1" } },
	);
	assert.deepEqual(
		keepLocalSyncSecrets("engine", local, {
			"engine-overrides": { "all-proxy": "http://other:1" },
		}),
		{ "engine-overrides": { "all-proxy": "http://other:1" } },
	);
});

test("a local proxy override without a scheme is kept when it matches", () => {
	assert.deepEqual(
		keepLocalSyncSecrets(
			"engine",
			{ "engine-overrides": { "all-proxy": "user:pass@host:1" } },
			{ "engine-overrides": { "all-proxy": "host:1" } },
		),
		{ "engine-overrides": { "all-proxy": "user:pass@host:1" } },
	);
});

test("a local override URL is matched with the URL rule", () => {
	assert.deepEqual(
		keepLocalSyncSecrets(
			"engine",
			{ "engine-overrides": { referer: "https://u:p@site.example/a@b" } },
			{ "engine-overrides": { referer: "https://site.example/a@b" } },
		),
		{ "engine-overrides": { referer: "https://u:p@site.example/a@b" } },
	);
});

test("engine overrides are not added when the incoming data lacks them", () => {
	assert.deepEqual(
		keepLocalSyncSecrets(
			"engine",
			{ "engine-overrides": { "http-passwd": "x" } },
			{ "rpc-listen-port": 1 },
		),
		{ "rpc-listen-port": 1 },
	);
});

test("a clean remote category needs no scrub", () => {
	const data = { "all-proxy": "http://h:1", "no-proxy": "" };
	assert.equal(remoteNeedsScrub("network", data, { ...data }), false);
});

test("an unknown remote key does not need a scrub", () => {
	assert.equal(
		remoteNeedsScrub(
			"download",
			{ split: 8 },
			{ split: 8, "future-setting": true },
		),
		false,
	);
});

test("a remote secret key needs a scrub", () => {
	assert.equal(
		remoteNeedsScrub(
			"ftp",
			{ "ftp-user": "me" },
			{ "ftp-user": "me", "ftp-passwd": "x" },
		),
		true,
	);
});

test("a remote proxy address with credentials needs a scrub", () => {
	const data = { "all-proxy": "http://u:p@h:1" };
	assert.equal(remoteNeedsScrub("network", data, { ...data }), true);
});

test("a remote secret engine override needs a scrub", () => {
	const data = { "engine-overrides": { split: 8, "http-passwd": "x" } };
	assert.equal(remoteNeedsScrub("engine", data, { ...data }), true);
});

const PASSKEY = "0123456789abcdef0123";
const SHORT_PASSKEY = "0123456789abcdef012";

test("a tracker URL with user information is private", () => {
	assert.equal(
		isPrivateTracker("https://user:pass@tracker.example/announce"),
		true,
	);
	assert.equal(
		isPrivateTracker("http://user@tracker.example:6969/announce"),
		true,
	);
});

test("a tracker URL with a query string is private", () => {
	assert.equal(
		isPrivateTracker("https://tracker.example/announce.php?passkey=abc"),
		true,
	);
	assert.equal(isPrivateTracker("http://tracker.example/announce?uk=1"), true);
	assert.equal(isPrivateTracker("udp://tracker.example:1/announce?"), true);
});

test("a tracker URL with a long path segment is private", () => {
	assert.equal(
		isPrivateTracker(`https://tracker.example/${PASSKEY}/announce`),
		true,
	);
	assert.equal(
		isPrivateTracker(`https://tracker.example/announce/${PASSKEY}`),
		true,
	);
	assert.equal(
		isPrivateTracker("wss://tracker.example/a_b-c_d-e_f-g_h-i_j-k/announce"),
		true,
	);
});

test("a path segment shorter than 20 characters is public", () => {
	assert.equal(
		isPrivateTracker(`https://tracker.example/${SHORT_PASSKEY}/announce`),
		false,
	);
});

test("a long path segment with other characters is public", () => {
	assert.equal(
		isPrivateTracker("https://tracker.example/0123456789abcdef0123.php"),
		false,
	);
});

test("a long host name is not mistaken for a passkey", () => {
	assert.equal(
		isPrivateTracker("https://0123456789abcdef0123.tracker.example/announce"),
		false,
	);
});

test("public tracker URLs are public", () => {
	for (const url of [
		"udp://tracker.opentrackr.org:1337/announce",
		"https://tracker.example/announce",
		"http://tracker.example:6969/announce.php",
		"wss://tracker.example",
		"udp://[2001:db8::1]:6969/announce",
	]) {
		assert.equal(isPrivateTracker(url), false, url);
	}
});

test("a tracker entry that is not a URL is private", () => {
	for (const entry of ["tracker.example/announce", "udp:///announce", "x"]) {
		assert.equal(isPrivateTracker(entry), true, entry);
	}
});

test("only public tracker entries are uploaded", () => {
	assert.deepEqual(
		withoutSyncSecrets("tracker", {
			"bt-tracker": `udp://a:1/announce,https://t.example/${PASSKEY}/announce`,
		}),
		{ "bt-tracker": "udp://a:1/announce" },
	);
});

test("uploaded tracker entries keep their order and the other keys", () => {
	const data = {
		"auto-sync-tracker": true,
		"tracker-source": ["https://list.example/all.txt"],
		"bt-tracker": [
			"udp://a:1/announce",
			"https://t.example/announce?passkey=abc",
			"http://b.example:2/announce",
			`https://u.example/${PASSKEY}/announce`,
			"udp://c:3/announce",
		].join(","),
	};
	const before = structuredClone(data);
	assert.deepEqual(withoutSyncSecrets("tracker", data), {
		"auto-sync-tracker": true,
		"tracker-source": ["https://list.example/all.txt"],
		"bt-tracker":
			"udp://a:1/announce,http://b.example:2/announce,udp://c:3/announce",
	});
	assert.deepEqual(data, before);
});

test("line breaks separate tracker entries", () => {
	assert.deepEqual(
		withoutSyncSecrets("tracker", {
			"bt-tracker": `udp://a:1/announce\r\n https://t.example/announce?k=1 \n\nudp://b:2/announce,`,
		}),
		{ "bt-tracker": "udp://a:1/announce,udp://b:2/announce" },
	);
});

test("a tracker list without private entries is returned as it is", () => {
	const data = { "bt-tracker": "udp://a:1/announce,\nudp://b:2/announce " };
	assert.equal(
		withoutSyncSecrets("tracker", data)["bt-tracker"],
		"udp://a:1/announce,\nudp://b:2/announce ",
	);
});

test("a tracker list made only of private entries uploads as an empty string", () => {
	assert.deepEqual(
		withoutSyncSecrets("tracker", {
			"bt-tracker": `https://t.example/${PASSKEY}/announce,http://u:p@t.example/announce`,
		}),
		{ "bt-tracker": "" },
	);
});

test("tracker data without a list is returned unchanged", () => {
	assert.deepEqual(withoutSyncSecrets("tracker", { "tracker-source": [] }), {
		"tracker-source": [],
	});
	assert.deepEqual(withoutSyncSecrets("tracker", { "bt-tracker": 1 }), {
		"bt-tracker": 1,
	});
});

test("tracker lists of other categories are not filtered", () => {
	const data = { "bt-tracker": `https://t.example/${PASSKEY}/announce` };
	assert.deepEqual(withoutSyncSecrets("download", data), data);
});

test("an incoming tracker list replaces the public entries and keeps the private ones", () => {
	assert.deepEqual(
		keepLocalSyncSecrets(
			"tracker",
			{
				"bt-tracker": `udp://old:1/announce,https://t.example/${PASSKEY}/announce`,
			},
			{ "bt-tracker": "udp://new:2/announce" },
		),
		{
			"bt-tracker": `udp://new:2/announce,https://t.example/${PASSKEY}/announce`,
		},
	);
});

test("a private tracker entry already in the incoming list is not doubled", () => {
	const priv = `https://t.example/${PASSKEY}/announce`;
	assert.deepEqual(
		keepLocalSyncSecrets(
			"tracker",
			{ "bt-tracker": `${priv},${priv},http://u:p@x.example/announce` },
			{ "bt-tracker": `udp://new:2/announce,${priv}` },
		),
		{
			"bt-tracker": `udp://new:2/announce,${priv},http://u:p@x.example/announce`,
		},
	);
});

test("private tracker entries come back intact after an empty incoming list", () => {
	const local = {
		"bt-tracker": `https://t.example/${PASSKEY}/announce,http://u:p@t.example/announce`,
	};
	const uploaded = withoutSyncSecrets("tracker", local);
	assert.deepEqual(uploaded, { "bt-tracker": "" });
	assert.deepEqual(keepLocalSyncSecrets("tracker", local, uploaded), local);
});

test("private tracker entries from an older client are not imported", () => {
	assert.deepEqual(
		keepLocalSyncSecrets(
			"tracker",
			{ "bt-tracker": "udp://a:1/announce" },
			{
				"bt-tracker": `udp://b:2/announce,https://t.example/${PASSKEY}/announce`,
			},
		),
		{ "bt-tracker": "udp://b:2/announce" },
	);
});

test("a local list without private entries leaves the incoming list as it is", () => {
	assert.deepEqual(
		keepLocalSyncSecrets(
			"tracker",
			{ "bt-tracker": "udp://a:1/announce" },
			{ "bt-tracker": "udp://b:2/announce,\nudp://c:3/announce" },
		),
		{ "bt-tracker": "udp://b:2/announce,\nudp://c:3/announce" },
	);
});

test("tracker entries are not added when the incoming data lacks the list", () => {
	assert.deepEqual(
		keepLocalSyncSecrets(
			"tracker",
			{ "bt-tracker": `https://t.example/${PASSKEY}/announce` },
			{ "auto-sync-tracker": false },
		),
		{ "auto-sync-tracker": false },
	);
});

test("a server copy with a private tracker entry needs a scrub", () => {
	const data = {
		"bt-tracker": `udp://a:1/announce,https://t.example/${PASSKEY}/announce`,
	};
	assert.equal(remoteNeedsScrub("tracker", data, { ...data }), true);
});

test("a server copy with only public tracker entries needs no scrub", () => {
	const data = {
		"bt-tracker": "udp://a:1/announce,http://b.example:2/announce",
	};
	assert.equal(remoteNeedsScrub("tracker", data, { ...data }), false);
});

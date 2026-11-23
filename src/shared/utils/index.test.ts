import assert from "node:assert/strict";
import { createRequire } from "node:module";
import path from "node:path";
import { after, before, test } from "node:test";
import { fileURLToPath } from "node:url";
import { createServer, type Plugin, type ViteDevServer } from "vite";
import registry from "../configKeys.json" with { type: "json" };

type UtilsModule = typeof import("./index.ts");

const root = path.resolve(
	path.dirname(fileURLToPath(import.meta.url)),
	"../../..",
);

const LODASH_NAMED = "virtual:risuko-lodash-named";

function lodashNamedExports(): Plugin {
	const packageJson = path.join(root, "package.json");
	const names = Object.keys(createRequire(packageJson)("lodash")).filter(
		(name) => /^[A-Za-z_$][\w$]*$/.test(name),
	);
	const source = [
		'import { createRequire } from "node:module";',
		`const lodash = createRequire(${JSON.stringify(packageJson)})("lodash");`,
		...names.map((name) => `export const ${name} = lodash.${name};`),
		"export default lodash;",
	].join("\n");
	return {
		name: "risuko-lodash-named-exports",
		enforce: "pre",
		resolveId(id) {
			return id === "lodash" ? LODASH_NAMED : undefined;
		},
		load(id) {
			return id === LODASH_NAMED ? source : undefined;
		},
	};
}

let utils: UtilsModule;
let server: ViteDevServer;

before(async () => {
	server = await createServer({
		root,
		appType: "custom",
		logLevel: "silent",
		plugins: [lodashNamedExports()],
		ssr: { noExternal: ["lodash"] },
		resolve: {
			alias: [
				{ find: /^@shared\//, replacement: `${root}/src/shared/` },
				{ find: /^@\//, replacement: `${root}/src/renderer/` },
			],
		},
	});
	utils = (await server.ssrLoadModule(
		path.join(root, "src/shared/utils/index.ts"),
	)) as UtilsModule;
});

after(async () => {
	await server.close();
});

test("the module loads through vite", () => {
	assert.equal(typeof utils.splitTaskLinks, "function");
});

test("pasted links are split per line, trimmed, and blank lines are dropped", () => {
	assert.deepEqual(
		utils.splitTaskLinks(
			"  http://a.com/1  \n\n   \r\nhttps://b.com/2\r\nftp://c.com/3\nnot a link\n",
		),
		["http://a.com/1", "https://b.com/2", "ftp://c.com/3", "not a link"],
	);
});

test("magnet, ed2k and thunder links come through unchanged", () => {
	const magnet = "magnet:?xt=urn:btih:abc&dn=x";
	const ed2k = "ed2k://|file|a.zip|123|0123456789ABCDEF|/";
	const thunder = "thunder://QUFodHRwOi8v";
	assert.deepEqual(utils.splitTaskLinks(`${magnet}\n${ed2k}\n${thunder}`), [
		magnet,
		ed2k,
		thunder,
	]);
});

test("links separated by a space on one line stay a single entry", () => {
	assert.deepEqual(utils.splitTaskLinks("http://a.com/1 http://b.com/2"), [
		"http://a.com/1 http://b.com/2",
	]);
});

test("a backslash line continuation joins the rows with a space", () => {
	assert.deepEqual(utils.splitTaskLinks("http://a.com/1\\\nhttp://b.com/2"), [
		"http://a.com/1 http://b.com/2",
	]);
});

test("empty or missing link text gives no links", () => {
	assert.deepEqual(utils.splitTaskLinks(""), []);
	assert.deepEqual(utils.splitTaskLinks(undefined), []);
	assert.deepEqual(utils.splitTaskLinksWithRenames("  \n "), []);
});

test("a Rename suffix is split off the link and the keyword is case-insensitive", () => {
	assert.deepEqual(
		utils.splitTaskLinksWithRenames(
			[
				"http://a.com/f.zip Rename: my file.zip",
				"magnet:?xt=urn:btih:abc rename:m.iso",
				"http://d.com   RENAME:  x.bin  ",
				"http://e.com/plain",
			].join("\n"),
		),
		[
			{ uri: "http://a.com/f.zip", rename: "my file.zip" },
			{ uri: "magnet:?xt=urn:btih:abc", rename: "m.iso" },
			{ uri: "http://d.com", rename: "x.bin" },
			{ uri: "http://e.com/plain" },
		],
	);
});

test("a rename containing a path separator is dropped and the link is kept", () => {
	assert.deepEqual(
		utils.splitTaskLinksWithRenames(
			"http://a.com/g Rename: ../evil.bin\nhttp://b.com/h Rename: sub\\evil.bin",
		),
		[{ uri: "http://a.com/g" }, { uri: "http://b.com/h" }],
	);
});

test("a Rename suffix with no name stays part of the link text", () => {
	assert.deepEqual(
		utils.splitTaskLinksWithRenames("http://c.com/h Rename:   "),
		[{ uri: "http://c.com/h Rename:" }],
	);
});

test("every supported scheme is named by detectUriProtocol", () => {
	const cases: [string, string][] = [
		["magnet:?xt=urn:btih:abc", "Magnet"],
		["ed2k://|file|a|1|h|/", "ED2K"],
		["adc://host", "ADC / Direct Connect"],
		["adcs://host", "ADC / Direct Connect"],
		["dchub://host", "ADC / Direct Connect"],
		["nmdc://host", "ADC / Direct Connect"],
		["gnutella://host", "Gnutella"],
		["gnet://host", "Gnutella"],
		["g2://host", "Gnutella2 (G2)"],
		["gift://host", "giFT"],
		["thunder://abc", "Thunder"],
		["flashget://abc", "Thunder"],
		["qqdl://abc", "Thunder"],
		["sftp://host/f", "SFTP"],
		["ftps://host/f", "FTPS"],
		["ftp://host/f", "FTP"],
		["https://youtu.be/x", "YouTube"],
		["https://www.youtube.com/watch?v=1", "YouTube"],
		["https://vimeo.com/1", "Media (yt-dlp)"],
		["https://a.com/list.m3u8?x=1", "M3U8 / HLS"],
		["http://a.com/list.m3u", "M3U8 / HLS"],
		["https://a.com/f.zip", "HTTPS"],
		["http://a.com/f.zip", "HTTP"],
	];
	for (const [uri, expected] of cases) {
		assert.equal(utils.detectUriProtocol(uri), expected, uri);
	}
});

test("scheme detection ignores case and surrounding whitespace", () => {
	const cases: [string, string][] = [
		["MAGNET:?xt=x", "Magnet"],
		["ED2K://x", "ED2K"],
		["SFTP://host", "SFTP"],
		["FTP://host", "FTP"],
		["HTTP://A.COM/F", "HTTP"],
		["  https://a.com/f  ", "HTTPS"],
	];
	for (const [uri, expected] of cases) {
		assert.equal(utils.detectUriProtocol(uri), expected, uri);
	}
});

test("text that is not a supported link has no protocol", () => {
	for (const uri of ["", "   ", "a.com/f", "javascript:alert(1)", null]) {
		assert.equal(utils.detectUriProtocol(uri), null, String(uri));
	}
});

test("media hosts are matched by hostname, not by substring", () => {
	assert.equal(utils.detectUriProtocol("https://notyoutube.com/x"), "HTTPS");
	assert.equal(utils.detectUriProtocol("https://evilvimeo.com/x"), "HTTPS");
	assert.equal(
		utils.detectUriProtocol("https://a.com/?u=youtube.com"),
		"HTTPS",
	);
});

test("isMediaUri accepts http and https links on a media host or its subdomains", () => {
	assert.equal(utils.isMediaUri("https://vimeo.com/1"), true);
	assert.equal(utils.isMediaUri("http://www.vimeo.com/1"), true);
	assert.equal(utils.isMediaUri("https://x.com/a"), true);
	assert.equal(utils.isMediaUri("https://fb.watch/x"), true);
});

test("isMediaUri rejects other schemes, lookalike hosts and empty input", () => {
	assert.equal(utils.isMediaUri("ftp://vimeo.com/1"), false);
	assert.equal(utils.isMediaUri("vimeo.com/1"), false);
	assert.equal(utils.isMediaUri("https://evilvimeo.com/1"), false);
	assert.equal(utils.isMediaUri("https://example.com/"), false);
	assert.equal(utils.isMediaUri(""), false);
	assert.equal(utils.isMediaUri(null), false);
});

test("isMediaUri ignores the case of the scheme and surrounding spaces", () => {
	assert.equal(utils.isMediaUri("HTTPS://VIMEO.COM/1"), true);
	assert.equal(utils.isMediaUri("  https://vimeo.com/1  "), true);
	assert.equal(utils.isMediaUri("  FTP://vimeo.com/1  "), false);
	assert.equal(
		utils.detectUriProtocol("HTTPS://VIMEO.COM/1"),
		"Media (yt-dlp)",
	);
});

test("option names are kebab-cased and values become strings", () => {
	assert.deepEqual(
		utils.formatOptionsForEngine({
			maxDownloadLimit: "1M",
			userAgent: "UA/1",
			ed2KPort: 4662,
			http2Enabled: 1,
			maxConnectionPerServer: 16,
			enabled: true,
			disabled: false,
			empty: "",
			"already-kebab": "x",
			HTTPServer: "y",
		}),
		{
			"max-download-limit": "1M",
			"user-agent": "UA/1",
			"ed2k-port": "4662",
			"http2-enabled": "1",
			"max-connection-per-server": "16",
			enabled: "true",
			disabled: "false",
			empty: "",
			"already-kebab": "x",
			"http-server": "y",
		},
	);
});

test("array option values are joined with newlines", () => {
	assert.deepEqual(
		utils.formatOptionsForEngine({
			btTracker: ["udp://a", "udp://b"],
			none: [],
		}),
		{ "bt-tracker": "udp://a\nudp://b", none: "" },
	);
});

test("undefined option values are left out and null is sent as text unless it is preserved", () => {
	assert.deepEqual(
		utils.formatOptionsForEngine({ missing: undefined, nothing: null }),
		{ nothing: "null" },
	);
	assert.deepEqual(
		utils.formatOptionsForEngine(
			{ missing: undefined, nothing: null, a: 1 },
			true,
		),
		{ nothing: null, a: "1" },
	);
	assert.deepEqual(utils.formatOptionsForEngine(), {});
});

test("key conversion helpers rename keys and keep values", () => {
	assert.deepEqual(
		utils.changeKeysToKebabCase({ maxDownloadLimit: 1, a_b: 2, "x y": 3 }),
		{ "max-download-limit": 1, "a-b": 2, "x-y": 3 },
	);
	assert.deepEqual(
		utils.changeKeysToCamelCase({ "max-download-limit": 1, "user-agent": 2 }),
		{ maxDownloadLimit: 1, userAgent: 2 },
	);
	assert.deepEqual(utils.changeKeysToCamelCase(), {});
});

const registeredNames = [
	...registry.user,
	...registry.system,
	...registry.startupOnly,
];

const roundTrip = (name: string) =>
	Object.keys(
		utils.changeKeysToKebabCase(utils.changeKeysToCamelCase({ [name]: 1 })),
	)[0];

test("every registered setting name survives camel-case and back", () => {
	const broken = registeredNames
		.map((name) => [name, roundTrip(name)])
		.filter(([name, back]) => name !== back);
	assert.deepEqual(broken, []);
});

test("registered names with a letter after a digit keep their lodash camel form and convert back to themselves", () => {
	for (const [name, camel] of [
		["m3u8-output-format", "m3U8OutputFormat"],
		["p2p-proxy", "p2PProxy"],
		["p2p-udp-proxy", "p2PUdpProxy"],
		["p2p-no-proxy", "p2PNoProxy"],
		["p2p-udp-no-proxy", "p2PUdpNoProxy"],
	]) {
		assert.deepEqual(utils.changeKeysToCamelCase({ [name]: 1 }), {
			[camel]: 1,
		});
		assert.deepEqual(utils.changeKeysToKebabCase({ [camel]: 1 }), {
			[name]: 1,
		});
	}
});

test("the ed2k settings round-trip through camel case", () => {
	for (const name of registeredNames.filter((n) => n.startsWith("ed2k-"))) {
		assert.equal(roundTrip(name), name);
	}
});

test("separateConfig sorts keys into user, system and others", () => {
	assert.deepEqual(
		utils.separateConfig({
			locale: "en-US",
			dir: "/downloads",
			"rpc-secret": "x",
			"bogus-key": 1,
			maxDownloadLimit: "1M",
		}),
		{
			user: { locale: "en-US" },
			system: { dir: "/downloads", "rpc-secret": "x" },
			others: { "bogus-key": 1, maxDownloadLimit: "1M" },
		},
	);
	assert.deepEqual(utils.separateConfig({}), {
		user: {},
		system: {},
		others: {},
	});
});

test("separateConfig puts every registered key in the bucket the registry names", () => {
	const pairs = (names: string[]) =>
		Object.fromEntries(names.map((name) => [name, name]));
	const result = utils.separateConfig({
		...pairs(registry.user),
		...pairs(registry.system),
	});
	assert.deepEqual(Object.keys(result.user), registry.user);
	assert.deepEqual(Object.keys(result.system), registry.system);
	assert.deepEqual(result.others, {});
});

test("diffConfig keeps only the entries whose value changed", () => {
	assert.deepEqual(utils.diffConfig({ a: 1, b: "x" }, { a: 1, b: "x" }), {});
	assert.deepEqual(utils.diffConfig({ a: 1, b: "x" }, { a: 2, b: "x" }), {
		a: 2,
	});
	assert.deepEqual(utils.diffConfig({ a: 1 }, { a: "1" }), { a: "1" });
});

test("diffConfig compares arrays and objects by content", () => {
	assert.deepEqual(utils.diffConfig({ l: [1, 2] }, { l: [1, 2] }), {});
	assert.deepEqual(utils.diffConfig({ l: [1, 2] }, { l: [1, 3] }), {
		l: [1, 3],
	});
	assert.deepEqual(utils.diffConfig({ o: { a: 1 } }, { o: { a: 1 } }), {});
	assert.deepEqual(utils.diffConfig({ o: { a: 1 } }, { o: { a: 2 } }), {
		o: { a: 2 },
	});
});

test("diffConfig reports a key only the new side has and ignores one only the old side has", () => {
	assert.deepEqual(utils.diffConfig({ a: 1 }, { a: 1, b: 2 }), { b: 2 });
	assert.deepEqual(utils.diffConfig({ a: 1, b: 2 }, { a: 1 }), {});
	assert.deepEqual(utils.diffConfig({ a: 1 }, { b: undefined }), {});
	assert.deepEqual(utils.diffConfig(), {});
});

test("changedConfig starts with empty basic and advanced buckets", () => {
	assert.deepEqual(utils.changedConfig, { basic: {}, advanced: {} });
});

test("a header line becomes a camel-cased name with its value", () => {
	assert.deepEqual(utils.parseHeader("User-Agent: foo"), { userAgent: "foo" });
});

test("several header lines are parsed, including CRLF line ends", () => {
	assert.deepEqual(
		utils.parseHeader(
			"User-Agent: foo\nReferer: http://a.com/x\r\nX-Custom-Header: 1",
		),
		{ userAgent: "foo", referer: "http://a.com/x", xCustomHeader: "1" },
	);
});

test("only the first colon splits a header and spaces around it are trimmed", () => {
	assert.deepEqual(utils.parseHeader("Authorization:   Bearer a:b:c  "), {
		authorization: "Bearer a:b:c",
	});
	assert.deepEqual(utils.parseHeader("  Accept : text/html"), {
		accept: "text/html",
	});
});

test("empty header text gives no headers", () => {
	assert.deepEqual(utils.parseHeader(""), {});
	assert.deepEqual(utils.parseHeader("   \n  "), {});
	assert.deepEqual(utils.parseHeader(), {});
});

test("a header line without a colon is skipped and a repeated header keeps the last value", () => {
	assert.deepEqual(
		utils.parseHeader("no colon here\nCookie: a=1\nCookie: b=2"),
		{
			cookie: "b=2",
		},
	);
});

test("a header with an empty name is kept under an empty key", () => {
	assert.deepEqual(utils.parseHeader(": value\nEmpty:"), {
		"": "value",
		empty: "",
	});
});

test("a task shows its file name without the folder", () => {
	const name = (path: string) => utils.getTaskName({ files: [{ path }] });
	assert.equal(name("/downloads/movie.mkv"), "movie.mkv");
	assert.equal(name("C:\\Users\\me\\a.zip"), "a.zip");
	assert.equal(name("\\\\server\\share\\b.zip"), "b.zip");
});

test("a task with only a URI shows the decoded last path segment", () => {
	const files = (uri: string) => [{ path: "", uris: [{ uri }] }];
	assert.equal(
		utils.getTaskName({ files: files("http://a.com/dir/f%20x.zip") }),
		"f x.zip",
	);
	assert.equal(
		utils.getTaskName({ files: files("http://a.com/dir/") }),
		"http://a.com/dir/",
	);
});

test("a torrent shows its own name instead of the name of its first file", () => {
	assert.equal(
		utils.getTaskName({
			bittorrent: { info: { name: "Season 1" } },
			files: [{ path: "/d/Season 1/e1.mkv" }, { path: "/d/Season 1/e2.mkv" }],
		}),
		"Season 1",
	);
});

test("a magnet without metadata shows its placeholder file or the default name", () => {
	assert.equal(
		utils.getTaskName({
			bittorrent: {},
			files: [{ path: "/d/[METADATA]abc" }],
		}),
		"[METADATA]abc",
	);
	assert.equal(utils.getTaskName({ bittorrent: {}, files: [] }), "");
	assert.equal(
		utils.getTaskName({ bittorrent: {}, files: [] }, { defaultName: "Magnet" }),
		"Magnet",
	);
});

test("a missing task or a task without files shows the default name", () => {
	assert.equal(utils.getTaskName(null), "");
	assert.equal(utils.getTaskName(null, { defaultName: "Unknown" }), "Unknown");
	assert.equal(
		utils.getTaskName({ files: [] }, { defaultName: "Unknown" }),
		"Unknown",
	);
});

test("long task names are cut at maxLen and get an ellipsis", () => {
	const file = (name: string) => ({ files: [{ path: `/d/${name}` }] });
	assert.equal(utils.getTaskName(file("x".repeat(70))), `${"x".repeat(64)}...`);
	assert.equal(
		utils.getTaskName(file("abcdefghij.bin"), { maxLen: 5 }),
		"abcde...",
	);
	assert.equal(
		utils.getTaskName(file("abcdefghij.bin"), { maxLen: 0 }),
		"abcdefghij.bin",
	);
	assert.equal(utils.getTaskName(file("x".repeat(63))), "x".repeat(63));
});

test("a name exactly maxLen long is returned whole and one more character is cut", () => {
	assert.equal(
		utils.getTaskName({ files: [{ path: `/d/${"x".repeat(64)}` }] }),
		"x".repeat(64),
	);
	assert.equal(
		utils.getTaskName({ files: [{ path: `/d/${"x".repeat(65)}` }] }),
		`${"x".repeat(64)}...`,
	);
});

test("the temporary suffix is stripped from a task name only once the task is complete", () => {
	const task = (status: string) => ({
		status,
		files: [{ path: "/d/a.zip.part" }],
	});
	assert.equal(utils.getTaskName(task("active")), "a.zip.part");
	assert.equal(utils.getTaskName(task("complete")), "a.zip");
});

test("stripTempDownloadSuffix removes a trailing .part in any case", () => {
	assert.equal(utils.stripTempDownloadSuffix("a.zip.part"), "a.zip");
	assert.equal(utils.stripTempDownloadSuffix("A.ZIP.PART"), "A.ZIP");
	assert.equal(utils.stripTempDownloadSuffix("a.zip"), "a.zip");
	assert.equal(utils.stripTempDownloadSuffix("a.part.zip"), "a.part.zip");
	assert.equal(utils.stripTempDownloadSuffix(".part"), "");
	assert.equal(utils.stripTempDownloadSuffix(), "");
});

test("only an active task flagged as seeder counts as seeding", () => {
	assert.equal(
		utils.checkTaskIsSeeder({ seeder: "true", status: "active" }),
		true,
	);
	assert.equal(
		utils.checkTaskIsSeeder({ seeder: "true", status: "paused" }),
		false,
	);
	assert.equal(
		utils.checkTaskIsSeeder({ seeder: "false", status: "active" }),
		false,
	);
	assert.equal(utils.checkTaskIsSeeder({ status: "active" }), false);
	assert.equal(
		utils.checkTaskIsSeeder({ seeder: true, status: "active" }),
		false,
	);
});

test("a seeding task shows the seeding status and no download speed", () => {
	const seeding = { seeder: "true", status: "active", downloadSpeed: "100" };
	const paused = { seeder: "true", status: "paused", downloadSpeed: "100" };
	assert.equal(utils.getDisplayTaskStatus(seeding), "seeding");
	assert.equal(utils.getDisplayTaskStatus(paused), "paused");
	assert.equal(utils.getDisplayDownloadSpeed(seeding), 0);
	assert.equal(utils.getDisplayDownloadSpeed(paused), 100);
	assert.equal(utils.getDisplayDownloadSpeed({ status: "active" }), 0);
});

test("a missing task is not a seeder and shows no download speed", () => {
	assert.equal(utils.checkTaskIsSeeder(null), false);
	assert.equal(utils.checkTaskIsSeeder(undefined), false);
	assert.equal(utils.getDisplayDownloadSpeed(null), 0);
	assert.equal(utils.getDisplayDownloadSpeed(undefined), 0);
});

test("checkTaskIsBT is true only when the task has a bittorrent entry", () => {
	assert.equal(utils.checkTaskIsBT({ bittorrent: {} }), true);
	assert.equal(utils.checkTaskIsBT({}), false);
	assert.equal(utils.checkTaskIsBT(null), false);
	assert.equal(utils.checkTaskIsBT(undefined), false);
});

test("progress, ratio and time remaining handle zero and bad input", () => {
	assert.equal(utils.calcProgress("1000", "250"), 25);
	assert.equal(utils.calcProgress("3", "1"), 33.33);
	assert.equal(utils.calcProgress("3", "1", 0), 33);
	assert.equal(utils.calcProgress("0", "0"), 0);
	assert.equal(utils.calcProgress("abc", "1"), 0);
	assert.equal(utils.calcRatio("1000", "2500"), 2.5);
	assert.equal(utils.calcRatio("3", "1"), 0.3333);
	assert.equal(utils.calcRatio("0", "5"), 0);
	assert.equal(utils.timeRemaining(1000, 400, 100), 6);
	assert.equal(utils.timeRemaining(1001, 0, 100), 11);
	assert.equal(utils.timeRemaining(1000, 400, 0), 0);
	assert.equal(utils.timeRemaining(1000, 1000, 100), 0);
});

test("comma and line lists convert both ways", () => {
	assert.equal(utils.convertCommaToLine("a, b ,c"), "a\nb\nc");
	assert.equal(utils.convertCommaToLine("a,,b"), "a\n\nb");
	assert.equal(utils.convertCommaToLine(""), "");
	assert.equal(utils.convertLineToComma("a\nb\r\nc\rd"), "a,b,c,d");
	assert.equal(utils.convertLineToComma("  a\nb  \n"), "a,b");
});

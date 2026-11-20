import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { resolve } from "node:path";
import { test } from "node:test";
import {
	checkConfigKeys,
	rustDefaultKeys,
	syncCategoryKeys,
} from "./check-config-keys.mjs";

const rustSource = `use serde_json::{json, Map, Value};

pub fn system_defaults() -> Map<String, Value> {
    let mut m = Map::new();
    m.insert("all-proxy".into(), json!(""));
    m.insert(
        "user-agent".into(),
        json!("Mozilla/5.0"),
    );
    m
}

fn default_download_dir() -> std::path::PathBuf {
    let hint = "helper-only";
    std::path::PathBuf::from(hint)
}

pub fn user_defaults() -> Map<String, Value> {
    let mut m = Map::new();
    m.insert("theme".into(), json!("auto"));
    #[cfg(not(target_os = "android"))]
    {
        m.insert("clipboard-watch".into(), json!(true));
    }
    m.insert(
        "proxy".into(),
        json!({ "http": { "enable": false } }),
    );
    m
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn required() {
        let mut m = Map::new();
        m.insert("test-only".into(), json!(1));
        assert!(user_defaults().contains_key("theme"));
    }
}
`;

test("defaults are collected per function, including inserts split over lines", () => {
	assert.deepEqual(rustDefaultKeys(rustSource), {
		system: ["all-proxy", "user-agent"],
		user: ["theme", "clipboard-watch", "proxy"],
	});
});

test("quoted names in helpers and the test module are ignored", () => {
	const { system, user } = rustDefaultKeys(rustSource);
	for (const ignored of ["helper-only", "test-only", "http", "enable"]) {
		assert.ok(!system.includes(ignored));
		assert.ok(!user.includes(ignored));
	}
});

test("a missing defaults function is an error", () => {
	assert.throws(() => rustDefaultKeys("pub fn system_defaults() {\n}\n"), {
		message: "pub fn user_defaults not found",
	});
});

const syncSource = `import { userKeys } from "./configKeys";

const MISC_CATEGORY = "misc";

const DEVICE_LOCAL_KEYS = new Set<string>([
	"cloud-sync-enabled",
	"legal-accepted",
	...SECRET_SYNC_KEYS,
]);

const namedCategories: SyncCategory[] = [
	{
		id: "appearance",
		keys: [
			"theme",
			"font-size",
		],
	},
	{
		id: "language",
		keys: ["locale"],
	},
	{
		id: "ftp",
		keys: ["ftp-user", "sftp-user"],
	},
	{
		id: "stats",
		keys: [],
	},
	{ id: "inline-id", keys: ["m3u8-output-format"] },
];

const categorizedKeys = new Set(namedCategories.flatMap((c) => c.keys));
const notACategory = ["after-the-section"];
`;

test("sync names come from device-local keys and every category array", () => {
	assert.deepEqual(syncCategoryKeys(syncSource), [
		"cloud-sync-enabled",
		"legal-accepted",
		"theme",
		"font-size",
		"locale",
		"ftp-user",
		"sftp-user",
		"m3u8-output-format",
	]);
});

test("category ids and text outside the section are not sync names", () => {
	const names = syncCategoryKeys(syncSource);
	for (const ignored of [
		"appearance",
		"language",
		"ftp",
		"stats",
		"inline-id",
		"misc",
		"after-the-section",
	]) {
		assert.ok(!names.includes(ignored));
	}
});

test("a sync file without its markers is an error", () => {
	assert.throws(() => syncCategoryKeys("const other = [];\n"), {
		message: "DEVICE_LOCAL_KEYS or categorizedKeys not found",
	});
});

const agreeing = {
	userKeys: ["theme", "locale"],
	systemKeys: ["dir", "split"],
	startupOnlyKeys: ["split"],
	defaults: { user: ["theme", "locale"], system: ["dir", "split"] },
	syncKeys: ["theme", "dir"],
	exceptions: {},
};

test("lists that agree produce no failures", () => {
	assert.deepEqual(checkConfigKeys(agreeing), []);
});

test("rule 1: a name in both key lists fails", () => {
	assert.deepEqual(
		checkConfigKeys({
			...agreeing,
			userKeys: ["theme", "locale", "shared"],
			systemKeys: ["dir", "split", "shared"],
		}),
		["shared: listed as both a user and a system key"],
	);
});

test("rule 2: a startup-only name outside the system keys fails", () => {
	assert.deepEqual(
		checkConfigKeys({ ...agreeing, startupOnlyKeys: ["split", "theme"] }),
		["theme: startup-only but not a system key"],
	);
});

test("rule 3: a default missing from its key list fails", () => {
	assert.deepEqual(
		checkConfigKeys({
			...agreeing,
			defaults: {
				user: ["theme", "locale", "tray-theme"],
				system: ["dir", "split", "rpc-secret-initialized"],
			},
		}),
		[
			"tray-theme: has a user default but is not in userKeys, so the window cannot save it",
			"rpc-secret-initialized: has a system default but is not in systemKeys, so the window cannot save it",
		],
	);
});

test("rule 4: a default in the wrong settings file fails", () => {
	assert.deepEqual(
		checkConfigKeys({
			...agreeing,
			defaults: { user: ["theme", "locale", "dir"], system: ["dir", "split"] },
		}).filter((failure) => failure.endsWith("wrong settings file")),
		["dir: default is in the wrong settings file"],
	);
	assert.deepEqual(
		checkConfigKeys({
			...agreeing,
			defaults: {
				user: ["theme", "locale"],
				system: ["dir", "split", "theme"],
			},
		}).filter((failure) => failure.endsWith("wrong settings file")),
		["theme: default is in the wrong settings file"],
	);
});

test("rule 5: a sync name that is not a known key fails", () => {
	assert.deepEqual(
		checkConfigKeys({ ...agreeing, syncKeys: ["theme", "dir", "themee"] }),
		["themee: named in a sync category but not a known key"],
	);
});

test("rule 6: an exception silences rule 3 for that name only", () => {
	const defaults = {
		user: ["theme", "locale", "tray-theme", "window-state"],
		system: ["dir", "split"],
	};
	assert.deepEqual(
		checkConfigKeys({
			...agreeing,
			defaults,
			exceptions: { "tray-theme": "reason" },
		}),
		[
			"window-state: has a user default but is not in userKeys, so the window cannot save it",
		],
	);
});

test("rule 6: an exception does not silence the wrong-file rule", () => {
	assert.deepEqual(
		checkConfigKeys({
			...agreeing,
			defaults: { user: ["theme", "locale", "dir"], system: ["dir", "split"] },
			exceptions: { dir: "reason" },
		}),
		["dir: default is in the wrong settings file"],
	);
});

test("rule 6: an exception that no longer applies fails", () => {
	assert.deepEqual(
		checkConfigKeys({ ...agreeing, exceptions: { theme: "reason" } }),
		["theme: listed as an exception but no longer needed"],
	);
	assert.deepEqual(
		checkConfigKeys({ ...agreeing, exceptions: { unknown: "reason" } }),
		["unknown: listed as an exception but no longer needed"],
	);
});

test("the key check runs without native TypeScript support", (t) => {
	const result = spawnSync(
		process.execPath,
		[
			"--no-experimental-strip-types",
			resolve(import.meta.dirname, "check-config-keys.mjs"),
		],
		{ encoding: "utf8" },
	);
	if (/bad option/.test(result.stderr)) {
		t.skip("this Node cannot turn type stripping off");
		return;
	}
	assert.doesNotMatch(result.stderr, /ERR_UNKNOWN_FILE_EXTENSION/);
	assert.equal(result.status, 0, result.stderr);
	assert.match(result.stdout, /^\d+ user keys, \d+ system keys/);
});

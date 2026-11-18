#!/usr/bin/env node
import { readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

const ROOT = resolve(import.meta.dirname, "..");
const CONFIG_KEYS_FILE = join(ROOT, "src", "shared", "configKeys.ts");
const SYNC_CATEGORIES_FILE = join(ROOT, "src", "shared", "syncCategories.ts");
const DEFAULTS_FILE = join(
	ROOT,
	"src-tauri",
	"risuko-engine",
	"src",
	"config",
	"defaults.rs",
);
const EXCEPTIONS_FILE = join(ROOT, "scripts", "config-key-exceptions.json");

const BODY_END = /^(fn |pub fn |#\[cfg\(test\)\])/;
const INSERT = /\bm\.insert\(\s*"([^"]+)"/g;
const QUOTED_NAME = /"([a-z0-9]+(?:-[a-z0-9]+)*)"/g;
const SYNC_ID = /\bid:\s*"[^"]*"/g;

function functionBody(source, name) {
	const lines = source.split("\n");
	const start = lines.findIndex((line) => line.startsWith(`pub fn ${name}(`));
	if (start === -1) {
		throw new Error(`pub fn ${name} not found`);
	}
	let end = lines.length;
	for (let index = start + 1; index < lines.length; index++) {
		if (BODY_END.test(lines[index])) {
			end = index;
			break;
		}
	}
	return lines.slice(start, end).join("\n");
}

function insertedNames(body) {
	return [...body.matchAll(INSERT)].map((match) => match[1]);
}

export function rustDefaultKeys(source) {
	return {
		system: insertedNames(functionBody(source, "system_defaults")),
		user: insertedNames(functionBody(source, "user_defaults")),
	};
}

export function syncCategoryKeys(source) {
	const start = source.indexOf("const DEVICE_LOCAL_KEYS");
	const end = source.indexOf("const categorizedKeys");
	if (start === -1 || end === -1 || end < start) {
		throw new Error("DEVICE_LOCAL_KEYS or categorizedKeys not found");
	}
	const names = new Set();
	for (const line of source.slice(start, end).split("\n")) {
		for (const match of line.replace(SYNC_ID, "").matchAll(QUOTED_NAME)) {
			names.add(match[1]);
		}
	}
	return [...names];
}

export function checkConfigKeys({
	userKeys,
	systemKeys,
	startupOnlyKeys,
	defaults,
	syncKeys,
	exceptions = {},
}) {
	const user = new Set(userKeys);
	const system = new Set(systemKeys);
	const excepted = new Set(Object.keys(exceptions));
	const failures = [];

	for (const name of userKeys) {
		if (system.has(name)) {
			failures.push(`${name}: listed as both a user and a system key`);
		}
	}
	for (const name of startupOnlyKeys) {
		if (!system.has(name)) {
			failures.push(`${name}: startup-only but not a system key`);
		}
	}

	const unsaved = new Set();
	for (const [kind, names, known] of [
		["user", defaults.user, user],
		["system", defaults.system, system],
	]) {
		for (const name of names) {
			if (known.has(name)) {
				continue;
			}
			unsaved.add(name);
			if (!excepted.has(name)) {
				failures.push(
					`${name}: has a ${kind} default but is not in ${kind}Keys, so the window cannot save it`,
				);
			}
		}
	}

	for (const name of defaults.user) {
		if (system.has(name)) {
			failures.push(`${name}: default is in the wrong settings file`);
		}
	}
	for (const name of defaults.system) {
		if (user.has(name)) {
			failures.push(`${name}: default is in the wrong settings file`);
		}
	}

	for (const name of syncKeys) {
		if (!user.has(name) && !system.has(name)) {
			failures.push(`${name}: named in a sync category but not a known key`);
		}
	}

	for (const name of excepted) {
		if (!unsaved.has(name)) {
			failures.push(`${name}: listed as an exception but no longer needed`);
		}
	}
	return failures;
}

async function main() {
	const { userKeys, systemKeys, startupOnlyKeys } = await import(
		pathToFileURL(CONFIG_KEYS_FILE).href
	);
	const defaults = rustDefaultKeys(readFileSync(DEFAULTS_FILE, "utf8"));
	const syncKeys = syncCategoryKeys(readFileSync(SYNC_CATEGORIES_FILE, "utf8"));
	const exceptions = JSON.parse(readFileSync(EXCEPTIONS_FILE, "utf8"));

	const failures = checkConfigKeys({
		userKeys,
		systemKeys,
		startupOnlyKeys,
		defaults,
		syncKeys,
		exceptions,
	});
	console.log(
		`${userKeys.length} user keys, ${systemKeys.length} system keys, ${defaults.user.length + defaults.system.length} defaults, ${syncKeys.length} sync names, ${Object.keys(exceptions).length} exceptions`,
	);
	for (const failure of failures) {
		console.error(failure);
	}
	if (failures.length > 0) {
		process.exitCode = 1;
	}
}

if (import.meta.url === pathToFileURL(resolve(process.argv[1] ?? "")).href) {
	await main();
}

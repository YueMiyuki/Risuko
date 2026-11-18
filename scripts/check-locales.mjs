#!/usr/bin/env node
import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { basename, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

const ROOT = resolve(import.meta.dirname, "..");
export const LOCALES_DIR = join(ROOT, "src", "shared", "locales");
const BASELINE_FILE = join(ROOT, "scripts", "locale-baseline.json");
export const REFERENCE = "en-US";

export function isPlainObject(value) {
	return value !== null && typeof value === "object" && !Array.isArray(value);
}

export function flattenEntries(object, prefix = "") {
	const entries = [];
	for (const [key, value] of Object.entries(object)) {
		const path = prefix ? `${prefix}.${key}` : key;
		if (isPlainObject(value)) {
			entries.push(...flattenEntries(value, path));
		} else {
			entries.push([path, value]);
		}
	}
	return entries;
}

export function flattenKeys(object, prefix = "") {
	return flattenEntries(object, prefix).map(([path]) => path);
}

export function placeholders(text) {
	if (typeof text !== "string") {
		return [];
	}
	const names = new Set();
	for (const match of text.matchAll(/\{\{\s*([^{}\s][^{}]*?)\s*\}\}/g)) {
		names.add(match[1]);
	}
	return [...names].sort();
}

function difference(from, against) {
	const result = [];
	for (const [area, keys] of from) {
		const other = against.get(area);
		for (const key of keys) {
			if (!other?.has(key)) {
				result.push(`${area}:${key}`);
			}
		}
	}
	return result.sort();
}

export function compareLocale(reference, locale) {
	return {
		missing: difference(reference, locale),
		extra: difference(locale, reference),
	};
}

function countKeys(areas) {
	let count = 0;
	for (const keys of areas.values()) {
		count += keys.size;
	}
	return count;
}

export function listOrNone(names) {
	return names.length > 0 ? names.join(", ") : "none";
}

function placeholderMismatches(tag, referenceTexts, localeTexts) {
	const failures = [];
	for (const [area, entries] of referenceTexts) {
		const translated = localeTexts.get(area);
		for (const [key, text] of entries) {
			if (!translated?.has(key)) {
				continue;
			}
			const expected = placeholders(text);
			const actual = placeholders(translated.get(key));
			if (expected.join("\n") !== actual.join("\n")) {
				failures.push(
					`${tag}: ${area}:${key} placeholders differ (${REFERENCE}: ${listOrNone(expected)}; ${tag}: ${listOrNone(actual)})`,
				);
			}
		}
	}
	return failures;
}

export function checkLocales({ reference, locales, baseline, texts }) {
	const total = countKeys(reference);
	const rows = [];
	const failures = [];
	for (const [tag, areas] of locales) {
		const { missing, extra } = compareLocale(reference, areas);
		const coverage =
			total === 0
				? 100
				: Math.round(((total - missing.length) / total) * 1000) / 10;
		rows.push({
			tag,
			total,
			missing: missing.length,
			extra: extra.length,
			coverage,
		});
		if (extra.length > 0) {
			failures.push(
				`${tag}: ${extra.length} key(s) not in en-US: ${extra.slice(0, 5).join(", ")}`,
			);
		}
		const localeTexts = texts?.locales.get(tag);
		if (localeTexts) {
			failures.push(
				...placeholderMismatches(tag, texts.reference, localeTexts),
			);
		}
	}
	for (const [tag, allowed] of Object.entries(baseline)) {
		const row = rows.find((candidate) => candidate.tag === tag);
		if (!row) {
			failures.push(`${tag}: locale not found`);
		} else if (row.missing > allowed) {
			failures.push(
				`${tag}: ${row.missing} missing key(s), baseline allows ${allowed}`,
			);
		}
	}
	return { rows, failures };
}

let importCount = 0;

export async function loadLocaleObjects(dir) {
	const areas = new Map();
	const files = readdirSync(dir)
		.filter((file) => file.endsWith(".ts") && file !== "index.ts")
		.sort();
	for (const file of files) {
		importCount += 1;
		const url = `${pathToFileURL(join(dir, file)).href}?load=${importCount}`;
		const module = await import(url);
		areas.set(basename(file, ".ts"), module.default);
	}
	return areas;
}

export function keysByArea(objects) {
	return new Map(
		[...objects].map(([area, object]) => [area, new Set(flattenKeys(object))]),
	);
}

export function textsByArea(objects) {
	return new Map(
		[...objects].map(([area, object]) => [
			area,
			new Map(flattenEntries(object)),
		]),
	);
}

export async function loadLocale(dir) {
	return keysByArea(await loadLocaleObjects(dir));
}

function formatTable(rows) {
	const header = ["locale", "coverage", "missing", "extra"];
	const lines = [
		header,
		...[...rows]
			.sort((a, b) => b.coverage - a.coverage || a.tag.localeCompare(b.tag))
			.map((row) => [
				row.tag,
				`${row.coverage.toFixed(1)}%`,
				String(row.missing),
				String(row.extra),
			]),
	];
	const widths = header.map((_, column) =>
		Math.max(...lines.map((line) => line[column].length)),
	);
	return lines
		.map((line) =>
			line
				.map((cell, column) => cell.padEnd(widths[column]))
				.join("  ")
				.trimEnd(),
		)
		.join("\n");
}

async function main() {
	const tags = readdirSync(LOCALES_DIR, { withFileTypes: true })
		.filter((entry) => entry.isDirectory() && entry.name !== REFERENCE)
		.map((entry) => entry.name)
		.sort();
	const referenceObjects = await loadLocaleObjects(
		join(LOCALES_DIR, REFERENCE),
	);
	const reference = keysByArea(referenceObjects);
	const locales = new Map();
	const texts = {
		reference: textsByArea(referenceObjects),
		locales: new Map(),
	};
	for (const tag of tags) {
		const objects = await loadLocaleObjects(join(LOCALES_DIR, tag));
		locales.set(tag, keysByArea(objects));
		texts.locales.set(tag, textsByArea(objects));
	}
	const baseline = JSON.parse(readFileSync(BASELINE_FILE, "utf8"));

	if (process.argv.includes("--update-baseline")) {
		const { rows } = checkLocales({ reference, locales, baseline: {} });
		const updated = {};
		for (const [tag, allowed] of Object.entries(baseline)) {
			updated[tag] = rows.find((row) => row.tag === tag)?.missing ?? allowed;
		}
		writeFileSync(BASELINE_FILE, `${JSON.stringify(updated, null, "\t")}\n`);
		return;
	}

	const { rows, failures } = checkLocales({
		reference,
		locales,
		baseline,
		texts,
	});
	console.log(formatTable(rows));
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

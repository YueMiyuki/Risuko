#!/usr/bin/env node
import { existsSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { basename, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import {
	compareLocale,
	isPlainObject,
	keysByArea,
	LOCALES_DIR,
	listOrNone,
	loadLocaleObjects,
	placeholders,
	REFERENCE,
	textsByArea,
} from "./check-locales.mjs";

const USAGE = `Usage:
  node scripts/locale-tool.mjs export <tag> [--out <file>]
      Write the en-US strings <tag> is missing as JSON ("<area>:<key>": text).
  node scripts/locale-tool.mjs apply <tag> <file.json> [--overwrite]
      Merge translations from <file.json> into src/shared/locales/<tag>.
      Existing translations are kept unless --overwrite is given.
      The files are written unformatted: run pnpm fmt afterwards.`;

const STRING_LITERAL = /"(?:[^"\\\n]|\\.)*"|'(?:[^'\\\n]|\\.)*'/g;

export function parseAreaOrder(indexSource) {
	return [
		...indexSource.matchAll(
			/^import\s+[\w$]+\s+from\s+["']\.\/([\w$]+)["'];?[ \t]*$/gm,
		),
	].map((match) => match[1]);
}

function unquote(key) {
	if (key.startsWith('"')) {
		return JSON.parse(key);
	}
	if (key.startsWith("'")) {
		return key.slice(1, -1).replace(/\\(.)/g, "$1");
	}
	return key;
}

export function blankLineKeys(source) {
	const keys = new Set();
	const path = [];
	let afterBlank = false;
	for (const line of source.split("\n")) {
		const match = /^(\t+)("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|[\w$]+)\s*:/.exec(
			line,
		);
		if (match) {
			path.length = match[1].length - 1;
			path.push(unquote(match[2]));
			if (afterBlank) {
				keys.add(path.join("."));
			}
		}
		afterBlank = line.trim() === "";
	}
	return keys;
}

function renderObject(object, breaks, prefix, depth) {
	const entries = Object.entries(object);
	if (entries.length === 0) {
		return "{}";
	}
	const indent = "\t".repeat(depth);
	const lines = entries.map(([key, value], index) => {
		const path = prefix ? `${prefix}.${key}` : key;
		const text = isPlainObject(value)
			? renderObject(value, breaks, path, depth + 1)
			: JSON.stringify(value);
		const gap = index > 0 && breaks.has(path) ? "\n" : "";
		return `${gap}${indent}${JSON.stringify(key)}: ${text}`;
	});
	return `{\n${lines.join(",\n")}\n${"\t".repeat(depth - 1)}}`;
}

export function renderArea(object, breaks = new Set()) {
	return `export default ${renderObject(object, breaks, "", 1)};\n`;
}

export function renderIndex(areas) {
	const imports = areas
		.map((area) => `import ${area} from "./${area}";\n`)
		.join("");
	const names = areas.map((area) => `\t${area},\n`).join("");
	return `${imports}\nexport default {\n${names}};\n`;
}

export function isPlainAreaSource(source) {
	const stripped = source.replace(STRING_LITERAL, '""');
	return /^\s*export default \{[\s\w$":,{}]*\};?\s*$/.test(stripped);
}

function isPlainIndexSource(source) {
	const stripped = source.replace(
		/^import\s+[\w$]+\s+from\s+["']\.\/[\w$]+["'];?[ \t]*$/gm,
		"",
	);
	return /^\s*export default \{[\s\w$,]*\};?\s*$/.test(stripped);
}

export function missingTranslations(reference, locale) {
	const missing = {};
	for (const [area, entries] of reference) {
		const present = locale.get(area);
		for (const [key, text] of entries) {
			if (!present?.has(key)) {
				missing[`${area}:${key}`] = text;
			}
		}
	}
	return missing;
}

function splitId(id) {
	const separator = id.indexOf(":");
	return separator < 0
		? [id, undefined]
		: [id.slice(0, separator), id.slice(separator + 1)];
}

export function validateTranslations(reference, translations) {
	if (!isPlainObject(translations)) {
		return ["the translations file must hold one JSON object"];
	}
	const problems = [];
	for (const [id, text] of Object.entries(translations)) {
		const [area, key] = splitId(id);
		const entries = reference.get(area);
		if (key === undefined || !entries?.has(key)) {
			problems.push(`${id}: not a key in ${REFERENCE}`);
		} else if (typeof text !== "string" || text.trim() === "") {
			problems.push(`${id}: must be a non-empty string`);
		} else {
			const expected = placeholders(entries.get(key));
			const actual = placeholders(text);
			if (expected.join("\n") !== actual.join("\n")) {
				problems.push(
					`${id}: placeholders differ (${REFERENCE}: ${listOrNone(expected)}; translation: ${listOrNone(actual)})`,
				);
			}
		}
	}
	return problems;
}

function groupByArea(translations) {
	const grouped = new Map();
	for (const [id, text] of Object.entries(translations)) {
		const [area, key] = splitId(id);
		if (!grouped.has(area)) {
			grouped.set(area, new Map());
		}
		grouped.get(area).set(key, text);
	}
	return grouped;
}

function mergeObject(reference, existing, translated, options, prefix, counts) {
	const result = {};
	for (const [key, value] of Object.entries(reference)) {
		const path = prefix ? `${prefix}.${key}` : key;
		const current =
			isPlainObject(existing) && Object.hasOwn(existing, key)
				? existing[key]
				: undefined;
		if (isPlainObject(value)) {
			const nested = mergeObject(
				value,
				current,
				translated,
				options,
				path,
				counts,
			);
			if (Object.keys(nested).length > 0) {
				result[key] = nested;
			}
			continue;
		}
		const text = translated?.get(path);
		if (text !== undefined && (current === undefined || options.overwrite)) {
			result[key] = text;
			counts.applied += 1;
		} else if (current !== undefined) {
			result[key] = current;
			if (text !== undefined) {
				counts.kept += 1;
			}
		}
	}
	return result;
}

function mergeLocale(reference, locale, translations, options = {}) {
	const counts = { applied: 0, kept: 0 };
	const grouped = groupByArea(translations);
	const areas = new Map();
	for (const [area, object] of reference) {
		const merged = mergeObject(
			object,
			locale.get(area),
			grouped.get(area),
			options,
			"",
			counts,
		);
		if (Object.keys(merged).length > 0 || locale.has(area)) {
			areas.set(area, merged);
		}
	}
	return { areas, ...counts };
}

function localeDir(root, tag) {
	const tags = readdirSync(root, { withFileTypes: true })
		.filter((entry) => entry.isDirectory())
		.map((entry) => entry.name);
	return tags.includes(tag) ? join(root, tag) : undefined;
}

async function loadReference(root) {
	const dir = join(root, REFERENCE);
	const objects = await loadLocaleObjects(dir);
	const order = parseAreaOrder(readFileSync(join(dir, "index.ts"), "utf8"));
	if (order.length === 0) {
		throw new Error(`${REFERENCE}/index.ts imports no areas`);
	}
	return new Map(
		order.map((area) => {
			if (!objects.has(area)) {
				throw new Error(`${REFERENCE}/${area}.ts is missing`);
			}
			return [area, objects.get(area)];
		}),
	);
}

export async function exportLocale(root, tag) {
	if (tag === REFERENCE) {
		throw new Error(`${REFERENCE} is the reference locale`);
	}
	const dir = localeDir(root, tag);
	if (!dir) {
		throw new Error(`${tag}: no such locale`);
	}
	const reference = await loadReference(root);
	const locale = await loadLocaleObjects(dir);
	return missingTranslations(textsByArea(reference), keysByArea(locale));
}

function nonStringLeaves(tag, locale) {
	const problems = [];
	for (const [area, entries] of textsByArea(locale)) {
		for (const [key, text] of entries) {
			if (typeof text !== "string") {
				problems.push(`${tag}: ${area}:${key} is not a string`);
			}
		}
	}
	return problems;
}

function sourceProblems(tag, dir, reference) {
	const problems = [];
	for (const file of readdirSync(dir).sort()) {
		if (!file.endsWith(".ts")) {
			continue;
		}
		const source = readFileSync(join(dir, file), "utf8");
		if (file === "index.ts") {
			if (!isPlainIndexSource(source)) {
				problems.push(
					`${tag}/index.ts: not a plain list of area imports; edit it by hand`,
				);
			}
		} else if (!reference.has(basename(file, ".ts"))) {
			problems.push(`${tag}/${file}: not an ${REFERENCE} area`);
		} else if (!isPlainAreaSource(source)) {
			problems.push(
				`${tag}/${file}: not a plain object of strings; edit it by hand`,
			);
		}
	}
	return problems;
}

export async function applyLocale(root, tag, translations, options = {}) {
	if (tag === REFERENCE) {
		return { problems: [`${REFERENCE} is the reference locale`] };
	}
	const dir = localeDir(root, tag);
	if (!dir) {
		return { problems: [`${tag}: no such locale`] };
	}
	const reference = await loadReference(root);
	const locale = await loadLocaleObjects(dir);
	const problems = [
		...sourceProblems(tag, dir, reference),
		...compareLocale(keysByArea(reference), keysByArea(locale)).extra.map(
			(id) => `${tag}: ${id} is not in ${REFERENCE}`,
		),
		...nonStringLeaves(tag, locale),
		...validateTranslations(textsByArea(reference), translations),
	];
	if (problems.length > 0) {
		return { problems };
	}
	const { areas, applied, kept } = mergeLocale(
		reference,
		locale,
		translations,
		options,
	);
	const files = [...areas].map(([area, object]) => {
		const english = readFileSync(join(root, REFERENCE, `${area}.ts`), "utf8");
		return [
			join(dir, `${area}.ts`),
			renderArea(object, blankLineKeys(english)),
		];
	});
	files.push([join(dir, "index.ts"), renderIndex([...areas.keys()])]);
	for (const [file, content] of files) {
		writeFileSync(file, content);
	}
	return { problems: [], applied, kept, areas: [...areas.keys()] };
}

function takeOption(args, name) {
	const index = args.indexOf(name);
	if (index < 0) {
		return undefined;
	}
	const [, value] = args.splice(index, 2);
	return value ?? "";
}

function takeFlag(args, name) {
	const index = args.indexOf(name);
	if (index >= 0) {
		args.splice(index, 1);
	}
	return index >= 0;
}

function fail(message) {
	console.error(message);
	process.exitCode = 1;
}

async function main(argv) {
	const args = [...argv];
	const command = args.shift();
	if (command === "export") {
		const out = takeOption(args, "--out");
		if (args.length !== 1 || out === "") {
			return fail(USAGE);
		}
		const [tag] = args;
		const missing = await exportLocale(LOCALES_DIR, tag);
		const json = `${JSON.stringify(missing, null, "\t")}\n`;
		console.error(`${tag}: ${Object.keys(missing).length} missing string(s)`);
		if (out) {
			writeFileSync(out, json);
		} else {
			process.stdout.on("error", (error) => {
				if (error.code !== "EPIPE") {
					throw error;
				}
			});
			process.stdout.write(json);
		}
		return;
	}
	if (command === "apply") {
		const overwrite = takeFlag(args, "--overwrite");
		if (args.length !== 2) {
			return fail(USAGE);
		}
		const [tag, file] = args;
		if (!existsSync(file)) {
			return fail(`${file}: no such file`);
		}
		let translations;
		try {
			translations = JSON.parse(readFileSync(file, "utf8"));
		} catch (error) {
			return fail(`${file}: ${error.message}`);
		}
		const result = await applyLocale(LOCALES_DIR, tag, translations, {
			overwrite,
		});
		if (result.problems.length > 0) {
			for (const problem of result.problems) {
				console.error(problem);
			}
			return fail(`${tag}: nothing written`);
		}
		console.error(
			`${tag}: ${result.applied} translation(s) applied, ${result.kept} existing kept${result.kept > 0 ? " (use --overwrite to replace them)" : ""}`,
		);
		console.error(
			`${tag}: wrote ${result.areas.length} area file(s) and index.ts; run pnpm fmt`,
		);
		return;
	}
	if (command === "--help" || command === "-h" || command === "help") {
		console.log(USAGE);
		return;
	}
	fail(USAGE);
}

if (import.meta.url === pathToFileURL(resolve(process.argv[1] ?? "")).href) {
	try {
		await main(process.argv.slice(2));
	} catch (error) {
		fail(error.message);
	}
}

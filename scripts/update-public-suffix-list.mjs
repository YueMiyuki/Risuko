#!/usr/bin/env node
import { writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

const SOURCE = "https://publicsuffix.org/list/public_suffix_list.dat";
const TARGET = resolve(
	import.meta.dirname,
	"../src-tauri/risuko-http/data/public_suffix_list.dat",
);

export function compactSuffixList(text) {
	const lines = text.split(/\r?\n/);
	const licenceEnd = lines.findIndex((line) => !line.startsWith("//"));
	const header = lines.slice(0, Math.max(licenceEnd, 0));
	const body = lines.filter((line) => {
		const trimmed = line.trim();
		return (
			trimmed !== "" &&
			(!trimmed.startsWith("//") || /===(BEGIN|END) /.test(trimmed))
		);
	});
	return `${[...header, ...body.filter((line) => !header.includes(line))].join("\n")}\n`;
}

async function main() {
	const response = await fetch(SOURCE);
	if (!response.ok) {
		throw new Error(`${SOURCE} answered ${response.status}`);
	}
	const compact = compactSuffixList(await response.text());
	if (!compact.includes("===BEGIN ICANN DOMAINS===")) {
		throw new Error("downloaded file is not a public suffix list");
	}
	writeFileSync(TARGET, compact);
	console.log(`wrote ${TARGET} (${compact.length} bytes)`);
}

if (import.meta.url === pathToFileURL(resolve(process.argv[1] ?? "")).href) {
	await main();
}

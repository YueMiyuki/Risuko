#!/usr/bin/env node
import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

import { normalizeVersion } from "./set-version.mjs";

export const METAINFO = "src-tauri/linux/app.risuko.Risuko.metainfo.xml";

function escapeXml(text) {
	return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

export function releaseNotes(text) {
	const lines = String(text ?? "")
		.split("\n")
		.map((line) => line.trim());
	const start = lines.findIndex((line) => /^#+\s*What's Changed/i.test(line));
	if (start === -1) {
		const summary = lines
			.filter((line) => line && !line.startsWith("#") && !line.startsWith("**Full Changelog**"))
			.join(" ");
		return summary ? [summary] : [];
	}
	const notes = [];
	for (const line of lines.slice(start + 1)) {
		if (line.startsWith("#") || line.startsWith("**Full Changelog**")) break;
		const match = line.match(/^[*-]\s+(.+?)(?:\s+by\s+@\S+\s+in\s+\S+)?$/);
		if (match) notes.push(match[1]);
	}
	return notes;
}

export function addRelease(xml, version, date, notes = []) {
	version = normalizeVersion(version);
	if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
		throw new Error(`Invalid date: ${date}`);
	}
	if (xml.includes(`<release version="${version}"`)) {
		return xml;
	}
	if (!/<releases>\n/.test(xml)) {
		throw new Error("No <releases> element found");
	}
	let entry = `    <release version="${version}" date="${date}" />\n`;
	if (notes.length > 0) {
		const body =
			notes.length === 1
				? [`        <p>${escapeXml(notes[0])}</p>`]
				: ["        <ul>", ...notes.map((note) => `          <li>${escapeXml(note)}</li>`), "        </ul>"];
		entry = [
			`    <release version="${version}" date="${date}">`,
			"      <description>",
			...body,
			"      </description>",
			"    </release>",
			"",
		].join("\n");
	}
	return xml.replace(/<releases>\n/, `<releases>\n${entry}`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
	const [version, date, notesFile] = process.argv.slice(2);
	if (!version || !date) {
		console.error("Usage: node scripts/add-metainfo-release.mjs <version> <yyyy-mm-dd> [notes-file]");
		process.exit(2);
	}
	const path = resolve(import.meta.dirname, "..", METAINFO);
	const notes = notesFile ? releaseNotes(readFileSync(notesFile, "utf8")) : [];
	const xml = readFileSync(path, "utf8");
	const next = addRelease(xml, version, date, notes);
	if (next === xml) {
		console.log(`${METAINFO} already lists ${version}`);
	} else {
		writeFileSync(path, next);
		console.log(`Added ${version} (${date}) to ${METAINFO} with ${notes.length} note(s)`);
	}
}

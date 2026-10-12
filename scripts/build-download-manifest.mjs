#!/usr/bin/env node
import { createHash } from "node:crypto";
import { readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

const SKIPPED = /\.(sha256|sig)$|^(latest|manifest)\.json$/;

export function buildDownloadManifest(dir, { tag, name, publishedAt } = {}) {
	tag = String(tag ?? "");
	if (!/^v\d+\.\d+\.\d+/.test(tag)) throw new Error(`tag must look like vX.Y.Z, got '${tag}'`);
	const assets = [];
	for (const file of readdirSync(dir).sort()) {
		if (SKIPPED.test(file)) continue;
		const path = join(dir, file);
		const stat = statSync(path);
		if (!stat.isFile()) continue;
		const sha256 = createHash("sha256").update(readFileSync(path)).digest("hex");
		assets.push({ name: file, size: stat.size, sha256 });
	}
	if (assets.length === 0) throw new Error(`no assets in ${dir}`);

	return {
		version: tag.slice(1),
		tag,
		name: name || tag,
		pub_date: publishedAt || new Date().toISOString(),
		assets,
	};
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
	const [dir, out = "manifest.json"] = process.argv.slice(2);
	if (!dir) {
		console.error("Usage: build-download-manifest.mjs <assets-dir> [out]");
		process.exit(2);
	}
	const manifest = buildDownloadManifest(dir, {
		tag: process.env.TAG,
		name: process.env.RELEASE_NAME,
		publishedAt: process.env.PUBLISHED_AT,
	});
	writeFileSync(out, `${JSON.stringify(manifest, null, 2)}\n`);
	console.log(`wrote ${out}: ${manifest.assets.length} assets for ${manifest.tag}`);
}

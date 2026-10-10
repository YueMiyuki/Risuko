import { existsSync, readdirSync, readFileSync, realpathSync, statSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const SEMVER =
	/^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/;

export function normalizeVersion(raw) {
	const version = String(raw ?? "")
		.trim()
		.replace(/^v/, "");
	if (!SEMVER.test(version)) {
		throw new Error(`Invalid version: ${raw}`);
	}
	return version;
}

function isRisukoPackage(name) {
	return name.startsWith("@risuko/") || name === "risuko-js" || name === "risuko-cli";
}

function updateJson(path, version, changes) {
	const raw = readFileSync(path, "utf8");
	const pkg = JSON.parse(raw);
	pkg.version = version;
	for (const field of ["dependencies", "optionalDependencies"]) {
		for (const dep of Object.keys(pkg[field] ?? {})) {
			if (isRisukoPackage(dep)) {
				pkg[field][dep] = version;
			}
		}
	}
	const indent = raw.match(/^[ \t]+(?=")/m)?.[0] ?? "  ";
	const next = `${JSON.stringify(pkg, null, indent)}\n`;
	if (next !== raw) {
		writeFileSync(path, next);
		changes.push(path);
	}
}

function packageJsonsUnder(dir) {
	const found = [];
	for (const entry of readdirSync(dir)) {
		const full = join(dir, entry);
		if (entry === "package.json") {
			found.push(full);
		} else if (entry !== "node_modules" && statSync(full).isDirectory()) {
			found.push(...packageJsonsUnder(full));
		}
	}
	return found;
}

function replaceOnce(path, pattern, replacement, changes) {
	const raw = readFileSync(path, "utf8");
	if (!pattern.test(raw)) {
		throw new Error(`No version field found in ${path}`);
	}
	const next = raw.replace(pattern, replacement);
	if (next !== raw) {
		writeFileSync(path, next);
		changes.push(path);
	}
}

function tableBody(text, header) {
	const escaped = header.replace(/[\\^$.*+?()[\]{}|]/g, "\\$&");
	const match = new RegExp(`^\\[${escaped}\\][ \\t]*$`, "m").exec(text);
	if (!match) {
		return null;
	}
	const start = match.index + match[0].length;
	const next = /^\[/m.exec(text.slice(start));
	return [start, next ? start + next.index : text.length];
}

function workspaceCrates(tauriDir) {
	const manifests = [join(tauriDir, "Cargo.toml")];
	const members = readFileSync(manifests[0], "utf8").match(/^members\s*=\s*\[([^\]]*)\]/m);
	for (const member of members?.[1].match(/"([^"]+)"/g) ?? []) {
		manifests.push(join(tauriDir, member.slice(1, -1), "Cargo.toml"));
	}
	const names = [];
	for (const manifest of manifests) {
		const text = readFileSync(manifest, "utf8");
		const body = tableBody(text, "package");
		const pkg = body ? text.slice(...body) : "";
		const name = pkg.match(/^name\s*=\s*"([^"]+)"/m)?.[1];
		if (name && /^version\.workspace\s*=\s*true/m.test(pkg)) {
			names.push(name);
		}
	}
	return names;
}

function setWorkspaceVersion(path, version, changes) {
	const raw = readFileSync(path, "utf8");
	const body = tableBody(raw, "workspace.package");
	const section = body ? raw.slice(...body) : "";
	if (!/^version\s*=\s*"[^"]*"/m.test(section)) {
		throw new Error(`No [workspace.package] version in ${path}`);
	}
	const next =
		raw.slice(0, body[0]) +
		section.replace(/^(version\s*=\s*")[^"]*(")/m, `$1${version}$2`) +
		raw.slice(body[1]);
	if (next !== raw) {
		writeFileSync(path, next);
		changes.push(path);
	}
}

function updateCargoLock(path, crates, version, changes) {
	if (!existsSync(path)) {
		return;
	}
	const raw = readFileSync(path, "utf8");
	const wanted = new Set(crates);
	const next = raw.replace(
		/^(\[\[package\]\]\nname = "([^"]+)"\nversion = ")[^"]*(")$/gm,
		(whole, head, name, tail) => (wanted.has(name) ? `${head}${version}${tail}` : whole),
	);
	if (next !== raw) {
		writeFileSync(path, next);
		changes.push(path);
	}
}

export function setVersion(root, rawVersion) {
	const version = normalizeVersion(rawVersion);
	const changes = [];
	const tauriDir = join(root, "src-tauri");

	updateJson(join(root, "package.json"), version, changes);
	for (const dir of ["packages/risuko-cli", "packages/risuko-js", "packages/risuko-app"]) {
		const pkgDir = join(root, dir);
		if (existsSync(pkgDir)) {
			for (const path of packageJsonsUnder(pkgDir)) {
				updateJson(path, version, changes);
			}
		}
	}

	replaceOnce(
		join(tauriDir, "tauri.conf.json"),
		/("version"\s*:\s*")[^"]*(")/,
		`$1${version}$2`,
		changes,
	);
	setWorkspaceVersion(join(tauriDir, "Cargo.toml"), version, changes);
	updateCargoLock(join(tauriDir, "Cargo.lock"), workspaceCrates(tauriDir), version, changes);
	return { version, changes };
}

function isDirectExecution(entryPath = process.argv[1]) {
	if (!entryPath) {
		return false;
	}
	try {
		return realpathSync(entryPath) === realpathSync(fileURLToPath(import.meta.url));
	} catch {
		return false;
	}
}

if (isDirectExecution()) {
	try {
		const root = resolve(import.meta.dirname, "..");
		const { version, changes } = setVersion(root, process.argv[2]);
		console.log(`Version ${version}`);
		for (const path of changes) {
			console.log(`  updated ${path.slice(root.length + 1)}`);
		}
		if (changes.length === 0) {
			console.log("  already up to date");
		}
	} catch (error) {
		console.error(error instanceof Error ? error.message : error);
		console.error("Usage: node scripts/set-version.mjs <version>");
		process.exit(1);
	}
}

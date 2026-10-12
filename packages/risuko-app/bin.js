#!/usr/bin/env node
const fs = require("node:fs");
const path = require("node:path");
const https = require("node:https");
const crypto = require("node:crypto");
const { execFileSync, spawn } = require("node:child_process");

const REPO = "YueMiyuki/risuko";
const MIRROR = "https://dl.risuko.app";
const LATEST_SOURCES = [
	`${MIRROR}/manifest.json`,
	`https://github.com/${REPO}/releases/latest/download/latest.json`,
];
const LOOKUP_TIMEOUT_MS = 5_000;
const UPDATER_PUBKEY =
	"RWTd4Yzh9GXgnM46g1U78JdfLJDLxX+n8kRPAkn/Vdhm6KLi7JHknZSn";
const LAST_UNSIGNED_RELEASE = [1, 0, 0, ""];

class DownloadHttpError extends Error {
	constructor(statusCode, url) {
		super(
			`Download failed: HTTP ${statusCode} — ${url}\n` +
				`To download manually: https://github.com/${REPO}/releases/tag/v${version}`,
		);
		this.name = "DownloadHttpError";
		this.statusCode = statusCode;
		this.url = url;
	}
}

const rawArgs = process.argv.slice(2);
let version = null;
let noCache = false;
let allowLegacyNoChecksum = false;
const appArgs = [];

for (let i = 0; i < rawArgs.length; i++) {
	const arg = rawArgs[i];
	if ((arg === "--version" || arg === "-v") && rawArgs[i + 1]) {
		version = rawArgs[++i].replace(/^v/, "");
	} else if (arg === "--no-cache") {
		noCache = true;
	} else if (arg === "--allow-legacy-no-checksum") {
		allowLegacyNoChecksum = true;
	} else if (arg === "--help" || arg === "-h") {
		console.log(`Usage: risuko-app [launcher-options] [-- app-args...]

Launcher options:
  --version <x.y.z>   Use a specific release version (default: latest)
  --no-cache          Re-download even if the binary is already cached
  --allow-legacy-no-checksum
                      Allow unsigned installs when SHA-256 sidecar is missing
                      for legacy releases (pre-0.4.0 or prereleases)
  -h, --help          Show this help message

Any arguments after -- are passed through to the Risuko app.

Cache location:
  macOS:   ~/Library/Application Support/risuko-launcher/<version>/
  Linux:   $XDG_DATA_HOME/risuko-launcher/<version>/
  Windows: %APPDATA%\\risuko-launcher\\<version>\\`);
		process.exit(0);
	} else if (arg === "--") {
		appArgs.push(...rawArgs.slice(i + 1));
		break;
	} else {
		appArgs.push(arg);
	}
}

const { platform, arch } = process;

const PLATFORM_INFO = {
	darwin: {
		ext: "app.tar.gz",
		extract: "tar",
		binary: path.join("Risuko.app", "Contents", "MacOS", "Risuko"),
	},
	linux: {
		ext: "AppImage",
		extract: "chmod",
		binary: "",
	},
	win32: {
		ext: "portable.exe",
		extract: "none",
		binary: "",
	},
};

function getPlatformEntry(forVersion) {
	const info = PLATFORM_INFO[platform];
	const asset = `Risuko_${forVersion}_${platform}_${arch}.${info.ext}`;
	const binary =
		platform === "linux" || platform === "win32" ? asset : info.binary;
	return { asset, extract: info.extract, binary };
}

function getCacheBase() {
	if (platform === "darwin") {
		return path.join(
			process.env.HOME || "~",
			"Library",
			"Application Support",
			"risuko-launcher",
		);
	}
	if (platform === "win32") {
		return path.join(
			process.env.APPDATA ||
				path.join(process.env.USERPROFILE || "~", "AppData", "Roaming"),
			"risuko-launcher",
		);
	}
	return path.join(
		process.env.XDG_DATA_HOME ||
			path.join(process.env.HOME || "~", ".local", "share"),
		"risuko-launcher",
	);
}

function parseVersion(value) {
	const m = /^(\d+)\.(\d+)\.(\d+)(-[0-9A-Za-z.-]+)?$/.exec(value);
	return m ? [Number(m[1]), Number(m[2]), Number(m[3]), m[4] ?? ""] : null;
}

function compareVersions(a, b) {
	for (let i = 0; i < 3; i++) {
		if (a[i] !== b[i]) {
			return a[i] - b[i];
		}
	}
	if (a[3] === b[3]) {
		return 0;
	}
	if (!a[3] || !b[3]) {
		return a[3] ? -1 : 1;
	}
	return a[3] < b[3] ? -1 : 1;
}

function newestCachedVersion() {
	const base = getCacheBase();
	let names;
	try {
		names = fs.readdirSync(base);
	} catch {
		return null;
	}
	const cached = names.filter(
		(name) =>
			parseVersion(name) &&
			fs.existsSync(path.join(base, name, getPlatformEntry(name).binary)),
	);
	cached.sort((a, b) => compareVersions(parseVersion(b), parseVersion(a)));
	return cached[0] ?? null;
}

async function fetchLatestVersion() {
	for (const url of LATEST_SOURCES) {
		try {
			const res = await fetch(url, {
				headers: { "User-Agent": "risuko-app-launcher" },
				signal: AbortSignal.timeout(LOOKUP_TIMEOUT_MS),
			});
			if (!res.ok) {
				continue;
			}
			const manifest = await res.json();
			const latest = String(manifest?.version ?? "").replace(/^v/, "");
			if (parseVersion(latest)) {
				return latest;
			}
		} catch {}
	}
	return null;
}

async function resolveVersion() {
	if (version) {
		return version;
	}
	const latest = await fetchLatestVersion();
	if (latest) {
		return latest;
	}
	const cached = newestCachedVersion();
	if (cached) {
		console.warn(
			`Could not check for the latest release; launching cached v${cached}`,
		);
		return cached;
	}
	throw new Error(
		"Could not look up the latest release. Check your connection or pass --version <x.y.z>.",
	);
}

const MAX_REDIRECTS = 5;
const REQUEST_TIMEOUT_MS = 30_000;

function download(url, destPath, hops = 0) {
	return new Promise((resolve, reject) => {
		const req = https.get(
			url,
			{ headers: { "User-Agent": "risuko-app-launcher" } },
			(res) => {
				if (
					res.statusCode === 301 ||
					res.statusCode === 302 ||
					res.statusCode === 303 ||
					res.statusCode === 307 ||
					res.statusCode === 308
				) {
					res.resume();
					req.destroy();
					if (hops >= MAX_REDIRECTS) {
						return reject(new Error(`Too many redirects fetching ${url}`));
					}
					if (!res.headers.location) {
						return reject(new Error(`Redirect without location from ${url}`));
					}
					const nextUrl = new URL(res.headers.location, url).toString();
					return download(nextUrl, destPath, hops + 1).then(resolve, reject);
				}
				if (res.statusCode !== 200) {
					res.resume();
					req.destroy();
					return reject(new DownloadHttpError(res.statusCode, url));
				}

				const total = Number.parseInt(res.headers["content-length"] || "0", 10);
				let received = 0;
				let lastPrint = 0;

				fs.mkdirSync(path.dirname(destPath), { recursive: true });
				const file = fs.createWriteStream(destPath);
				const fail = (err) => {
					res.destroy();
					file.destroy();
					fs.unlink(destPath, () => {});
					reject(err);
				};

				res.on("data", (chunk) => {
					received += chunk.length;
					const now = Date.now();
					if (now - lastPrint < 100 && (total === 0 || received < total)) {
						return;
					}
					lastPrint = now;
					if (total > 0) {
						const pct = ((received / total) * 100).toFixed(1);
						process.stdout.write(
							`\r  Downloading… ${pct}% (${fmtBytes(received)} / ${fmtBytes(total)})`,
						);
					} else {
						process.stdout.write(`\r  Downloading… ${fmtBytes(received)}`);
					}
				});

				res.on("error", fail);
				res.on("aborted", () => fail(new Error("Download interrupted")));
				res.on("close", () => {
					if (total > 0 && received < total) {
						fail(
							new Error(`Download truncated (${received} of ${total} bytes)`),
						);
					}
				});

				res.pipe(file);

				file.on("finish", () => {
					process.stdout.write("\n");
					file.close(resolve);
				});
				file.on("error", fail);
			},
		);

		req.setTimeout(REQUEST_TIMEOUT_MS, () =>
			req.destroy(new Error(`Timed out fetching ${url}`)),
		);
		req.on("error", reject);
	});
}

async function downloadText(url) {
	const res = await fetch(url, {
		headers: { "User-Agent": "risuko-app-launcher" },
	});
	if (!res.ok) {
		throw new DownloadHttpError(res.status, url);
	}
	return res.text();
}

async function downloadOptionalText(url) {
	try {
		return await downloadText(url);
	} catch (err) {
		if (err instanceof DownloadHttpError && err.statusCode === 404) {
			return null;
		}
		throw err;
	}
}

function hashFile(filePath) {
	const sha256 = crypto.createHash("sha256");
	const blake2b = crypto.createHash("blake2b512");
	const input = fs.createReadStream(filePath);
	return new Promise((resolve, reject) => {
		input.on("data", (chunk) => {
			sha256.update(chunk);
			blake2b.update(chunk);
		});
		input.on("error", reject);
		input.on("end", () =>
			resolve({ sha256: sha256.digest("hex"), blake2b: blake2b.digest() }),
		);
	});
}

function verifySignature(blake2b, signatureText, assetName) {
	const publicKey = Buffer.from(UPDATER_PUBKEY, "base64");
	const lines = Buffer.from(signatureText.trim(), "base64")
		.toString("utf8")
		.split(/\r?\n/);
	const signature = Buffer.from(lines[1] ?? "", "base64");
	const trustedLine = lines[2] ?? "";
	const globalSignature = Buffer.from(lines[3] ?? "", "base64");
	if (
		publicKey.length !== 42 ||
		signature.length !== 74 ||
		globalSignature.length !== 64 ||
		!trustedLine.startsWith("trusted comment: ")
	) {
		throw new Error(`Malformed signature for ${assetName}`);
	}
	if (signature.subarray(0, 2).toString() !== "ED") {
		throw new Error(`Unsupported signature algorithm for ${assetName}`);
	}
	if (!signature.subarray(2, 10).equals(publicKey.subarray(2, 10))) {
		throw new Error(`${assetName} is signed with an unknown key`);
	}
	const key = crypto.createPublicKey({
		key: {
			kty: "OKP",
			crv: "Ed25519",
			x: publicKey.subarray(10).toString("base64url"),
		},
		format: "jwk",
	});
	const trusted = trustedLine.slice("trusted comment: ".length);
	const raw = signature.subarray(10);
	if (
		!crypto.verify(null, blake2b, key, raw) ||
		!crypto.verify(
			null,
			Buffer.concat([raw, Buffer.from(trusted)]),
			key,
			globalSignature,
		)
	) {
		throw new Error(`Signature verification failed for ${assetName}`);
	}
	const meta = Object.fromEntries(
		trusted.split("\t").map((field) => {
			const i = field.indexOf(":");
			return [field.slice(0, i), field.slice(i + 1)];
		}),
	);
	if (meta.version ? meta.version !== version : meta.file !== assetName) {
		throw new Error(
			`Signature for ${assetName} belongs to a different release (${trusted})`,
		);
	}
}

function isUnsignedRelease(releaseVersion) {
	const parsed = parseVersion(releaseVersion);
	return parsed !== null && compareVersions(parsed, LAST_UNSIGNED_RELEASE) <= 0;
}

function parseExpectedSha256(text, assetName) {
	for (const line of text.split(/\r?\n/)) {
		const trimmed = line.trim();
		if (!trimmed) {
			continue;
		}
		const [hash, fileName] = trimmed.split(/\s+/, 2);
		if (
			/^[a-fA-F0-9]{64}$/.test(hash) &&
			(!fileName || fileName === assetName)
		) {
			return hash.toLowerCase();
		}
	}
	throw new Error(`No SHA-256 digest found for ${assetName}`);
}

function isLegacyChecksumRelease(releaseVersion) {
	const m = /^v?(\d+)\.(\d+)\.(\d+)(.*)/.exec(releaseVersion);
	if (!m) {
		return false;
	}
	const required = [0, 4, 0];
	for (let i = 0; i < 3; i++) {
		if (Number(m[i + 1]) !== required[i]) {
			return Number(m[i + 1]) < required[i];
		}
	}
	return m[4].startsWith("-");
}

async function verifySha256(actual, checksumUrl, assetName) {
	const checksumText = await downloadOptionalText(checksumUrl);
	if (checksumText === null) {
		if (isLegacyChecksumRelease(version) && allowLegacyNoChecksum) {
			return "unverified";
		}
		throw new Error(
			`SHA-256 sidecar not found for ${assetName}: ${checksumUrl}`,
		);
	}
	const expected = parseExpectedSha256(checksumText, assetName);
	if (actual !== expected) {
		throw new Error(
			`SHA-256 mismatch for ${assetName}: expected ${expected}, got ${actual}`,
		);
	}
	return "checksum";
}

async function verifyDownload(assetPath, assetUrl, assetName) {
	const hashes = await hashFile(assetPath);
	const signatureText = await downloadOptionalText(`${assetUrl}.sig`);
	if (signatureText !== null) {
		verifySignature(hashes.blake2b, signatureText, assetName);
		return "signature";
	}
	if (!isUnsignedRelease(version)) {
		throw new Error(`Signature not found for ${assetName}: ${assetUrl}.sig`);
	}
	return verifySha256(
		hashes.sha256,
		`${githubAssetUrl(assetName)}.sha256`,
		assetName,
	);
}

function githubAssetUrl(assetName) {
	return `https://github.com/${REPO}/releases/download/v${version}/${assetName}`;
}

function fmtBytes(n) {
	if (n >= 1024 * 1024) {
		return `${(n / 1024 / 1024).toFixed(1)} MB`;
	}
	if (n >= 1024) {
		return `${(n / 1024).toFixed(1)} KB`;
	}
	return `${n} B`;
}

function extract(entry, assetPath, cacheDir) {
	switch (entry.extract) {
		case "tar":
			{
				const tmpDir = fs.mkdtempSync(path.join(cacheDir, ".extract-"));
				try {
					execFileSync("tar", ["xzf", assetPath, "-C", tmpDir], {
						stdio: "inherit",
					});
					for (const name of fs.readdirSync(tmpDir)) {
						const dest = path.join(cacheDir, name);
						fs.rmSync(dest, { recursive: true, force: true });
						fs.renameSync(path.join(tmpDir, name), dest);
					}
				} finally {
					fs.rmSync(tmpDir, { recursive: true, force: true });
				}
			}
			fs.unlinkSync(assetPath);
			break;

		case "chmod":
			fs.chmodSync(assetPath, 0o755);
			break;

		case "none":
			break;

		default:
			throw new Error(`Unknown extract type: ${entry.extract}`);
	}
}

async function main() {
	if (!PLATFORM_INFO[platform]) {
		throw new Error(`Unsupported platform: ${platform}`);
	}
	version = await resolveVersion();
	const entry = getPlatformEntry(version);
	const cacheDir = path.join(getCacheBase(), version);
	const binaryPath = path.join(cacheDir, entry.binary);

	const isCached = fs.existsSync(binaryPath);

	if (!isCached || noCache) {
		const sources = [
			`${MIRROR}/v${version}/${entry.asset}`,
			githubAssetUrl(entry.asset),
		];
		const assetPath = path.join(cacheDir, entry.asset);
		const tmpPath = `${assetPath}.download`;

		fs.mkdirSync(cacheDir, { recursive: true });

		console.log(`Downloading Risuko v${version} for ${platform}/${arch}…`);

		for (const [i, assetUrl] of sources.entries()) {
			console.log(`  From: ${assetUrl}`);
			try {
				await download(assetUrl, tmpPath);
				const verified = await verifyDownload(tmpPath, assetUrl, entry.asset);
				if (verified === "signature") {
					console.log("  Verified signature");
				} else if (verified === "checksum") {
					console.log("  Verified SHA-256 checksum from GitHub Releases");
				} else {
					console.warn(
						`  SHA-256 sidecar not found for legacy release v${version}; continuing without checksum verification`,
					);
				}
				fs.renameSync(tmpPath, assetPath);
				break;
			} catch (err) {
				fs.rmSync(tmpPath, { force: true });
				if (i === sources.length - 1) {
					throw err;
				}
				console.warn(`  ${err.message.split("\n")[0]}`);
				console.warn("  Falling back to GitHub Releases…");
			}
		}
		console.log("  Extracting…");
		extract(entry, assetPath, cacheDir);
		console.log(`  Cached to: ${cacheDir}`);
	}

	const child = spawn(binaryPath, appArgs, {
		detached: true,
		stdio: "ignore",
	});
	child.unref();
	process.exit(0);
}

main().catch((err) => {
	console.error(`\nError: ${err.message}`);
	process.exit(1);
});

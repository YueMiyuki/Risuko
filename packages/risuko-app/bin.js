#!/usr/bin/env node
const fs = require("node:fs");
const path = require("node:path");
const https = require("node:https");
const crypto = require("node:crypto");
const { execFileSync, spawn } = require("node:child_process");

const PKG_VERSION = require("./package.json").version;
const REPO = "YueMiyuki/risuko";

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
let version = PKG_VERSION;
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
  --version <x.y.z>   Use a specific release version (default: ${PKG_VERSION})
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

function getPlatformEntry() {
	const info = PLATFORM_INFO[platform];
	if (!info) {
		throw new Error(`Unsupported platform: ${platform}`);
	}
	const asset = `Risuko_${version}_${platform}_${arch}.${info.ext}`;
	const binary =
		platform === "linux" || platform === "win32" ? asset : info.binary;
	return { asset, extract: info.extract, binary };
}

function getCacheDir() {
	let base;
	if (platform === "darwin") {
		base = path.join(
			process.env.HOME || "~",
			"Library",
			"Application Support",
			"risuko-launcher",
		);
	} else if (platform === "win32") {
		base = path.join(
			process.env.APPDATA ||
				path.join(process.env.USERPROFILE || "~", "AppData", "Roaming"),
			"risuko-launcher",
		);
	} else {
		base = path.join(
			process.env.XDG_DATA_HOME ||
				path.join(process.env.HOME || "~", ".local", "share"),
			"risuko-launcher",
		);
	}
	return path.join(base, version);
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

function sha256File(filePath) {
	const hash = crypto.createHash("sha256");
	const input = fs.createReadStream(filePath);
	return new Promise((resolve, reject) => {
		input.on("data", (chunk) => hash.update(chunk));
		input.on("error", reject);
		input.on("end", () => resolve(hash.digest("hex")));
	});
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

async function verifySha256(assetPath, checksumUrl, assetName) {
	let checksumText;
	try {
		checksumText = await downloadText(checksumUrl);
	} catch (err) {
		if (err instanceof DownloadHttpError && err.statusCode === 404) {
			if (isLegacyChecksumRelease(version) && allowLegacyNoChecksum) {
				return false;
			}
			throw new Error(
				`SHA-256 sidecar not found for ${assetName}: ${checksumUrl}`,
			);
		}
		throw err;
	}
	const expected = parseExpectedSha256(checksumText, assetName);
	const actual = await sha256File(assetPath);
	if (actual !== expected) {
		throw new Error(
			`SHA-256 mismatch for ${assetName}: expected ${expected}, got ${actual}`,
		);
	}
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
	const entry = getPlatformEntry();
	const cacheDir = getCacheDir();
	const binaryPath = path.join(cacheDir, entry.binary);

	const isCached = fs.existsSync(binaryPath);

	if (!isCached || noCache) {
		const assetUrl = `https://github.com/${REPO}/releases/download/v${version}/${entry.asset}`;
		const checksumUrl = `${assetUrl}.sha256`;
		const assetPath = path.join(cacheDir, entry.asset);
		const tmpPath = `${assetPath}.download`;

		fs.mkdirSync(cacheDir, { recursive: true });

		console.log(`Downloading Risuko v${version} for ${platform}/${arch}…`);
		console.log(`  From: ${assetUrl}`);

		try {
			await download(assetUrl, tmpPath);
			const checksumVerified = await verifySha256(
				tmpPath,
				checksumUrl,
				entry.asset,
			);
			if (checksumVerified === false) {
				console.warn(
					`  SHA-256 sidecar not found for legacy release v${version}; continuing without checksum verification`,
				);
			} else {
				console.log("  Verified SHA-256 checksum");
			}
			fs.renameSync(tmpPath, assetPath);
		} catch (err) {
			fs.rmSync(tmpPath, { force: true });
			throw err;
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

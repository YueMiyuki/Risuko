/* eslint-disable @typescript-eslint/no-require-imports */
const { existsSync } = require("node:fs");
const { join } = require("node:path");

const { platform, arch } = process;

const platformArchMap = {
	darwin: {
		arm64: "@risuko/js-darwin-arm64",
		x64: "@risuko/js-darwin-x64",
	},
	linux: {
		arm64: "@risuko/js-linux-arm64-gnu",
		x64: "@risuko/js-linux-x64-gnu",
	},
	win32: {
		arm64: "@risuko/js-win32-arm64-msvc",
		x64: "@risuko/js-win32-x64-msvc",
	},
};

function loadNativeBinding() {
	const platformPackages = platformArchMap[platform];
	if (!platformPackages) {
		throw new Error(`Unsupported platform: ${platform}`);
	}

	const packageName = platformPackages[arch];
	if (!packageName) {
		throw new Error(`Unsupported architecture: ${platform}-${arch}`);
	}

	try {
		return require(packageName);
	} catch (err) {
		if (err.code !== "MODULE_NOT_FOUND") {
			throw err;
		}
		const localPath = join(__dirname, `risuko.${platform}-${arch}.node`);
		if (existsSync(localPath)) {
			return require(localPath);
		}

		throw new Error(
			`Failed to load native binding for ${platform}-${arch}.\n` +
				`Tried: ${packageName}\n` +
				`Make sure the correct platform package is installed.\n` +
				`Run: npm install ${packageName}`,
		);
	}
}

module.exports = loadNativeBinding();

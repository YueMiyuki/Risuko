import assert from "node:assert/strict";
import { test } from "node:test";
import { toKebabCasePreserveNumbers } from "./configKeyCase.ts";

test("keeps canonical ED2K settings in their native kebab-case form", () => {
	const keys = ["ed2KEnableKad", "ed2KKadPort", "ed2KPort"].map(
		toKebabCasePreserveNumbers,
	);

	assert.deepEqual(keys, ["ed2k-enable-kad", "ed2k-kad-port", "ed2k-port"]);
});

test("registered names with a letter after a digit convert back from their lodash camel form", () => {
	const keys = [
		"m3U8OutputFormat",
		"p2PProxy",
		"p2PUdpProxy",
		"p2PNoProxy",
		"p2PUdpNoProxy",
	].map(toKebabCasePreserveNumbers);

	assert.deepEqual(keys, [
		"m3u8-output-format",
		"p2p-proxy",
		"p2p-udp-proxy",
		"p2p-no-proxy",
		"p2p-udp-no-proxy",
	]);
});

test("ordinary and unregistered names convert as before", () => {
	const keys = [
		"maxDownloadLimit",
		"m3u8OutputFormat",
		"p2pProxy",
		"someNewSetting",
		"foo2BarBaz",
		"ed2KFutureOption",
		"a_b",
		"already-kebab",
	].map(toKebabCasePreserveNumbers);

	assert.deepEqual(keys, [
		"max-download-limit",
		"m3u8-output-format",
		"p2p-proxy",
		"some-new-setting",
		"foo2-bar-baz",
		"ed2k-future-option",
		"a-b",
		"already-kebab",
	]);
});

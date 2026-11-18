import assert from "node:assert/strict";
import { test } from "node:test";
import { compactSuffixList } from "./update-public-suffix-list.mjs";

test("comments are dropped but the licence, section markers and rules stay", () => {
	const source = [
		"// This Source Code Form is subject to the terms of the Mozilla Public",
		"// License, v. 2.0.",
		"",
		"// ===BEGIN ICANN DOMAINS===",
		"",
		"// ac : http://nic.ac/rules.htm",
		"ac",
		"com.ac",
		"*.ck",
		"!www.ck",
		"// ===END ICANN DOMAINS===",
		"// ===BEGIN PRIVATE DOMAINS===",
		"github.io",
		"// ===END PRIVATE DOMAINS===",
		"",
	].join("\n");
	assert.equal(
		compactSuffixList(source),
		[
			"// This Source Code Form is subject to the terms of the Mozilla Public",
			"// License, v. 2.0.",
			"// ===BEGIN ICANN DOMAINS===",
			"ac",
			"com.ac",
			"*.ck",
			"!www.ck",
			"// ===END ICANN DOMAINS===",
			"// ===BEGIN PRIVATE DOMAINS===",
			"github.io",
			"// ===END PRIVATE DOMAINS===",
			"",
		].join("\n"),
	);
});

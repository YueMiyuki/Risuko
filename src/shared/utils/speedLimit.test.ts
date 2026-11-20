import assert from "node:assert/strict";
import { test } from "node:test";
import {
	engineSpeedLimitOptions,
	formatSpeedLimit,
	touchesEngineSpeedLimits,
} from "./speedLimit.ts";

const stored = {
	maxOverallDownloadLimit: "4M",
	maxOverallUploadLimit: "1M",
	btUploadRateLimit: "512K",
};

test("with speed limits on the engine gets the stored limits", () => {
	assert.deepEqual(engineSpeedLimitOptions(stored, false), {
		"max-overall-download-limit": "4M",
		"max-overall-upload-limit": "1M",
		"bt-upload-rate-limit": "512K",
	});
});

test("with speed limits off the engine gets 0 for every limit", () => {
	assert.deepEqual(engineSpeedLimitOptions(stored, true), {
		"max-overall-download-limit": 0,
		"max-overall-upload-limit": 0,
		"bt-upload-rate-limit": 0,
	});
});

test("a limit that was never stored reaches the engine as 0", () => {
	assert.deepEqual(
		engineSpeedLimitOptions({ maxOverallUploadLimit: "1M" }, false),
		{
			"max-overall-download-limit": 0,
			"max-overall-upload-limit": "1M",
			"bt-upload-rate-limit": 0,
		},
	);
});

test("saving any speed limit re-applies the engine mode", () => {
	assert.equal(touchesEngineSpeedLimits({ btUploadRateLimit: "2M" }), true);
	assert.equal(touchesEngineSpeedLimits({ maxOverallUploadLimit: 0 }), true);
	assert.equal(touchesEngineSpeedLimits({ maxOverallDownloadLimit: 0 }), true);
	assert.equal(touchesEngineSpeedLimits({ seedRatio: 1 }), false);
});

test("a positive amount keeps its unit and anything else stores 0", () => {
	assert.equal(formatSpeedLimit(512, "K"), "512K");
	assert.equal(formatSpeedLimit(2, "M"), "2M");
	assert.equal(formatSpeedLimit(0, "M"), 0);
	assert.equal(formatSpeedLimit(-1, "K"), 0);
	assert.equal(formatSpeedLimit(Number.NaN, "K"), 0);
});

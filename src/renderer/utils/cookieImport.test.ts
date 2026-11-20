import assert from "node:assert/strict";
import { test } from "node:test";
import { cookieImportErrorKey } from "./cookieImport.ts";

test("protected browser cookies get their own message", () => {
	assert.equal(
		cookieImportErrorKey("APP_BOUND_COOKIES_UNSUPPORTED"),
		"task.task-cookie-app-bound",
	);
	assert.equal(
		cookieImportErrorKey(new Error("Error: APP_BOUND_COOKIES_UNSUPPORTED")),
		"task.task-cookie-app-bound",
	);
	assert.equal(
		cookieImportErrorKey("database is locked"),
		"task.task-cookie-import-failed",
	);
});

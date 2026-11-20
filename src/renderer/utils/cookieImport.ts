const APP_BOUND_COOKIES_ERROR = "APP_BOUND_COOKIES_UNSUPPORTED";

export function cookieImportErrorKey(error: unknown): string {
	return String(error).includes(APP_BOUND_COOKIES_ERROR)
		? "task.task-cookie-app-bound"
		: "task.task-cookie-import-failed";
}

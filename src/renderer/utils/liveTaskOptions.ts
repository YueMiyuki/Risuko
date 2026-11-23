export const LIVE_TASK_OPTION_KEYS: readonly string[] = [
	"max-download-limit",
	"lowest-speed-limit",
	"lowest-speed-limit-timeout",
	"bt-tracker",
	"send-auth-to-all-mirrors",
];

export function liveTaskOptions(
	options: Record<string, unknown>,
): Record<string, unknown> {
	return Object.fromEntries(
		Object.entries(options).filter(([key]) =>
			LIVE_TASK_OPTION_KEYS.includes(key),
		),
	);
}

export const LIVE_USER_OPTION_KEYS: readonly string[] = ["keep-seeding"];

export function liveUserOptions(
	options: Record<string, unknown>,
): Record<string, unknown> {
	return Object.fromEntries(
		Object.entries(options).filter(([key]) =>
			LIVE_USER_OPTION_KEYS.includes(key),
		),
	);
}

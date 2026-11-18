export function pickDeclaredKeys(
	keys: readonly string[],
	data: Record<string, unknown>,
): Record<string, unknown> {
	return Object.fromEntries(
		Object.entries(data).filter(([key]) => keys.includes(key)),
	);
}

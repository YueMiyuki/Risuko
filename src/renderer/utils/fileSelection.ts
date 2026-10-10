export type SelectionState = "none" | "partial" | "all";

export function selectionState(
	total: number,
	selected: ReadonlySet<number>,
): SelectionState {
	if (total <= 0 || selected.size === 0) {
		return "none";
	}
	return selected.size === total ? "all" : "partial";
}

export function sortedIndexList(selected: ReadonlySet<number>): string {
	return Array.from(selected)
		.sort((a, b) => a - b)
		.join(",");
}

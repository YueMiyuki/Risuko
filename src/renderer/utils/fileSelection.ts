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

export function splitBySelection<T extends { selectFile?: string }>(
	items: readonly T[],
	noneValue: string,
): { selectable: T[]; empty: T[] } {
	const selectable: T[] = [];
	const empty: T[] = [];
	for (const item of items) {
		(item.selectFile === noneValue ? empty : selectable).push(item);
	}
	return { selectable, empty };
}

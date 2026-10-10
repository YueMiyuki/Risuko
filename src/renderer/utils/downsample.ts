export function downsampleSeries<T>(
	items: readonly T[],
	max: number,
	score: (item: T) => number,
): T[] {
	const limit = Math.max(2, Math.floor(max));
	if (items.length <= limit) {
		return items as T[];
	}
	const out: T[] = [];
	const size = items.length / limit;
	for (let bucket = 0; bucket < limit; bucket++) {
		const start = Math.floor(bucket * size);
		const end = Math.min(items.length, Math.floor((bucket + 1) * size));
		let best = items[start];
		let bestScore = score(best);
		for (let i = start + 1; i < end; i++) {
			const value = score(items[i]);
			if (value > bestScore) {
				best = items[i];
				bestScore = value;
			}
		}
		out.push(best);
	}
	return out;
}

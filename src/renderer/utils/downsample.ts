export interface TimeBucket<T> {
	time: number;
	items: T[];
}

export function bucketByTime<T>(
	items: readonly T[],
	max: number,
	step: number,
	time: (item: T) => number,
): TimeBucket<T>[] {
	if (items.length === 0) {
		return [];
	}
	const limit = Math.max(2, Math.floor(max));
	const start = time(items[0]);
	const span = time(items[items.length - 1]) - start;
	const width = Math.max(step, span / limit);
	const dense = items.length > limit;
	const slotOf = (at: number) =>
		span > 0
			? Math.min(limit - 1, Math.floor(((at - start) / span) * limit))
			: 0;
	const buckets: TimeBucket<T>[] = [];
	let slot = -1;
	let first = 0;
	for (const item of items) {
		const at = time(item);
		const next = dense ? slotOf(at) : slot + 1;
		if (next !== slot) {
			buckets.push({ time: at, items: [] });
			slot = next;
			first = at;
		}
		const bucket = buckets[buckets.length - 1];
		bucket.items.push(item);
		bucket.time = (first + at) / 2;
	}
	const filled: TimeBucket<T>[] = [];
	for (const bucket of buckets) {
		const previous = filled[filled.length - 1];
		if (previous && bucket.time - previous.time > width * 2) {
			filled.push({ time: previous.time + width, items: [] });
			filled.push({ time: bucket.time - width, items: [] });
		}
		filled.push(bucket);
	}
	return filled;
}

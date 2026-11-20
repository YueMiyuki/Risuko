export const TASK_PAGE_SIZE = 5000;
export const TASK_LIST_MAX_ROWS = 50000;

export async function fetchAllPages<T extends { gid: string }>(
	fetchPage: (offset: number, num: number) => Promise<T[] | null | undefined>,
	pageSize = TASK_PAGE_SIZE,
	maxRows = TASK_LIST_MAX_ROWS,
): Promise<T[]> {
	const pages: T[][] = [];
	const seen = new Set<string>();
	let collected = 0;
	for (let offset = 0; collected < maxRows; offset += pageSize) {
		const page = await fetchPage(offset, pageSize);
		if (!Array.isArray(page)) {
			break;
		}
		const fresh = page.filter((row) => {
			if (seen.has(row.gid)) {
				return false;
			}
			seen.add(row.gid);
			return true;
		});
		pages.push(fresh);
		collected += fresh.length;
		if (page.length < pageSize) {
			break;
		}
	}
	const rows = pages.reverse().flat();
	return rows.length > maxRows ? rows.slice(rows.length - maxRows) : rows;
}

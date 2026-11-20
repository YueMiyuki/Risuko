const ITEM_TITLE_MAX_CODE_POINTS = 80;
const UNSAFE_FILE_NAME_CHARS = /[\\/<>:"|?*\p{Cc}]/gu;

function cleanTitle(title: string): string {
	return title.replace(/\s+/g, "_").replace(UNSAFE_FILE_NAME_CHARS, "");
}

export function rssOutputName(
	feedTitle: string,
	itemTitle: string,
	ext: string,
): string {
	const feed = cleanTitle(feedTitle);
	const item = Array.from(cleanTitle(itemTitle))
		.slice(0, ITEM_TITLE_MAX_CODE_POINTS)
		.join("");
	return `${feed}_${item}${ext}`;
}

export const isMirrorUrl = (candidate: string): boolean => {
	let parsed: URL;
	try {
		parsed = new URL(candidate.trim());
	} catch {
		return false;
	}
	return (
		(parsed.protocol === "http:" || parsed.protocol === "https:") &&
		!!parsed.hostname
	);
};

const STREAM_OR_INDEX_SUFFIXES = [".m3u8", ".m3u", ".nzb"];

const isPlainFileUrl = (uri: string): boolean => {
	if (!isMirrorUrl(uri)) {
		return false;
	}
	const path = new URL(uri).pathname.toLowerCase();
	return !STREAM_OR_INDEX_SUFFIXES.some((suffix) => path.endsWith(suffix));
};

const lowerScheme = (uri: string): string =>
	uri.replace(/^[a-z][a-z\d+.-]*:/i, (scheme) => scheme.toLowerCase());

export interface MirrorList {
	uris: string[];
	rejected: string[];
}

export const parseMirrorList = (
	text: string,
	isMedia: (uri: string) => boolean = () => false,
): MirrorList => {
	const uris: string[] = [];
	const rejected: string[] = [];
	const seen = new Set<string>();
	for (const token of `${text || ""}`.split(/\s+/)) {
		const key = token.toLowerCase();
		if (!token || seen.has(key)) {
			continue;
		}
		seen.add(key);
		const uri = lowerScheme(token);
		if (isPlainFileUrl(uri) && !isMedia(uri)) {
			uris.push(uri);
		} else {
			rejected.push(token);
		}
	}
	return { uris, rejected };
};

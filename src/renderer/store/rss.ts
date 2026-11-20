import type { DryRunMatch, RssFeed, RssItem, RssRule } from "@shared/types/rss";
import logger from "@shared/utils/logger";
import { defineStore } from "pinia";
import api from "@/api";
import { usePreferenceStore } from "@/store/preference";
import { rssOutputName } from "@/utils/rssName";

const DEFAULT_ITEMS_PER_PAGE = 20;
const ITEMS_PER_PAGE_OPTIONS = [10, 20, 30, 50, 100];
const STORAGE_KEY = "risuko.rss-items-per-page";
const PREFS_STORAGE_KEY = "risuko.rss-reader-prefs";

export type DensityMode = "compact" | "comfortable";
export type SortMode = "newest" | "oldest" | "size-desc" | "rule-match";

interface ReaderPrefs {
	densityMode: DensityMode;
	unreadOnly: boolean;
	matchedOnly: boolean;
	sortMode: SortMode;
}

const DEFAULT_PREFS: ReaderPrefs = {
	densityMode: "comfortable",
	unreadOnly: false,
	matchedOnly: false,
	sortMode: "newest",
};

function loadItemsPerPage(): number {
	try {
		const raw = localStorage.getItem(STORAGE_KEY);
		const parsed = Number(raw);
		if (ITEMS_PER_PAGE_OPTIONS.includes(parsed)) {
			return parsed;
		}
	} catch {}
	return DEFAULT_ITEMS_PER_PAGE;
}

const VALID_DENSITY_MODES: DensityMode[] = ["compact", "comfortable"];
const VALID_SORT_MODES: SortMode[] = [
	"newest",
	"oldest",
	"size-desc",
	"rule-match",
];

function loadPrefs(): ReaderPrefs {
	try {
		const raw = localStorage.getItem(PREFS_STORAGE_KEY);
		if (raw) {
			const parsed = JSON.parse(raw) as Record<string, unknown>;
			const prefs: ReaderPrefs = { ...DEFAULT_PREFS };
			if (VALID_DENSITY_MODES.includes(parsed.densityMode as DensityMode)) {
				prefs.densityMode = parsed.densityMode as DensityMode;
			}
			if (typeof parsed.unreadOnly === "boolean") {
				prefs.unreadOnly = parsed.unreadOnly;
			}
			if (typeof parsed.matchedOnly === "boolean") {
				prefs.matchedOnly = parsed.matchedOnly;
			}
			if (VALID_SORT_MODES.includes(parsed.sortMode as SortMode)) {
				prefs.sortMode = parsed.sortMode as SortMode;
			}
			return prefs;
		}
	} catch {}
	return { ...DEFAULT_PREFS };
}

function savePrefs(prefs: ReaderPrefs) {
	try {
		localStorage.setItem(PREFS_STORAGE_KEY, JSON.stringify(prefs));
	} catch {}
}

export const useRssStore = defineStore("rss", {
	state: () => {
		const prefs = loadPrefs();
		return {
			feeds: [] as RssFeed[],
			currentFeedId: null as string | null,
			items: {} as Record<string, RssItem[]>,
			rules: [] as RssRule[],
			loading: false,
			selectedItemIds: [] as string[],
			filterText: "",
			itemsPerPage: loadItemsPerPage(),
			currentPage: 1,
			densityMode: prefs.densityMode,
			unreadOnly: prefs.unreadOnly,
			matchedOnly: prefs.matchedOnly,
			sortMode: prefs.sortMode,
			_eventUnlisteners: [] as (() => void)[],
			_listenerSetupToken: null as symbol | null,
		};
	},

	getters: {
		currentFeed(): RssFeed | undefined {
			if (!this.currentFeedId) {
				return undefined;
			}
			return this.feeds.find((f) => f.id === this.currentFeedId);
		},
		currentItems(): RssItem[] {
			let items: RssItem[];
			if (!this.currentFeedId || this.currentFeedId === "__downloaded__") {
				const all = Object.values(
					this.items as Record<string, RssItem[]>,
				).flat();
				items =
					this.currentFeedId === "__downloaded__"
						? all.filter((i) => i.is_downloaded)
						: all;
			} else {
				items = (this.items[this.currentFeedId] ?? []).slice();
			}
			if (this.unreadOnly) {
				items = items.filter((i) => !i.is_read);
			}
			if (this.matchedOnly) {
				items = items.filter((i) => i.matched_rule_id);
			}
			const q = this.filterText.trim().toLowerCase();
			if (q) {
				items = items.filter((i) => i.title.toLowerCase().includes(q));
			}
			switch (this.sortMode) {
				case "oldest":
					items.sort((a, b) => (a.pub_date ?? 0) - (b.pub_date ?? 0));
					break;
				case "size-desc":
					items.sort(
						(a, b) => (b.enclosure_length ?? 0) - (a.enclosure_length ?? 0),
					);
					break;
				case "rule-match":
					items.sort((a, b) => {
						const am = a.matched_rule_id ? 1 : 0;
						const bm = b.matched_rule_id ? 1 : 0;
						if (am !== bm) {
							return bm - am;
						}
						return (b.pub_date ?? 0) - (a.pub_date ?? 0);
					});
					break;
				default:
					items.sort((a, b) => (b.pub_date ?? 0) - (a.pub_date ?? 0));
			}
			return items;
		},
		totalPages(): number {
			return Math.max(
				1,
				Math.ceil(this.currentItems.length / this.itemsPerPage),
			);
		},
		paginatedItems(): RssItem[] {
			const start = (this.currentPage - 1) * this.itemsPerPage;
			return this.currentItems.slice(start, start + this.itemsPerPage);
		},
		itemsPerPageOptions(): number[] {
			return ITEMS_PER_PAGE_OPTIONS;
		},
	},

	actions: {
		_handleRssDownloadComplete(data: {
			feedId?: string;
			itemId?: string;
			downloadPath?: string;
		}) {
			const { feedId, itemId, downloadPath } = data;
			if (!feedId || !itemId) {
				return;
			}
			const feedItems = this.items[feedId];
			if (feedItems) {
				const item = feedItems.find((i) => i.id === itemId);
				if (item) {
					item.is_downloaded = true;
					item.download_path = downloadPath;
				}
			}
		},
		_handleRssDownloadError(data: {
			feedId?: string;
			itemId?: string;
			reason?: string;
		}) {
			const { feedId, itemId, reason } = data;
			logger.warn(
				`[Risuko] RSS download failed: feed=${feedId} item=${itemId} reason=${reason || "error"}`,
			);
		},
		async initEventListeners() {
			this.cleanupEventListeners();
			const setupToken = Symbol("rss-listener-setup");
			this._listenerSetupToken = setupToken;
			const pendingUnlisteners: (() => void)[] = [];
			try {
				const { listen } = await import("@tauri-apps/api/event");
				const unlistenComplete = await listen<{
					feedId?: string;
					itemId?: string;
					downloadPath?: string;
				}>("rss:download-complete", (event) => {
					this._handleRssDownloadComplete(event.payload ?? {});
				});
				pendingUnlisteners.push(unlistenComplete);
				const unlistenError = await listen<{
					feedId?: string;
					itemId?: string;
					reason?: string;
				}>("rss:download-error", (event) => {
					this._handleRssDownloadError(event.payload ?? {});
				});
				pendingUnlisteners.push(unlistenError);
				if (this._listenerSetupToken !== setupToken) {
					for (const unlisten of pendingUnlisteners) {
						unlisten();
					}
					return;
				}
				this._eventUnlisteners.push(...pendingUnlisteners);
			} catch (err) {
				for (const unlisten of pendingUnlisteners) {
					unlisten();
				}
				logger.warn(
					"[Risuko] RSS event listener setup failed:",
					(err as Error)?.message || err,
				);
			}
		},

		cleanupEventListeners() {
			this._listenerSetupToken = null;
			for (const unlisten of this._eventUnlisteners) {
				unlisten();
			}
			this._eventUnlisteners = [];
		},

		async fetchFeeds() {
			this.feeds = (await api.getRssFeeds()) as RssFeed[];
		},

		async addFeed(url: string) {
			this.loading = true;
			try {
				const feed = (await api.addRssFeed(url)) as RssFeed;
				this.feeds.push(feed);
				try {
					await this.fetchItems(feed.id);
				} catch (err) {
					logger.warn(
						"[Risuko] RSS initial item fetch failed:",
						(err as Error)?.message || err,
					);
				}
				return feed;
			} finally {
				this.loading = false;
			}
		},

		async removeFeed(feedId: string) {
			await api.removeRssFeed(feedId);
			this.feeds = this.feeds.filter((f) => f.id !== feedId);
			delete this.items[feedId];
			if (this.currentFeedId === feedId) {
				this.currentFeedId = null;
			}
		},

		async refreshFeed(feedId: string) {
			this.loading = true;
			try {
				await api.refreshRssFeed(feedId);
				await this.fetchItems(feedId);
			} finally {
				this.loading = false;
			}
		},

		async refreshAll() {
			this.loading = true;
			try {
				await api.refreshAllRssFeeds();
				await this.fetchFeeds();
				for (const feed of this.feeds) {
					await this.fetchItems(feed.id);
				}
				this.selectedItemIds = [];
				this.currentPage = 1;
			} finally {
				this.loading = false;
			}
		},

		selectFeed(feedId: string | null) {
			this.currentFeedId = feedId;
			this.currentPage = 1;
			this.selectedItemIds = [];
			if (feedId && feedId !== "__downloaded__") {
				this.fetchItems(feedId);
			}
		},

		async fetchItems(feedId: string) {
			const items = (await api.getRssItems(feedId)) as RssItem[];
			this.items[feedId] = items;
		},

		async downloadItem(feedId: string, itemId: string) {
			const options: Record<string, string> = {};

			const prefStore = usePreferenceStore();
			const categoryDirs = prefStore.config.fileCategoryDirs;
			if (categoryDirs?.rss) {
				options.dir = categoryDirs.rss;
			}

			const feed = this.feeds.find((f) => f.id === feedId);
			if (feed) {
				const feedItems = this.items[feedId];
				const item = feedItems?.find((i) => i.id === itemId);
				if (item) {
					const url = item.enclosure_url || item.link || "";
					let ext = "";
					try {
						const lastSegment = new URL(url).pathname.split("/").pop() ?? "";
						const dotIdx = lastSegment.lastIndexOf(".");
						if (dotIdx > 0) {
							ext = lastSegment.slice(dotIdx);
						}
					} catch {}
					options.out = rssOutputName(feed.title, item.title, ext);
				}
			}

			try {
				await api.downloadRssItemTracked(
					feedId,
					itemId,
					Object.keys(options).length > 0 ? options : undefined,
				);
			} catch (err) {
				logger.warn(
					"[Risuko] RSS download failed:",
					(err as Error)?.message || err,
				);
			}
		},

		async fetchRules() {
			this.rules = (await api.getRssRules()) as RssRule[];
		},

		async addRule(rule: RssRule) {
			const created = (await api.addRssRule(rule)) as RssRule;
			this.rules.push(created);
			return created;
		},

		async updateRule(rule: RssRule) {
			const updated = (await api.updateRssRule(rule)) as RssRule;
			const idx = this.rules.findIndex((r) => r.id === updated.id);
			if (idx >= 0) {
				this.rules[idx] = updated;
			} else {
				this.rules.push(updated);
			}
			return updated;
		},

		async dryRunRule(rule: RssRule, sampleSize?: number) {
			return (await api.dryRunRssRule(rule, sampleSize)) as DryRunMatch[];
		},

		async removeRule(ruleId: string) {
			await api.removeRssRule(ruleId);
			this.rules = this.rules.filter((r) => r.id !== ruleId);
		},

		_persistPrefs() {
			savePrefs({
				densityMode: this.densityMode,
				unreadOnly: this.unreadOnly,
				matchedOnly: this.matchedOnly,
				sortMode: this.sortMode,
			});
		},

		setDensityMode(mode: DensityMode) {
			this.densityMode = mode;
			this._persistPrefs();
		},
		setUnreadOnly(value: boolean) {
			this.unreadOnly = value;
			this.currentPage = 1;
			this._persistPrefs();
		},
		setMatchedOnly(value: boolean) {
			this.matchedOnly = value;
			this.currentPage = 1;
			this._persistPrefs();
		},
		setSortMode(mode: SortMode) {
			this.sortMode = mode;
			this.currentPage = 1;
			this._persistPrefs();
		},

		async markItemRead(feedId: string, itemId: string) {
			const feedItems = this.items[feedId];
			if (!feedItems) {
				return;
			}
			const item = feedItems.find((i) => i.id === itemId);
			if (item && !item.is_read) {
				item.is_read = true;
				if (this.unreadOnly) {
					this.ensurePageInRange();
				}
				try {
					await api.markRssItemRead(feedId, itemId);
				} catch (_e) {
					item.is_read = false;
					if (this.unreadOnly) {
						this.ensurePageInRange();
					}
				}
			}
		},

		async _commitRead(entries: [string, string][]) {
			if (entries.length === 0) {
				return;
			}
			try {
				await api.markRssItemsRead(entries);
			} catch (_e) {
				for (const [fid, itemId] of entries) {
					const it = this.items[fid]?.find((i) => i.id === itemId);
					if (it) {
						it.is_read = false;
					}
				}
				if (this.unreadOnly) {
					this.ensurePageInRange();
				}
			}
		},

		async markAllRead(feedId?: string) {
			const feedIds = feedId ? [feedId] : Object.keys(this.items);
			const entries: [string, string][] = [];
			for (const fid of feedIds) {
				const feedItems = this.items[fid];
				if (!feedItems) {
					continue;
				}
				for (const item of feedItems) {
					if (!item.is_read) {
						item.is_read = true;
						entries.push([fid, item.id]);
					}
				}
			}
			if (this.unreadOnly) {
				this.ensurePageInRange();
			}
			await this._commitRead(entries);
		},

		async markItemsReadBulk(items: { feed_id: string; id: string }[]) {
			const entries: [string, string][] = [];
			for (const { feed_id, id } of items) {
				const feedItems = this.items[feed_id];
				if (!feedItems) {
					continue;
				}
				const item = feedItems.find((i) => i.id === id);
				if (item && !item.is_read) {
					item.is_read = true;
					entries.push([feed_id, id]);
				}
			}
			if (this.unreadOnly) {
				this.ensurePageInRange();
			}
			await this._commitRead(entries);
		},

		async updateFeedSettings(
			feedId: string,
			interval?: number,
			isActive?: boolean,
		) {
			await api.updateRssFeedSettings(feedId, interval, isActive);
			const feed = this.feeds.find((f) => f.id === feedId);
			if (feed) {
				if (interval !== undefined) {
					feed.update_interval_secs = interval;
				}
				if (isActive !== undefined) {
					feed.is_active = isActive;
				}
			}
		},

		toggleItemSelection(itemId: string) {
			const idx = this.selectedItemIds.indexOf(itemId);
			if (idx >= 0) {
				this.selectedItemIds = this.selectedItemIds.filter(
					(id) => id !== itemId,
				);
			} else {
				this.selectedItemIds = [...this.selectedItemIds, itemId];
			}
		},

		selectAllItems() {
			this.selectedItemIds = this.paginatedItems.map((i) => i.id);
		},

		setFilterText(text: string) {
			this.filterText = text;
			this.selectedItemIds = [];
			this.currentPage = 1;
		},

		clearSelection() {
			this.selectedItemIds = [];
		},

		setItemsPerPage(value: number | string) {
			const num = Number(value);
			if (!ITEMS_PER_PAGE_OPTIONS.includes(num)) {
				return;
			}
			this.itemsPerPage = num;
			this.currentPage = 1;
			this.selectedItemIds = [];
			try {
				localStorage.setItem(STORAGE_KEY, String(num));
			} catch {}
		},

		changePage(page: number) {
			const clamped = Math.max(1, Math.min(page, this.totalPages));
			if (clamped !== this.currentPage) {
				this.currentPage = clamped;
				this.selectedItemIds = [];
			}
		},

		ensurePageInRange() {
			if (this.currentPage > this.totalPages) {
				this.currentPage = this.totalPages;
			}
		},

		async clearItemDownload(feedId: string, itemId: string) {
			await api.clearRssDownload(feedId, itemId);
			const feedItems = this.items[feedId];
			if (feedItems) {
				const item = feedItems.find((i) => i.id === itemId);
				if (item) {
					item.is_downloaded = false;
					item.download_path = undefined;
				}
			}
		},

		async batchDownload() {
			const selected = new Set(this.selectedItemIds);
			const items = this.currentItems.filter(
				(i) =>
					selected.has(i.id) && !i.is_downloaded && (i.enclosure_url || i.link),
			);
			for (const item of items) {
				await this.downloadItem(item.feed_id, item.id);
			}
			this.selectedItemIds = [];
		},

		async batchDelete() {
			const selected = new Set(this.selectedItemIds);
			const items = this.currentItems.filter((i) => selected.has(i.id));
			const grouped = new Map<string, string[]>();
			for (const item of items) {
				const list = grouped.get(item.feed_id);
				if (list) {
					list.push(item.id);
				} else {
					grouped.set(item.feed_id, [item.id]);
				}
			}
			const itemsByFeed: [string, string[]][] = Array.from(grouped.entries());
			await api.deleteRssItems(itemsByFeed);
			for (const [feedId, itemIds] of itemsByFeed) {
				const feedItems = this.items[feedId];
				if (feedItems) {
					this.items[feedId] = feedItems.filter((i) => !itemIds.includes(i.id));
				}
			}
			this.selectedItemIds = [];
			this.ensurePageInRange();
		},

		unreadCount(feedId: string): number {
			const items = this.items[feedId] ?? [];
			return items.filter((i) => !i.is_read).length;
		},
	},
});

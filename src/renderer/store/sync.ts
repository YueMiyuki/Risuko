import {
	getCategoriesForKey,
	syncCategories,
	syncCategoryIds,
} from "@shared/syncCategories";
import {
	normalizeNetworkProxyConfig,
	normalizeProxyConfig,
} from "@shared/types/config";
import {
	ANDROID_USENET_ARCHIVE_LIMITS,
	DEFAULT_USENET_ARCHIVE_LIMITS,
} from "@shared/types/usenet";
import { changeKeysToKebabCase, getApiErrorMessage } from "@shared/utils";
import logger from "@shared/utils/logger";
import axios from "axios";
import { defineStore } from "pinia";
import api from "@/api";
import is from "@/shims/platform";
import { useAuthStore } from "./auth";
import { usePreferenceStore } from "./preference";

const getAxiosErrorMessage = (err: unknown): string =>
	getApiErrorMessage(err, "Sync failed");

interface SyncState {
	syncing: boolean;
	lastSyncAt: number | null;
}

let pushTimer: ReturnType<typeof setTimeout> | null = null;
const pendingCategories = new Set<string>();
const STATS_CATEGORY = "stats";

const shouldApplyCategoryData = (categoryId: string): boolean =>
	categoryId === STATS_CATEGORY ||
	categoryId === "usenet" ||
	categoryId === "network";

function mergeUsenetProfiles(
	localValue: unknown,
	remoteValue: unknown,
): unknown[] {
	const local = Array.isArray(localValue) ? localValue : [];
	const remote = Array.isArray(remoteValue) ? remoteValue : [];
	const byId = new Map<string, Record<string, unknown>>();
	for (const value of [...local, ...remote]) {
		if (!value || typeof value !== "object" || Array.isArray(value)) {
			continue;
		}
		const profile = value as Record<string, unknown>;
		const id = typeof profile.id === "string" ? profile.id : "";
		if (!id) {
			continue;
		}
		const previous = byId.get(id);
		const currentAt =
			typeof profile.updatedAt === "number" &&
			Number.isFinite(profile.updatedAt)
				? profile.updatedAt
				: 0;
		const previousAt =
			typeof previous?.updatedAt === "number" &&
			Number.isFinite(previous.updatedAt)
				? previous.updatedAt
				: 0;
		if (!previous || currentAt > previousAt) {
			byId.set(id, { ...profile });
		}
	}
	return [...byId.values()];
}

function mergeUsenetCategory(
	localData: Record<string, unknown>,
	remoteData: Record<string, unknown>,
): Record<string, unknown> {
	const defaults: Record<string, number> = is.android()
		? { ...ANDROID_USENET_ARCHIVE_LIMITS }
		: { ...DEFAULT_USENET_ARCHIVE_LIMITS };
	const limits = remoteData["usenet-archive-limits"];
	let adjusted = false;
	if (limits && typeof limits === "object" && !Array.isArray(limits)) {
		const clamped = { ...(limits as Record<string, unknown>) };
		for (const [key, fallback] of Object.entries(defaults)) {
			const value = Number(clamped[key]);
			if (
				!Number.isFinite(value) ||
				!Number.isSafeInteger(value) ||
				value <= 0
			) {
				clamped[key] = fallback;
				adjusted = true;
			} else if (value > fallback * 4) {
				clamped[key] = fallback * 4;
				adjusted = true;
			} else {
				clamped[key] = value;
			}
		}
		remoteData = { ...remoteData, "usenet-archive-limits": clamped };
	}
	return {
		...remoteData,
		"usenet-profiles": mergeUsenetProfiles(
			localData["usenet-profiles"],
			remoteData["usenet-profiles"],
		),
		"usenet-limits-adjusted":
			adjusted || remoteData["usenet-limits-adjusted"] === true,
	};
}

function normalizeNetworkCategory(
	data: Record<string, unknown>,
): Record<string, unknown> {
	return normalizeNetworkProxyConfig(data);
}

export const useSyncStore = defineStore("sync", {
	state: (): SyncState => ({
		syncing: false,
		lastSyncAt: null,
	}),
	actions: {
		getSelectedCategories(): string[] {
			const preferenceStore = usePreferenceStore();
			const selected =
				(preferenceStore.config.cloudSyncCategories as string[]) || [];
			return selected.filter((id) => syncCategoryIds.includes(id));
		},

		isAutoSyncEnabled(): boolean {
			const preferenceStore = usePreferenceStore();
			return (
				!!preferenceStore.config.cloudSyncAuto && !!useAuthStore().isLoggedIn
			);
		},

		async extractCategoryData(
			categoryId: string,
		): Promise<Record<string, unknown>> {
			if (categoryId === STATS_CATEGORY) {
				return (await api.exportDownloadStats()) as unknown as Record<
					string,
					unknown
				>;
			}

			const cat = syncCategories.find((c) => c.id === categoryId);
			if (!cat) {
				return {};
			}

			const preferenceStore = usePreferenceStore();
			const config = changeKeysToKebabCase(
				preferenceStore.config || {},
			) as Record<string, unknown>;
			const data: Record<string, unknown> = {};
			for (const key of cat.keys) {
				if (key in config) {
					data[key] =
						key === "proxy" ? normalizeProxyConfig(config[key]) : config[key];
				}
			}
			return data;
		},

		async applyCategoryData(
			categoryId: string,
			data: Record<string, unknown>,
		): Promise<void> {
			if (categoryId === STATS_CATEGORY) {
				await api.mergeDownloadStats(data);
				return;
			}
			if (categoryId === "usenet") {
				const localData = await this.extractCategoryData(categoryId);
				data = mergeUsenetCategory(localData, data);
			}
			if (categoryId === "network") {
				data = normalizeNetworkCategory(data);
			}
			await usePreferenceStore().save(data, { skipSync: true });
		},

		getCategoryTimestamp(categoryId: string): number | undefined {
			const preferenceStore = usePreferenceStore();
			const timestamps = preferenceStore.config.cloudSyncCategoryTimestamps as
				| Record<string, number>
				| undefined;
			return timestamps?.[categoryId];
		},

		async setCategoryTimestamp(
			categoryId: string,
			timestamp: number,
		): Promise<void> {
			const preferenceStore = usePreferenceStore();
			const timestamps = {
				...preferenceStore.config.cloudSyncCategoryTimestamps,
			};
			timestamps[categoryId] = timestamp;
			await preferenceStore.save(
				{ cloudSyncCategoryTimestamps: timestamps },
				{ skipSync: true },
			);
		},

		async pushCategory(categoryId: string): Promise<number | undefined> {
			const authStore = useAuthStore();
			if (!authStore.isLoggedIn || !authStore.token) {
				return;
			}

			const data = await this.extractCategoryData(categoryId);
			if (Object.keys(data).length === 0) {
				return;
			}

			const { data: resp } = await axios.put(
				`${authStore.serverUrl}/settings`,
				{ category: categoryId, data },
				{ headers: authStore.getAuthHeaders() },
			);
			return resp.updatedAt as number;
		},

		async pullAll(): Promise<void> {
			const authStore = useAuthStore();
			if (!authStore.isLoggedIn || !authStore.token) {
				throw new Error("Not logged in");
			}

			this.syncing = true;
			try {
				const { data } = await axios.get(`${authStore.serverUrl}/settings`, {
					headers: authStore.getAuthHeaders(),
				});

				const preferenceStore = usePreferenceStore();
				const selectedCategories = this.getSelectedCategories();
				const settings = data.settings || {};
				const timestamps = data.timestamps || {};
				const merged: Record<string, unknown> = {};
				const pulledTimestamps: Record<string, number> = {};

				for (const categoryId of selectedCategories) {
					const remoteData = settings[categoryId] as
						| Record<string, unknown>
						| undefined;
					if (remoteData) {
						if (shouldApplyCategoryData(categoryId)) {
							await this.applyCategoryData(categoryId, remoteData);
						} else {
							Object.assign(merged, remoteData);
						}
						if (timestamps[categoryId]) {
							pulledTimestamps[categoryId] = timestamps[categoryId] as number;
						}
					}
				}

				if (Object.keys(merged).length > 0) {
					await preferenceStore.save(merged, { skipSync: true });
				}

				for (const [cat, ts] of Object.entries(pulledTimestamps)) {
					await this.setCategoryTimestamp(cat, ts);
				}

				this.lastSyncAt = Date.now();
				await preferenceStore.save({ cloudSyncLastAt: this.lastSyncAt });
				logger.info("[Risuko] sync pullAll done");
			} catch (err) {
				const message = getAxiosErrorMessage(err);
				logger.warn("[Risuko] sync pullAll failed:", message);
				throw new Error(message);
			} finally {
				this.syncing = false;
			}
		},

		async syncBidirectional(): Promise<void> {
			const authStore = useAuthStore();
			if (!authStore.isLoggedIn || !authStore.token) {
				throw new Error("Not logged in");
			}

			this.syncing = true;
			try {
				const { data } = await axios.get(`${authStore.serverUrl}/settings`, {
					headers: authStore.getAuthHeaders(),
				});

				const preferenceStore = usePreferenceStore();
				const selectedCategories = this.getSelectedCategories();
				const remoteSettings = data.settings || {};
				const remoteTimestamps = data.timestamps || {};

				const toPull: Record<string, unknown> = {};
				const toPullTimestamps: Record<string, number> = {};

				for (const categoryId of selectedCategories) {
					const localTimestamp = this.getCategoryTimestamp(categoryId);
					const remoteTimestamp = remoteTimestamps[categoryId] as
						| number
						| undefined;
					const localData = await this.extractCategoryData(categoryId);
					const remoteData = remoteSettings[categoryId] as
						| Record<string, unknown>
						| undefined;
					const hasLocal = Object.keys(localData).length > 0;
					const hasRemote = remoteTimestamp !== undefined;

					if (!hasLocal && !hasRemote) {
						continue;
					}

					if (!hasLocal && hasRemote && remoteData) {
						if (shouldApplyCategoryData(categoryId)) {
							await this.applyCategoryData(categoryId, remoteData);
						} else {
							Object.assign(toPull, remoteData);
						}
						toPullTimestamps[categoryId] = remoteTimestamp;
						continue;
					}

					if (hasLocal && !hasRemote) {
						const updatedAt = await this.pushCategory(categoryId);
						if (updatedAt) {
							await this.setCategoryTimestamp(categoryId, updatedAt);
						}
						continue;
					}

					if (localTimestamp === undefined && hasRemote && remoteData) {
						if (shouldApplyCategoryData(categoryId)) {
							await this.applyCategoryData(categoryId, remoteData);
						} else {
							Object.assign(toPull, remoteData);
						}
						toPullTimestamps[categoryId] = remoteTimestamp;
						continue;
					}

					if (localTimestamp && remoteTimestamp) {
						if (localTimestamp > remoteTimestamp) {
							const updatedAt = await this.pushCategory(categoryId);
							if (updatedAt) {
								await this.setCategoryTimestamp(categoryId, updatedAt);
							}
						} else if (remoteTimestamp > localTimestamp && remoteData) {
							if (shouldApplyCategoryData(categoryId)) {
								await this.applyCategoryData(categoryId, remoteData);
							} else {
								Object.assign(toPull, remoteData);
							}
							toPullTimestamps[categoryId] = remoteTimestamp;
						}
					}
				}

				if (Object.keys(toPull).length > 0) {
					await preferenceStore.save(toPull, { skipSync: true });
				}

				for (const [cat, ts] of Object.entries(toPullTimestamps)) {
					await this.setCategoryTimestamp(cat, ts);
				}

				this.lastSyncAt = Date.now();
				await preferenceStore.save({ cloudSyncLastAt: this.lastSyncAt });
				logger.info("[Risuko] sync bidirectional done");
			} catch (err) {
				const message = getAxiosErrorMessage(err);
				logger.warn("[Risuko] sync bidirectional failed:", message);
				throw new Error(message);
			} finally {
				this.syncing = false;
			}
		},

		async syncOnStartup(): Promise<void> {
			if (!this.isAutoSyncEnabled()) {
				return;
			}
			try {
				await this.syncBidirectional();
			} catch (err) {
				logger.warn("[Risuko] syncOnStartup failed:", (err as Error).message);
			}
		},

		syncCategoryOnChange(categoryId: string): void {
			if (!this.isAutoSyncEnabled()) {
				return;
			}
			if (!this.getSelectedCategories().includes(categoryId)) {
				return;
			}

			pendingCategories.add(categoryId);
			this.schedulePendingPush();
		},

		async markCategoryChanged(categoryId: string): Promise<void> {
			await this.setCategoryTimestamp(
				categoryId,
				Math.floor(Date.now() / 1000),
			);
			this.syncCategoryOnChange(categoryId);
		},

		syncOnChange(changedConfig: Record<string, unknown>): void {
			if (!this.isAutoSyncEnabled()) {
				return;
			}

			const kebabConfig = changeKeysToKebabCase(changedConfig);

			for (const key of Object.keys(kebabConfig)) {
				const cats = getCategoriesForKey(key);
				for (const cat of cats) {
					this.syncCategoryOnChange(cat);
				}
			}
		},

		schedulePendingPush(): void {
			if (pendingCategories.size === 0) {
				return;
			}

			if (pushTimer) {
				clearTimeout(pushTimer);
			}

			pushTimer = setTimeout(async () => {
				pushTimer = null;
				const categories = [...pendingCategories];
				pendingCategories.clear();
				try {
					for (const categoryId of categories) {
						const updatedAt = await this.pushCategory(categoryId);
						if (updatedAt) {
							await this.setCategoryTimestamp(categoryId, updatedAt);
						}
					}
					this.lastSyncAt = Date.now();
					await api.savePreference({ cloudSyncLastAt: this.lastSyncAt });
				} catch (err) {
					logger.warn("[Risuko] syncOnChange failed:", (err as Error).message);
				}
			}, 500);
		},
	},
});

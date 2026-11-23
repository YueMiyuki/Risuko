import type {
	SinkConfig,
	UploadJob,
	UploadRule,
	UploadSinkRecord,
} from "@shared/types/upload";
import logger from "@shared/utils/logger";
import { defineStore } from "pinia";
import api from "@/api";

interface UploadState {
	sinks: UploadSinkRecord[];
	rules: UploadRule[];
	jobs: UploadJob[];
	defaultSinkId: string | null;
	loading: boolean;
	_eventUnlisteners: (() => void)[];
	_listenerSetupToken: symbol | null;
	_pollTimer: ReturnType<typeof setInterval> | null;
	_isRefreshing: boolean;
}

const POLL_MS = 1000;

function withoutSecrets(config: SinkConfig): SinkConfig {
	switch (config.kind) {
		case "s3": {
			const { secretAccessKey: _, ...rest } = config;
			return rest;
		}
		case "sftp": {
			const { password: _, privateKey: __, ...rest } = config;
			return rest;
		}
		default: {
			const { password: _, ...rest } = config;
			return rest;
		}
	}
}

export const useUploadSinkStore = defineStore("uploadSink", {
	state: (): UploadState => ({
		sinks: [],
		rules: [],
		jobs: [],
		defaultSinkId: null,
		loading: false,
		_eventUnlisteners: [],
		_listenerSetupToken: null,
		_pollTimer: null,
		_isRefreshing: false,
	}),

	getters: {
		hasActiveJobs(): boolean {
			return this.jobs.some(
				(j) => j.status === "active" || j.status === "queued",
			);
		},
	},

	actions: {
		async fetchAll() {
			this.loading = true;
			try {
				const [sinks, rules, def, jobs] = await Promise.all([
					api.listUploadSinks(),
					api.listUploadRules(),
					api.getDefaultUploadSink(),
					api.listUploadJobs(),
				]);
				this.sinks = sinks;
				this.rules = rules;
				this.defaultSinkId = def;
				this.jobs = jobs;
			} finally {
				this.loading = false;
			}
		},

		async addSink(record: Parameters<typeof api.addUploadSink>[0]) {
			const created = await api.addUploadSink(record);
			this.sinks.push(created);
			if (!this.defaultSinkId) {
				try {
					await api.setDefaultUploadSink(created.id);
					this.defaultSinkId = created.id;
				} catch (err) {
					logger.warn("uploadSink: failed to persist default sink", err);
					await this._resyncFromBackend();
				}
			}
			return created;
		},

		async updateSink(record: UploadSinkRecord) {
			await api.updateUploadSink(record);
			const idx = this.sinks.findIndex((s) => s.id === record.id);
			if (idx >= 0) {
				this.sinks[idx] = { ...record, config: withoutSecrets(record.config) };
			}
		},

		async removeSink(id: string) {
			await api.removeUploadSink(id);
			this.sinks = this.sinks.filter((s) => s.id !== id);
			this.rules = this.rules.filter((r) => r.sinkId !== id);
			if (this.defaultSinkId === id) {
				const nextId = this.sinks[0]?.id ?? null;
				try {
					await api.setDefaultUploadSink(nextId);
					this.defaultSinkId = nextId;
				} catch (err) {
					logger.warn(
						"uploadSink: failed to update default sink after remove",
						err,
					);
					await this._resyncFromBackend();
				}
			}
		},

		async _resyncFromBackend() {
			try {
				const [sinks, def] = await Promise.all([
					api.listUploadSinks(),
					api.getDefaultUploadSink(),
				]);
				this.sinks = sinks;
				this.defaultSinkId = def;
			} catch (err) {
				logger.error("uploadSink: backend resync failed", err);
			}
		},

		testSink(id: string) {
			return api.testUploadSink(id);
		},

		async setDefaultSink(id: string | null) {
			await api.setDefaultUploadSink(id);
			this.defaultSinkId = id;
		},

		async addRule(rule: Parameters<typeof api.addUploadRule>[0]) {
			const created = await api.addUploadRule(rule);
			this.rules.push(created);
			return created;
		},

		async updateRule(rule: UploadRule) {
			await api.updateUploadRule(rule);
			const idx = this.rules.findIndex((r) => r.id === rule.id);
			if (idx >= 0) {
				this.rules[idx] = rule;
			}
		},

		async removeRule(id: string) {
			await api.removeUploadRule(id);
			this.rules = this.rules.filter((r) => r.id !== id);
		},

		async refreshJobs() {
			if (this._isRefreshing) {
				return;
			}
			this._isRefreshing = true;
			try {
				this.jobs = await api.listUploadJobs();
			} catch (err) {
				logger.warn("[Risuko] upload jobs refresh failed:", err);
			} finally {
				this._isRefreshing = false;
			}
		},

		cancelJob(id: string) {
			return api.cancelUploadJob(id);
		},

		async clearHistory() {
			await api.clearUploadHistory();
			await this.refreshJobs();
		},

		_upsertJob(payload: UploadJob) {
			const idx = this.jobs.findIndex((j) => j.id === payload.id);
			if (idx >= 0) {
				this.jobs[idx] = payload;
			} else {
				this.jobs.unshift(payload);
			}
		},

		async initEventListeners() {
			this.cleanupEventListeners();
			const setupToken = Symbol("upload-sink-listener-setup");
			this._listenerSetupToken = setupToken;
			const pendingUnlisteners: (() => void)[] = [];
			try {
				const { listen } = await import("@tauri-apps/api/event");
				const events = [
					"engine:upload-queued",
					"engine:upload-start",
					"engine:upload-complete",
					"engine:upload-error",
					"engine:upload-cancelled",
				];
				for (const ev of events) {
					const u = await listen<UploadJob>(ev, (e) => {
						if (e.payload) {
							this._upsertJob(e.payload);
						}
					});
					pendingUnlisteners.push(u);
					if (this._listenerSetupToken !== setupToken) {
						for (const unlisten of pendingUnlisteners) {
							unlisten();
						}
						return;
					}
				}
				this._eventUnlisteners.push(...pendingUnlisteners);
				if (this._listenerSetupToken === setupToken && !this._pollTimer) {
					this._pollTimer = setInterval(() => {
						if (this.hasActiveJobs) {
							this.refreshJobs();
						}
					}, POLL_MS);
				}
			} catch (err) {
				for (const unlisten of pendingUnlisteners) {
					unlisten();
				}
				logger.warn(
					"[Risuko] upload sink event listener setup failed:",
					(err as Error)?.message || err,
				);
			}
		},

		cleanupEventListeners() {
			this._listenerSetupToken = null;
			for (const u of this._eventUnlisteners) {
				u();
			}
			this._eventUnlisteners = [];
			if (this._pollTimer) {
				clearInterval(this._pollTimer);
				this._pollTimer = null;
			}
		},
	},
});

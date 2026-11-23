import { getLanguage } from "@shared/locales";
import logger from "@shared/utils/logger";
import axios from "axios";
import { defineStore } from "pinia";
import api from "@/api";
import { normalizeSyncServer } from "@/utils/syncServer";
import { usePreferenceStore } from "./preference";

const syncAxios = axios.create({ timeout: 10_000 });
const STARTUP_CONFIG_WAIT_MS = 1500;

const SYNC_CLIENT_VERSION = "2";
const PENDING_LOGIN_KEY = "risuko.pendingLogin";
const PENDING_LOGIN_TTL = 15 * 60 * 1000;

function base64Url(bytes: Uint8Array): string {
	let binary = "";
	for (const byte of bytes) {
		binary += String.fromCharCode(byte);
	}
	return btoa(binary)
		.replace(/\+/g, "-")
		.replace(/\//g, "_")
		.replace(/=+$/, "");
}

function clearPendingLogin() {
	try {
		localStorage.removeItem(PENDING_LOGIN_KEY);
	} catch {}
}

function readPendingLoginSecret(): string | null {
	try {
		const raw = localStorage.getItem(PENDING_LOGIN_KEY);
		if (!raw) {
			return null;
		}
		const pending = JSON.parse(raw);
		if (typeof pending?.secret === "string" && pending.expiresAt > Date.now()) {
			return pending.secret;
		}
	} catch {}
	clearPendingLogin();
	return null;
}

async function beginLogin(): Promise<string> {
	const secret =
		readPendingLoginSecret() ??
		base64Url(crypto.getRandomValues(new Uint8Array(32)));
	localStorage.setItem(
		PENDING_LOGIN_KEY,
		JSON.stringify({ secret, expiresAt: Date.now() + PENDING_LOGIN_TTL }),
	);
	const digest = await crypto.subtle.digest(
		"SHA-256",
		new TextEncoder().encode(secret),
	);
	return base64Url(new Uint8Array(digest));
}

interface SyncUser {
	id: string;
	email: string | null;
	githubUsername: string | null;
}

export type LinkLoginOutcome = "signed-in" | "failed" | "ignored";

interface AuthState {
	user: SyncUser | null;
	token: string | null;
	isLoggedIn: boolean;
	loading: boolean;
	serverConfig: {
		email: boolean;
		github: boolean;
		loginHandoff: boolean;
	} | null;
}

export const useAuthStore = defineStore("auth", {
	state: (): AuthState => ({
		user: null,
		token: null,
		isLoggedIn: false,
		loading: false,
		serverConfig: null,
	}),
	getters: {
		serverUrl(): string {
			return normalizeSyncServer(
				usePreferenceStore().config.cloudSyncServerUrl as string | undefined,
			);
		},
	},
	actions: {
		getAuthHeaders() {
			return this.token
				? {
						Authorization: `Bearer ${this.token}`,
						"X-Risuko-Client": SYNC_CLIENT_VERSION,
					}
				: {};
		},

		async fetchServerConfig(): Promise<void> {
			try {
				const { data } = await syncAxios.get(`${this.serverUrl}/config`);
				this.serverConfig = {
					email: data.authMethods?.email === true,
					github: data.authMethods?.github === true,
					loginHandoff: data.loginHandoff === true,
				};
			} catch (err) {
				logger.warn(
					"[Risuko] fetchServerConfig failed:",
					(err as Error).message,
				);
				this.serverConfig = null;
			}
		},

		async requestOTP(email: string): Promise<{ linkOnly: boolean }> {
			const challenge = await beginLogin().catch(() => {
				clearPendingLogin();
				return undefined;
			});
			if (!challenge && this.serverConfig?.loginHandoff) {
				throw new Error("Sign-in could not be started on this device");
			}
			try {
				const preferenceStore = usePreferenceStore();
				const locale = getLanguage(preferenceStore.config.locale || "auto");
				const { data } = await syncAxios.post(
					`${this.serverUrl}/auth/magic-link`,
					{ email, locale, challenge },
				);
				return { linkOnly: data?.linkOnly === true };
			} catch (err) {
				throw new Error(
					(err as { response?: { data?: { error?: string } } }).response?.data
						?.error || "Failed to send verification code",
				);
			}
		},

		async verifyOTP(email: string, code: string): Promise<void> {
			const secret = readPendingLoginSecret() ?? undefined;
			if (!secret && this.serverConfig?.loginHandoff) {
				throw new Error("This code has expired. Request a new one.");
			}
			try {
				const { data } = await syncAxios.post(
					`${this.serverUrl}/auth/verify-otp`,
					{ email, code, secret },
				);
				this.token = data.token;
				this.user = data.user;
				this.isLoggedIn = true;
				clearPendingLogin();
				await this.persistAuth();
			} catch (err) {
				throw new Error(
					(err as { response?: { data?: { error?: string } } }).response?.data
						?.error || "Verification failed",
				);
			}
		},

		async loginWithGitHub(): Promise<void> {
			const url = `${this.serverUrl}/auth/github?challenge=${await beginLogin()}`;
			const { invoke } = await import("@tauri-apps/api/core");
			try {
				await invoke("plugin:shell|open", { path: url });
			} catch {
				window.open(url, "_blank");
			}
		},

		async completeLogin(code: string | null): Promise<LinkLoginOutcome> {
			const secret = readPendingLoginSecret();
			if (!secret) {
				logger.warn(
					"[Risuko] sign-in link ignored: no sign-in was started here",
				);
				return "ignored";
			}
			if (!code) {
				logger.warn("[Risuko] sign-in link carried no login code");
				return "failed";
			}
			try {
				const { data } = await syncAxios.post(
					`${this.serverUrl}/auth/exchange`,
					{
						code,
						secret,
					},
				);
				this.token = data.token;
				this.user = data.user;
				this.isLoggedIn = true;
				clearPendingLogin();
				await this.persistAuth();
				return "signed-in";
			} catch (err) {
				logger.warn(
					"[Risuko] sign-in link could not be redeemed:",
					(err as Error).message,
				);
				return "failed";
			}
		},

		async fetchMe(): Promise<boolean> {
			const token = this.token;
			if (!token) {
				return false;
			}
			try {
				const { data } = await syncAxios.get(`${this.serverUrl}/auth/me`, {
					headers: this.getAuthHeaders(),
				});
				if (this.token !== token) {
					return false;
				}
				this.user = data;
				this.isLoggedIn = true;
				return true;
			} catch (err) {
				const status = (err as { response?: { status?: number } }).response
					?.status;
				if (this.token !== token) {
					return false;
				}
				if (status === 401 || status === 403) {
					logger.warn("[Risuko] fetchMe failed: token invalid");
					this.clearAuth();
					await this.persistAuth();
				} else {
					logger.warn(
						"[Risuko] fetchMe failed (network):",
						(err as Error).message,
					);
				}
				return false;
			}
		},

		async logout(): Promise<void> {
			if (this.token) {
				try {
					await syncAxios.post(
						`${this.serverUrl}/auth/logout`,
						{},
						{ headers: this.getAuthHeaders() },
					);
				} catch (err) {
					logger.warn("[Risuko] logout failed:", (err as Error).message);
				}
			}
			this.clearAuth();
			clearPendingLogin();
			await this.persistAuth();
			try {
				const { useSyncStore } = await import("./sync");
				useSyncStore().resetLocalState();
				await usePreferenceStore().save(
					{
						cloudSyncCategoryTimestamps: {},
						cloudSyncCategoryRevs: {},
						cloudSyncDirtyKeys: {},
						cloudSyncLastAt: 0,
					},
					{ skipSync: true },
				);
			} catch (err) {
				logger.warn(
					"[Risuko] clearing sync state failed:",
					(err as Error).message,
				);
			}
		},

		async persistAuth(): Promise<void> {
			await api.savePreference({
				cloudSyncToken: this.token ?? null,
			});
		},

		clearAuth() {
			this.user = null;
			this.token = null;
			this.isLoggedIn = false;
		},

		async initFromConfig(): Promise<void> {
			await Promise.race([
				this.fetchServerConfig(),
				new Promise<void>((resolve) =>
					setTimeout(resolve, STARTUP_CONFIG_WAIT_MS),
				),
			]);
			const token = api.config?.cloudSyncToken as string | undefined;
			if (token) {
				this.token = token;
				this.isLoggedIn = true;
				this.fetchMe();
			}
		},
	},
});

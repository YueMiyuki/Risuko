<template>
  <Dialog :open="visible" @update:open="onOpenChange">
    <DialogContent
      :show-close-button="false"
      class="flex max-h-[80vh] w-130 flex-col gap-0 overflow-hidden p-0"
    >
      <DialogHeader class="flex shrink-0 flex-row items-center gap-2 border-b border-border/60 px-4 py-3">
        <div class="flex size-7 items-center justify-center rounded-md bg-amber-500/15 text-amber-600 dark:text-amber-400">
          <ShieldAlert :size="14" />
        </div>
        <DialogTitle class="flex-1 text-sm font-medium">{{ $t('task.task-cloudflare-title') }}</DialogTitle>
      </DialogHeader>

      <div class="atd-scroll min-h-0 flex-1 overflow-y-auto">
        <div class="flex flex-col gap-3 px-4 py-3 text-xs">
          <p
            :class="retried
              ? 'leading-relaxed text-amber-600 dark:text-amber-400'
              : 'leading-relaxed text-muted-foreground'"
          >
            {{ retried ? $t('task.task-cloudflare-tls-fingerprint') : $t('task.task-cloudflare-explain') }}
          </p>
          <div v-if="host" class="rounded-md border border-border/60 bg-muted/40 px-2.5 py-1.5 text-[11px]">
            <span class="text-muted-foreground">{{ $t('preferences.saved-cookies-host') }}:</span>
            <span class="ml-1 font-mono">{{ host }}</span>
          </div>

          <div class="flex flex-col gap-1">
            <span class="text-[11px] text-muted-foreground">
              {{ $t('task.task-cloudflare-pick-browser') }}
            </span>
            <div class="grid grid-cols-2 gap-1">
              <template v-if="loadingBrowsers">
                <div
                  v-for="i in browserSlotCount"
                  :key="`skel-${i}`"
                  class="h-7 animate-pulse rounded-md border border-border/60 bg-muted/40"
                />
              </template>
              <template v-else-if="browsers.length === 0">
                <div
                  class="col-span-2 px-1 py-1 text-[11px] text-muted-foreground"
                >
                  {{ $t('task.task-cookie-import-no-browsers') }}
                </div>
              </template>
              <template v-else>
                <button
                  v-for="b in browsers"
                  :key="b.id"
                  type="button"
                  class="rounded-md flex items-center justify-between gap-2 border border-border/60 px-2 py-1.5 text-left text-[11px] transition-colors hover:bg-muted disabled:opacity-50"
                  :class="{ 'border-primary/70 bg-primary/5': selectedBrowser === b.id }"
                  :disabled="!b.available || importing || retrying"
                  @click="selectBrowser(b)"
                >
                  <span class="truncate">{{ b.name }}</span>
                  <span v-if="!b.available" class="text-[10px] text-muted-foreground">n/a</span>
                  <span
                    v-else-if="importing && selectedBrowser === b.id"
                    class="text-[10px] text-muted-foreground"
                  >
                    ...
                  </span>
                </button>
              </template>
            </div>
          </div>

          <div
            class="rounded-md flex flex-col gap-2 border border-border/60 bg-muted/30 p-2 transition-opacity"
            :class="imported ? 'opacity-100' : 'opacity-60'"
          >
            <div class="flex items-center justify-between">
              <div class="text-[11px] font-medium">
                {{ imported
                  ? $t('task.task-cloudflare-preview-title', { count: imported.count })
                  : $t('task.task-cloudflare-preview') }}
              </div>
              <span
                v-if="imported && imported.hasCfClearance"
                class="rounded-full bg-emerald-500/15 px-2 py-0.5 text-[10px] text-emerald-700 dark:text-emerald-400"
              >
                cf_clearance
              </span>
              <span
                v-else-if="imported"
                class="rounded-full bg-amber-500/15 px-2 py-0.5 text-[10px] text-amber-700 dark:text-amber-400"
              >
                {{ $t('task.task-cloudflare-no-clearance-badge') }}
              </span>
              <span
                v-else
                aria-hidden="true"
                class="rounded-full px-2 py-0.5 text-[10px] opacity-0"
              >
                cf_clearance
              </span>
            </div>

            <div>
              <label class="mb-1 flex items-center justify-between gap-2 text-[10px] uppercase tracking-wide text-muted-foreground">
                <span>{{ $t('task.task-user-agent') }}</span>
                <UiButton
                  type="button"
                  size="sm"
                  variant="ghost"
                  class="h-6 px-2 text-[10px] normal-case tracking-normal"
                  :disabled="capturingUa"
                  @click="handleCaptureUa"
                >
                  <Wand2 :size="11" class="mr-1" :class="{ 'animate-pulse': capturingUa }" />
                  {{ capturingUa ? $t('task.task-cloudflare-ua-capturing') : $t('task.task-cloudflare-ua-detect') }}
                </UiButton>
              </label>
              <Textarea
                v-model="userAgentDraft"
                rows="2"
                class="resize-none text-[11px] font-mono"
                :placeholder="$t('task.task-cloudflare-ua-placeholder')"
              />
              <p class="mt-1 text-[10px] text-muted-foreground">
                {{ $t('task.task-cloudflare-ua-hint') }}
              </p>
            </div>

            <details v-if="imported" class="text-[10px]">
              <summary class="cursor-pointer text-muted-foreground hover:text-foreground">
                {{ $t('task.task-cloudflare-show-cookies', { count: imported.cookies.length }) }}
              </summary>
              <div class="rounded-md mt-1 max-h-48 overflow-y-auto border border-border/60 bg-background">
                <table class="w-full text-left font-mono text-[10px]">
                  <thead class="sticky top-0 bg-muted/60 text-muted-foreground">
                    <tr>
                      <th class="px-1.5 py-1">name</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr v-for="c in imported.cookies" :key="c.name" class="border-t border-border/40 align-top">
                      <td class="px-1.5 py-1 whitespace-nowrap">{{ c.name }}</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </details>
            <div
              v-else
              aria-hidden="true"
              class="text-[10px] text-muted-foreground/50"
            >
              {{ $t('task.task-cloudflare-show-cookies', { count: 0 }) }}
            </div>
          </div>

          <ui-checkbox
            :model-value="skipForDomain"
            @change="(v) => (skipForDomain = !!v)"
          >
            {{ $t('task.task-cloudflare-skip') }}
          </ui-checkbox>
        </div>
      </div>

      <DialogFooter class="border-t border-border/60 px-4 py-3">
        <UiButton
          type="button"
          variant="ghost"
          size="sm"
          :disabled="retrying || importing"
          @click="handleCancel"
        >
          {{ $t('task.task-cloudflare-cancel') }}
        </UiButton>
        <UiButton
          type="button"
          variant="outline"
          size="sm"
          :disabled="!url || retrying || importing"
          @click="handleOpenInBrowser"
        >
          <ExternalLink :size="12" class="mr-1" />
          {{ $t('task.task-cloudflare-open-browser') }}
        </UiButton>
        <UiButton
          type="button"
          size="sm"
          :disabled="!selectedBrowser || retrying || importing"
          @click="imported ? handleApplyAndRetry() : handlePreview()"
        >
          <RefreshCw
            v-if="imported"
            :size="12"
            class="mr-1"
            :class="{ 'animate-spin': retrying }"
          />
          <Eye
            v-else
            :size="12"
            class="mr-1"
            :class="{ 'animate-pulse': importing }"
          />
          {{ imported
            ? $t('task.task-cloudflare-retry')
            : $t('task.task-cloudflare-preview') }}
        </UiButton>
      </DialogFooter>
    </DialogContent>
  </Dialog>
</template>

<script lang="ts">
import { ExternalLink, Eye, RefreshCw, ShieldAlert, Wand2 } from "@lucide/vue";
import logger from "@shared/utils/logger";
import { toast } from "vue-sonner";
import api, { type BrowserInfo, type ImportedCookies } from "@/api";
import UiButton from "@/components/ui/compat/UiButton.vue";
import UiCheckbox from "@/components/ui/compat/UiCheckbox.vue";
import {
	Dialog,
	DialogContent,
	DialogFooter,
	DialogHeader,
	DialogTitle,
} from "@/components/ui/dialog";
import { Textarea } from "@/components/ui/textarea";
import { useAppStore } from "@/store/app";
import { usePreferenceStore } from "@/store/preference";
import { cookieImportErrorKey } from "@/utils/cookieImport";
import { openExternalUrl } from "@/utils/external";

let cachedBrowsers: BrowserInfo[] | null = null;

const platformSlotCount = (): number => {
	if (typeof navigator === "undefined") {
		return 10;
	}
	const ua = navigator.userAgent || "";
	if (/Mac/i.test(ua)) {
		return 12;
	}
	if (/Windows/i.test(ua)) {
		return 13;
	}
	if (/Linux/i.test(ua)) {
		return 11;
	}
	return 10;
};

export default {
	name: "cloudflare-dialog",
	components: {
		Dialog,
		DialogContent,
		DialogHeader,
		DialogTitle,
		DialogFooter,
		UiButton,
		[UiCheckbox.name as string]: UiCheckbox,
		Textarea,
		ShieldAlert,
		ExternalLink,
		RefreshCw,
		Eye,
		Wand2,
	},
	data() {
		return {
			browsers: [] as BrowserInfo[],
			loadingBrowsers: false,
			selectedBrowser: "",
			importing: false,
			retrying: false,
			capturingUa: false,
			skipForDomain: false,
			imported: null as ImportedCookies | null,
			userAgentDraft: "",
		};
	},
	computed: {
		dialogState(): {
			visible: boolean;
			gid: string;
			host: string;
			url: string;
			taskName: string;
			retried: boolean;
			lastImportedNames: string[];
		} {
			return useAppStore().cloudflareDialog;
		},
		visible(): boolean {
			return this.dialogState.visible;
		},
		gid(): string {
			return this.dialogState.gid;
		},
		host(): string {
			return this.dialogState.host;
		},
		url(): string {
			return this.dialogState.url;
		},
		retried(): boolean {
			return this.dialogState.retried;
		},
		browserSlotCount(): number {
			if (this.browsers.length > 0) {
				return this.browsers.length;
			}
			if (cachedBrowsers && cachedBrowsers.length > 0) {
				return cachedBrowsers.length;
			}
			return platformSlotCount();
		},
	},
	watch: {
		visible(value: boolean) {
			if (value) {
				this.refreshBrowsers();
				this.skipForDomain = false;
				this.imported = null;
				this.userAgentDraft =
					(usePreferenceStore().config.userAgent as string | undefined) || "";
			} else {
				this.selectedBrowser = "";
			}
		},
	},
	methods: {
		async refreshBrowsers() {
			if (cachedBrowsers && cachedBrowsers.length > 0) {
				this.browsers = cachedBrowsers;
				this.loadingBrowsers = false;
				const firstAvailable = this.browsers.find((b) => b.available);
				if (firstAvailable && !this.selectedBrowser) {
					this.selectedBrowser = firstAvailable.id;
				}
			} else {
				this.loadingBrowsers = true;
			}
			try {
				const fresh = await api.listBrowsers();
				cachedBrowsers = fresh;
				this.browsers = fresh;
				const firstAvailable = fresh.find((b) => b.available);
				if (firstAvailable && !this.selectedBrowser) {
					this.selectedBrowser = firstAvailable.id;
				}
			} finally {
				this.loadingBrowsers = false;
			}
		},
		selectBrowser(b: BrowserInfo) {
			if (this.selectedBrowser !== b.id) {
				this.selectedBrowser = b.id;
				this.imported = null;
				this.userAgentDraft =
					(usePreferenceStore().config.userAgent as string | undefined) || "";
			}
		},
		onOpenChange(value: boolean) {
			if (!value && !this.retrying && !this.importing) {
				this.handleCancel();
			}
		},
		handleCancel() {
			if (this.skipForDomain && this.host) {
				useAppStore().addCloudflareSkipHost(this.host);
			}
			if (this.gid) {
				useAppStore().clearCloudflareRetryFlag(this.gid);
			}
			useAppStore().hideCloudflareDialog();
		},
		async handleOpenInBrowser() {
			if (!this.url) {
				return;
			}
			try {
				if (!(await openExternalUrl(this.url))) {
					toast.error(this.$t("app.open-url-failed"));
				}
			} catch (e) {
				toast.error(String(e));
			}
		},
		async handleCaptureUa() {
			this.capturingUa = true;
			try {
				toast.info(this.$t("task.task-cloudflare-ua-capturing-hint"));
				const { userAgent } = await api.captureUserAgent();
				if (userAgent) {
					this.userAgentDraft = userAgent;
					logger.log(`[Risuko] CF dialog: captured ua=${userAgent}`);
					try {
						await usePreferenceStore().save({ userAgent });
						toast.success(this.$t("task.task-cloudflare-ua-captured-saved"), {
							description: userAgent,
						});
					} catch (e) {
						logger.warn(
							`[Risuko] CF dialog: persist UA preference failed: ${String(e)}`,
						);
						toast.success(this.$t("task.task-cloudflare-ua-captured"), {
							description: userAgent,
						});
					}
				} else {
					toast.warning(this.$t("task.task-cloudflare-ua-empty"));
				}
			} catch (e) {
				logger.error(`[Risuko] CF dialog: capture ua failed: ${String(e)}`);
				toast.error(
					this.$t("task.task-cloudflare-ua-failed", { error: String(e) }),
				);
			} finally {
				this.capturingUa = false;
			}
		},
		async handlePreview() {
			if (!this.selectedBrowser) {
				return;
			}
			this.importing = true;
			try {
				const targetUrl = this.url || `https://${this.host}`;
				logger.log(
					`[Risuko] CF dialog: previewing cookies browser=${this.selectedBrowser} url=${targetUrl}`,
				);
				const imported = await api.importBrowserCookies({
					browser: this.selectedBrowser,
					url: targetUrl,
					persist: false,
				});
				logger.log(
					`[Risuko] CF dialog: imported ${imported.count} cookie(s) from ${this.selectedBrowser} for host=${imported.host}`,
					{
						hasCfClearance: imported.hasCfClearance,
					},
				);
				this.imported = imported;
				if (!(this.userAgentDraft || "").trim()) {
					this.userAgentDraft = imported.userAgent || "";
				}
			} catch (e) {
				logger.error(`[Risuko] CF dialog: preview failed: ${String(e)}`);
				toast.error(this.$t(cookieImportErrorKey(e), { error: String(e) }));
			} finally {
				this.importing = false;
			}
		},
		async handleApplyAndRetry() {
			if (!this.imported || !this.gid) {
				return;
			}
			const ua = (this.userAgentDraft || "").trim();
			if (!ua) {
				toast.warning(this.$t("task.task-cloudflare-ua-required"));
				return;
			}
			this.retrying = true;
			try {
				const imported = await api.importBrowserCookies({
					browser: this.selectedBrowser,
					url: this.url || `https://${this.host}`,
					persist: true,
					userAgent: ua,
				});
				this.imported = imported;
				const appStore = useAppStore();
				await api.retryWithCookies({
					gid: this.gid,
					userAgent: imported.userAgent || ua,
				});
				appStore.markCloudflareRetried(this.gid, imported.cookieNames || []);
				toast.success(
					this.$t("task.task-cookie-import-success", {
						count: imported.count,
						browser:
							this.browsers.find((b) => b.id === this.selectedBrowser)?.name ||
							this.selectedBrowser,
					}),
					{
						description: imported.cookieNames.join(", "),
					},
				);
				appStore.hideCloudflareDialog();
			} catch (e) {
				logger.error(`[Risuko] CF dialog: retry failed: ${String(e)}`);
				toast.error(this.$t(cookieImportErrorKey(e), { error: String(e) }));
			} finally {
				this.retrying = false;
			}
		},
	},
};
</script>

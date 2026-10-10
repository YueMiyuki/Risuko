<template>
  <Dialog :open="visible" @update:open="handleDialogOpenChange">
    <DialogContent
      class="about-dialog gap-0 overflow-hidden p-0 outline-none sm:max-w-[420px]"
      :show-close-button="true"
      @open-auto-focus="handleOpenAutoFocus"
    >
      <header class="about-hero">
        <div class="about-logo">
          <img src="@static/logo.svg" width="40" height="40" alt="" />
        </div>
        <div class="about-identity">
          <div class="about-title-row">
            <DialogTitle class="about-name">Risuko</DialogTitle>
            <span v-if="version" class="about-version">v{{ version }}</span>
          </div>
          <DialogDescription class="about-tagline">
            {{ $t('about.tagline') }}
          </DialogDescription>
        </div>
      </header>

      <dl class="about-specs">
        <div class="about-spec">
          <dt>{{ $t('about.engine-version') }}</dt>
          <dd class="about-spec-value">{{ engineInfo?.version || '--' }}</dd>
        </div>
        <div
          v-if="!isMas && engineInfo?.enabledFeatures?.length"
          class="about-spec about-spec--stack"
        >
          <dt>{{ $t('about.features') }}</dt>
          <dd class="about-features">
            <span
              v-for="feature in engineInfo.enabledFeatures"
              :key="feature"
              class="about-feature"
            >
              {{ feature }}
            </span>
          </dd>
        </div>
      </dl>

      <div class="about-actions">
        <button
          v-if="updaterAvailable"
          type="button"
          class="about-action about-action--accent"
          :disabled="updateBusy"
          @click="handleCheckUpdates"
        >
          <LoaderCircle v-if="updateBusy" :size="14" class="animate-spin" />
          <CircleCheck v-else-if="updateStatus === 'up-to-date'" :size="14" />
          <RefreshCw v-else :size="14" />
          <span>{{ updateLabel }}</span>
        </button>
        <button type="button" class="about-action" @click="handleCopyInfo">
          <Check v-if="copied" :size="14" />
          <Copy v-else :size="14" />
          <span>{{ copied ? $t('about.copied') : $t('about.copy-info') }}</span>
        </button>
      </div>

      <footer class="about-footer">
        <a
          target="_blank"
          rel="noopener noreferrer"
          href="https://risuko.app"
          class="about-footer-brand"
        >
          &copy; {{ year }} Risuko
        </a>
        <nav class="about-footer-links">
          <a
            target="_blank"
            rel="noopener noreferrer"
            href="https://github.com/YueMiyuki/Risuko"
          >
            GitHub
          </a>
          <a
            target="_blank"
            rel="noopener noreferrer"
            href="https://github.com/YueMiyuki/Risuko/releases"
          >
            {{ $t('about.release') }}
          </a>
          <a
            target="_blank"
            rel="noopener noreferrer"
            href="https://github.com/YueMiyuki/Risuko/issues"
          >
            {{ $t('about.support') }}
          </a>
          <a
            target="_blank"
            rel="noopener noreferrer"
            href="https://github.com/YueMiyuki/Risuko/blob/master/LICENSE"
          >
            {{ $t('about.license') }}
          </a>
        </nav>
      </footer>
    </DialogContent>
  </Dialog>
</template>

<script lang="ts">
import { Check, CircleCheck, Copy, LoaderCircle, RefreshCw } from "@lucide/vue";
import { DialogDescription, DialogTitle } from "reka-ui";
import { toast } from "vue-sonner";
import { Dialog, DialogContent } from "@/components/ui/dialog";
import is from "@/shims/platform";
import { useAppStore } from "@/store/app";
import { copyText } from "@/utils/clipboard";
import {
	checkForUpdates,
	isDesktopUpdaterAvailable,
	updaterState,
} from "@/utils/updater";

const BUSY_STATUSES = new Set([
	"checking",
	"downloading",
	"ready-to-install",
	"installing",
]);

export default {
	name: "about-panel",
	components: {
		Dialog,
		DialogContent,
		DialogDescription,
		DialogTitle,
		Check,
		CircleCheck,
		Copy,
		LoaderCircle,
		RefreshCw,
	},
	props: {
		visible: {
			type: Boolean,
			default: false,
		},
	},
	data() {
		return {
			version: "",
			year: new Date().getFullYear(),
			copied: false,
			copiedTimer: null as ReturnType<typeof setTimeout> | null,
		};
	},
	async created() {
		try {
			const { getVersion } = await import("@tauri-apps/api/app");
			this.version = await getVersion();
		} catch {
			this.version = "";
		}
	},
	beforeUnmount() {
		this.clearCopiedTimer();
	},
	computed: {
		engineInfo() {
			return useAppStore().engineInfo;
		},
		isMas() {
			return is.mas();
		},
		updaterAvailable() {
			return isDesktopUpdaterAvailable();
		},
		updateStatus() {
			return updaterState.status;
		},
		updateBusy() {
			return BUSY_STATUSES.has(this.updateStatus);
		},
		updateLabel() {
			switch (this.updateStatus) {
				case "checking":
					return this.$t("app.update-checking");
				case "downloading":
				case "ready-to-install":
				case "installing":
					return this.$t("app.update-download");
				case "up-to-date":
					return this.$t("app.update-unavailable");
				case "available":
					return this.$t("app.update-available");
				default:
					return this.$t("about.check-updates");
			}
		},
		platformName() {
			if (is.macOS()) {
				return "macOS";
			}
			if (is.windows()) {
				return "Windows";
			}
			if (is.linux()) {
				return "Linux";
			}
			if (is.android()) {
				return "Android";
			}
			return "";
		},
	},
	watch: {
		visible(val) {
			if (val) {
				this.handleOpen();
			} else {
				this.clearCopiedTimer();
				this.copied = false;
			}
		},
	},
	methods: {
		handleOpen() {
			useAppStore().fetchEngineInfo();
		},
		handleOpenAutoFocus(event: Event) {
			event.preventDefault();
			(event.target as HTMLElement | null)?.focus({ preventScroll: true });
		},
		handleDialogOpenChange(open) {
			if (!open) {
				this.handleClose();
			}
		},
		handleClose() {
			useAppStore().hideAboutPanel();
		},
		handleCheckUpdates() {
			void checkForUpdates("manual");
		},
		async handleCopyInfo() {
			const lines = [
				`Risuko ${this.version || "--"}`,
				`Engine ${this.engineInfo?.version || "--"}`,
			];
			if (this.platformName) {
				lines.push(this.platformName);
			}
			try {
				await copyText(lines.join("\n"));
			} catch {
				toast.error(this.$t("about.copy-failed"));
				return;
			}
			this.copied = true;
			this.clearCopiedTimer();
			this.copiedTimer = setTimeout(() => {
				this.copied = false;
				this.copiedTimer = null;
			}, 1800);
		},
		clearCopiedTimer() {
			if (this.copiedTimer) {
				clearTimeout(this.copiedTimer);
				this.copiedTimer = null;
			}
		},
	},
};
</script>

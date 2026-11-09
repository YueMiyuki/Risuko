<template>
  <div class="task-general" v-if="task">
    <div class="activity-stats general-stats">
      <div class="activity-stat-item" v-if="!isSeeder">
        <span class="activity-stat-label">{{ $t('task.task-download-speed') }}</span>
        <span class="activity-stat-value">{{ formatBytes(displayDownloadSpeed) }}/s</span>
      </div>
      <div class="activity-stat-item" v-if="uploads">
        <span class="activity-stat-label">{{ $t('task.task-upload-speed') }}</span>
        <span class="activity-stat-value">{{ formatBytes(Number(task.uploadSpeed || 0)) }}/s</span>
      </div>
      <div class="activity-stat-item">
        <span class="activity-stat-label">{{ $t('task.remaining-prefix') }}</span>
        <span class="activity-stat-value">{{ remainingText }}</span>
      </div>
      <div class="activity-stat-item">
        <span class="activity-stat-label">{{ $t('task.task-connections') }}</span>
        <span class="activity-stat-value">{{ task.connections }}</span>
      </div>
      <div class="activity-stat-item" v-if="isBT">
        <span class="activity-stat-label">{{ $t('task.task-num-seeders') }}</span>
        <span class="activity-stat-value">{{ task.numSeeders }}</span>
      </div>
      <div class="activity-stat-item">
        <span class="activity-stat-label">{{ $t('task.task-file-size') }}</span>
        <span class="activity-stat-value">
          {{ formatBytes(task.completedLength) }} / {{ formatBytes(task.totalLength) }}
        </span>
      </div>
    </div>
    <div class="general-card">
      <div class="general-card-row">
        <span class="general-card-label">{{ $t('task.task-gid') }}</span>
        <span class="general-card-value mono">{{ task.gid }}</span>
      </div>
      <div class="general-card-row">
        <span class="general-card-label">{{ $t('task.task-name') }}</span>
        <span class="general-card-value">{{ taskFullName }}</span>
      </div>
      <div class="general-card-row">
        <span class="general-card-label">{{ $t('task.task-dir') }}</span>
        <span class="general-card-value general-card-value--dir">
          <span class="dir-text">{{ path }}</span>
          <show-in-folder v-if="isRenderer" :path="revealPath" />
        </span>
      </div>
      <div class="general-card-row">
        <span class="general-card-label">{{ $t('task.task-status') }}</span>
        <span class="general-card-value"
          ><task-status :theme="currentTheme" :status="taskStatus"
        /></span>
      </div>
      <div v-if="task.ed2kKad" class="general-card-row">
        <span class="general-card-label">{{ $t('task.ed2k-kad-status') }}</span>
        <span class="general-card-value">
          {{ ed2kKadStatusLabel }}
          <template v-if="Number.isFinite(Number(task.ed2kKad.discoveredSources))">
            <span class="ml-2 text-muted-foreground">
              {{
                $t('task.ed2k-kad-sources', {
                  count: Number(task.ed2kKad.discoveredSources),
                })
              }}
            </span>
          </template>
          <template v-if="Number.isFinite(Number(task.ed2kKad.queriedNodes))">
            <span class="ml-2 text-muted-foreground">
              {{
                $t('task.ed2k-kad-nodes', {
                  count: Number(task.ed2kKad.queriedNodes),
                })
              }}
            </span>
          </template>
        </span>
      </div>
      <div class="general-card-row" v-if="isScheduled">
        <span class="general-card-label">{{ $t('task.schedule-start-at') }}</span>
        <span class="general-card-value">{{ scheduledStartText }}</span>
      </div>
      <div
        class="general-card-row"
        v-if="(task.errorCode && task.errorCode !== '0') || taskErrorMessage"
      >
        <span class="general-card-label">{{ $t('task.task-error-info') }}</span>
        <span class="general-card-value general-card-value--error flex flex-wrap items-center gap-2">
          <span class="min-w-0">{{ task.errorCode }} {{ taskErrorMessage }}</span>
          <Button
            v-if="errorCodeReferenceUrl"
            variant="outline"
            size="sm"
            class="ml-2 shrink-0"
            :title="$t('task.open-error-code-reference')"
            :aria-label="$t('task.open-error-code-reference')"
            @click="openErrorCodeReference"
          >
            <ExternalLink :size="13" />
            {{ $t('task.open-error-code-reference') }}
          </Button>
        </span>
      </div>
    </div>
    <div
      v-if="isScheduled && startingAfterText"
      class="mt-2 flex items-center gap-1.5 rounded-md bg-sky-500/10 px-3 py-2 text-xs font-medium text-sky-600 dark:text-sky-400"
    >
      <Clock :size="13" />
      <span>{{ $t('task.starting-after', { duration: startingAfterText }) }}</span>
    </div>
    <div v-if="isBT" class="general-section-header">
      <Magnet :size="14" /><span>{{ $t('task.task-bittorrent-info') }}</span>
    </div>
    <div v-if="isBT" class="general-card">
      <div class="general-card-row">
        <span class="general-card-label">{{ $t('task.task-info-hash') }}</span>
        <span class="general-card-value mono">
          {{ task.infoHash }}
          <button
            type="button"
            class="copy-link"
            :title="$t('task.copy-link')"
            :aria-label="$t('task.copy-link')"
            @click="handleCopyClick"
          >
            <Link :size="12" aria-hidden="true" />
          </button>
        </span>
      </div>
      <div v-if="task.infoHashV2" class="general-card-row">
        <span class="general-card-label">{{ $t('task.task-info-hash-v2') }}</span>
        <span class="general-card-value mono">{{ task.infoHashV2 }}</span>
      </div>
      <div v-if="task.metaVersion" class="general-card-row">
        <span class="general-card-label">{{ $t('task.task-meta-version') }}</span>
        <span class="general-card-value">{{ task.metaVersion }}</span>
      </div>
      <div class="general-card-row">
        <span class="general-card-label">{{ $t('task.task-piece-length') }}</span>
        <span class="general-card-value">{{ pieceLengthText }}</span>
      </div>
      <div class="general-card-row">
        <span class="general-card-label">{{ $t('task.task-num-pieces') }}</span>
        <span class="general-card-value">{{ task.numPieces }}</span>
      </div>
      <div class="general-card-row">
        <span class="general-card-label">{{ $t('task.task-bittorrent-creation-date') }}</span>
        <span class="general-card-value">{{ creationDateText }}</span>
      </div>
      <div class="general-card-row">
        <span class="general-card-label">{{ $t('task.task-bittorrent-comment') }}</span>
        <span class="general-card-value">{{ task.bittorrent.comment }}</span>
      </div>
    </div>
  </div>
</template>
<script lang="ts">
import { Clock, ExternalLink, Link, Magnet } from "@lucide/vue";
import { APP_THEME, TASK_STATUS } from "@shared/constants";
import {
	bytesToSize,
	checkTaskIsBT,
	checkTaskIsSeeder,
	checkTaskUploads,
	formatUsenetRepairFailure,
	getTaskName,
	getTaskUri,
	localeDateTimeFormat,
	timeFormat,
	timeRemaining,
} from "@shared/utils";
import ShowInFolder from "@/components/Native/ShowInFolder.vue";
import TaskStatus from "@/components/Task/TaskStatus.vue";
import { Button } from "@/components/ui/button";
import is from "@/shims/platform";
import { useAppStore } from "@/store/app";
import { usePreferenceStore } from "@/store/preference";
import { copyText } from "@/utils/clipboard";
import { getErrorCodeReferenceUrl, openExternalUrl } from "@/utils/external";
import { getTaskRevealDir, getTaskRevealPath } from "@/utils/native";

export default {
	name: "task-general",
	components: {
		[ShowInFolder.name]: ShowInFolder,
		[TaskStatus.name]: TaskStatus,
		Button,
		Magnet,
		ExternalLink,
		Link,
		Clock,
	},
	props: {
		task: {
			type: Object,
		},
	},
	data() {
		return {
			nowMs: Date.now(),
		};
	},
	computed: {
		isRenderer: () => is.renderer(),
		systemTheme() {
			return useAppStore().systemTheme;
		},
		theme() {
			return usePreferenceStore().config.theme;
		},
		currentTheme() {
			return this.theme === APP_THEME.AUTO ? this.systemTheme : this.theme;
		},
		taskFullName() {
			return getTaskName(this.task, {
				defaultName: this.$t("task.get-task-name"),
				maxLen: -1,
			});
		},
		isSeeder() {
			return checkTaskIsSeeder(this.task);
		},
		taskStatus() {
			return this.isSeeder ? TASK_STATUS.SEEDING : this.task.status;
		},
		path() {
			return getTaskRevealPath(this.task);
		},
		revealPath() {
			return is.android() ? getTaskRevealDir(this.task) : this.path;
		},
		isBT() {
			return checkTaskIsBT(this.task);
		},
		uploads() {
			return checkTaskUploads(this.task);
		},
		ed2kKadStatusLabel() {
			const state = this.task?.ed2kKad?.state;
			if (!state) {
				return "";
			}
			const key = `task.ed2k-kad-state-${state}`;
			const label = this.$t(key);
			return label === key ? state : label;
		},
		usenetRepairFailureMessage() {
			const failure = this.task?.usenetRepairFailure;
			if (!failure) {
				return "";
			}
			return formatUsenetRepairFailure(failure, (key, params) =>
				this.$t(key, params),
			);
		},
		taskErrorMessage() {
			return this.usenetRepairFailureMessage || this.task?.errorMessage || "";
		},
		errorCodeReferenceUrl(): string {
			return getErrorCodeReferenceUrl(this.task?.errorCode);
		},
		displayDownloadSpeed() {
			return Number(this.task?.downloadSpeed || 0);
		},
		remaining() {
			return timeRemaining(
				this.task?.totalLength,
				this.task?.completedLength,
				this.displayDownloadSpeed,
			);
		},
		remainingText() {
			if (this.task?.status !== TASK_STATUS.ACTIVE || this.remaining <= 0) {
				return "—";
			}
			return timeFormat(this.remaining, {
				prefix: "",
				i18n: {
					gt1d: this.$t("app.gt1d"),
					hour: this.$t("app.hour"),
					minute: this.$t("app.minute"),
					second: this.$t("app.second"),
				},
			});
		},
		pieceLengthText() {
			return bytesToSize(this.task?.pieceLength);
		},
		creationDateText() {
			const locale = usePreferenceStore().config?.locale || "en-US";
			const creationDate = this.task?.bittorrent?.creationDate;
			return localeDateTimeFormat(creationDate, locale);
		},
		isScheduled() {
			return this.task?.status === TASK_STATUS.SCHEDULED;
		},
		scheduledStartText() {
			const secs = Number(this.task?.startAt);
			const locale = usePreferenceStore().config?.locale || "en-US";
			return Number.isFinite(secs) && secs > 0
				? localeDateTimeFormat(secs, locale)
				: "";
		},
		startingAfterText() {
			const startAt = Number(this.task?.startAt);
			if (!Number.isFinite(startAt) || startAt <= 0) {
				return "";
			}
			const nowSecs = Math.floor(this.nowMs / 1000);
			const remain = startAt - nowSecs;
			if (remain <= 0) {
				return "";
			}
			return timeFormat(remain, {
				prefix: "",
				i18n: {
					gt1d: this.$t("app.gt1d"),
					hour: this.$t("app.hour"),
					minute: this.$t("app.minute"),
					second: this.$t("app.second"),
				},
			});
		},
	},
	watch: {
		isScheduled: {
			immediate: true,
			handler(scheduled: boolean) {
				if (scheduled && !this._nowTimer) {
					this._nowTimer = setInterval(() => {
						this.nowMs = Date.now();
					}, 1000);
				} else if (!scheduled && this._nowTimer) {
					clearInterval(this._nowTimer);
					this._nowTimer = null;
				}
			},
		},
	},
	methods: {
		formatBytes: bytesToSize,
		handleCopyClick() {
			const uri = getTaskUri(this.task);
			copyText(uri)
				.then(() => {
					this.$msg.success(this.$t("task.copy-link-success"));
				})
				.catch(() => {
					this.$msg.error(this.$t("task.copy-link-failed"));
				});
		},
		async openErrorCodeReference() {
			if (!(await openExternalUrl(this.errorCodeReferenceUrl))) {
				this.$msg.error(this.$t("app.open-url-failed"));
			}
		},
	},
	beforeUnmount() {
		if (this._nowTimer) {
			clearInterval(this._nowTimer);
			this._nowTimer = null;
		}
	},
};
</script>

<template>
  <div class="task-progress-info">
    <div class="task-progress-info-left">
      <div class="task-progress-percent">
        <span>{{ progressPercent }}%</span>
      </div>
      <div v-if="task.completedLength > 0 || task.totalLength > 0">
        <span>{{ formatBytes(task.completedLength, 2) }}</span>
        <span v-if="task.totalLength > 0"> / {{ formatBytes(task.totalLength, 2) }}</span>
      </div>
    </div>
    <div class="task-progress-info-right" v-show="isActive">
      <div class="task-speed-info">
        <div class="task-speed-text" v-if="isBT">
          <i><ArrowUp :size="10" /></i>
          <span>{{ formatBytes(displayUploadSpeed) }}/s</span>
        </div>
        <div class="task-speed-text" v-if="showDownloadSpeed">
          <i><ArrowDown :size="10" /></i>
          <span>{{ formatBytes(displayDownloadSpeed) }}/s</span>
        </div>
        <div class="task-speed-text hidden-sm-and-down" v-if="remaining > 0">
          <span>{{ remainingText }}</span>
        </div>
        <div class="task-speed-text hidden-sm-and-down" v-if="isBT">
          <i><Magnet :size="10" /></i>
          <span>{{ task.numSeeders }}</span>
        </div>
        <div class="task-speed-text hidden-sm-and-down">
          <i><Network :size="10" /></i>
          <span>{{ task.connections }}</span>
        </div>
      </div>
    </div>
  </div>
</template>

<script lang="ts">
import { ArrowDown, ArrowUp, Magnet, Network } from "@lucide/vue";
import { TASK_STATUS } from "@shared/constants";
import {
	bytesToSize,
	checkTaskIsBT,
	checkTaskIsSeeder,
	formatProgressPercent,
	getDisplayDownloadSpeed,
	timeFormat,
	timeRemaining,
} from "@shared/utils";

export default {
	name: "task-progress-info",
	components: {
		ArrowUp,
		ArrowDown,
		Magnet,
		Network,
	},
	props: {
		task: {
			type: Object,
		},
	},
	computed: {
		isActive() {
			return this.task.status === TASK_STATUS.ACTIVE;
		},
		isBT() {
			return checkTaskIsBT(this.task);
		},
		isSeeder() {
			return checkTaskIsSeeder(this.task);
		},
		displayUploadSpeed() {
			return Number(this.task?.uploadSpeed || 0);
		},
		displayDownloadSpeed() {
			return getDisplayDownloadSpeed(this.task);
		},
		showDownloadSpeed() {
			return !this.isSeeder;
		},
		remaining() {
			const { totalLength, completedLength } = this.task;
			return timeRemaining(
				totalLength,
				completedLength,
				this.displayDownloadSpeed,
			);
		},
		remainingText() {
			return timeFormat(this.remaining, {
				prefix: this.$t("task.remaining-prefix"),
				i18n: {
					gt1d: this.$t("app.gt1d"),
					hour: this.$t("app.hour"),
					minute: this.$t("app.minute"),
					second: this.$t("app.second"),
				},
			});
		},
		progressPercent() {
			return formatProgressPercent(
				this.task.totalLength,
				this.task.completedLength,
			);
		},
	},
	methods: {
		formatBytes(value, precision) {
			return bytesToSize(value, precision);
		},
	},
};
</script>

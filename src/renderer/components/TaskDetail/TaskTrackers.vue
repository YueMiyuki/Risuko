<template>
  <div v-if="task">
    <div class="tracker-list" v-if="announceList">
      <Textarea readonly :model-value="announceList" />
    </div>
  </div>
</template>

<script lang="ts">
import { checkTaskIsBT } from "@shared/utils";
import { convertTrackerDataToLine } from "@shared/utils/tracker";
import { Textarea } from "@/components/ui/textarea";

export default {
	name: "task-trackers",
	components: {
		Textarea,
	},
	props: {
		task: {
			type: Object,
		},
	},
	computed: {
		isBT() {
			return checkTaskIsBT(this.task);
		},
		announceList() {
			if (!this.isBT) {
				return "";
			}

			const { bittorrent } = this.task;
			const data = (bittorrent.announceList || []).flat();
			return convertTrackerDataToLine(data);
		},
	},
};
</script>

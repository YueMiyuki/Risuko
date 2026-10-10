<template>
  <aside v-if="visible && inline" class="task-detail-pane">
    <task-detail-header :task="task" @close="handleClose" />
    <task-detail-content :gid="gid" :task="task" :files="files" :peers="peers" />
  </aside>
  <Sheet v-else :open="visible" @update:open="handleSheetOpenChange">
    <SheetContent side="right" class="task-detail-drawer">
      <SheetHeader>
        <SheetTitle class="sr-only">{{ $t('task.task-detail-title') }}</SheetTitle>
        <task-detail-header :task="task" @close="handleClose" />
      </SheetHeader>
      <task-detail-content :gid="gid" :task="task" :files="files" :peers="peers" />
    </SheetContent>
  </Sheet>
</template>

<script lang="ts">
import {
	Sheet,
	SheetContent,
	SheetHeader,
	SheetTitle,
} from "@/components/ui/sheet";
import is from "@/shims/platform";
import { useTaskStore } from "@/store/task";
import TaskDetailContent from "./TaskDetailContent.vue";
import TaskDetailHeader from "./TaskDetailHeader.vue";

const INLINE_MIN_WIDTH = 1100;

export default {
	name: "task-detail",
	components: {
		[TaskDetailContent.name]: TaskDetailContent,
		[TaskDetailHeader.name]: TaskDetailHeader,
		Sheet,
		SheetContent,
		SheetHeader,
		SheetTitle,
	},
	props: {
		gid: {
			type: String,
		},
		task: {
			type: Object,
		},
		files: {
			type: Array,
			default() {
				return [];
			},
		},
		peers: {
			type: Array,
			default() {
				return [];
			},
		},
		visible: {
			type: Boolean,
			default: false,
		},
	},
	data() {
		return {
			inlineWide: false,
			inlineMql: null as MediaQueryList | null,
			closedTimer: null as ReturnType<typeof setTimeout> | null,
		};
	},
	computed: {
		inline() {
			return !is.android() && this.inlineWide;
		},
	},
	created() {
		this.inlineMql = window.matchMedia(`(min-width: ${INLINE_MIN_WIDTH}px)`);
		this.inlineWide = this.inlineMql.matches;
		this.inlineMql.addEventListener("change", this.onInlineMqlChange);
	},
	beforeUnmount() {
		if (this.closedTimer) {
			clearTimeout(this.closedTimer);
			this.closedTimer = null;
		}
		this.inlineMql?.removeEventListener("change", this.onInlineMqlChange);
	},
	watch: {
		visible(newVal, oldVal) {
			if (this.closedTimer) {
				clearTimeout(this.closedTimer);
				this.closedTimer = null;
			}
			if (oldVal && !newVal) {
				this.closedTimer = setTimeout(() => {
					this.closedTimer = null;
					this.handleClosed();
				}, 350);
			}
		},
	},
	methods: {
		onInlineMqlChange(event: MediaQueryListEvent) {
			this.inlineWide = event.matches;
		},
		handleSheetOpenChange(open) {
			if (!open) {
				this.handleClose();
			}
		},
		handleClose() {
			useTaskStore().hideTaskDetail();
		},
		handleClosed() {
			const taskStore = useTaskStore();
			taskStore.updateCurrentTaskGid("");
			taskStore.updateCurrentTaskItem(null);
		},
	},
};
</script>

<template>
  <div class="task-list-wrapper" v-if="taskList.length > 0">
    <p class="sr-only" aria-live="polite" aria-atomic="true">{{ reorderAnnouncement }}</p>
    <recycle-scroller
      v-if="useVirtualList"
      class="task-list task-list-virtual"
      :items="paginatedTaskList"
      :item-size="virtualItemSize"
      key-field="_displayKey"
    >
      <template #default="{ item, index }">
        <div
          :data-task-key="item._displayKey"
          :class="dropClass(item._displayKey)"
          @click="handleItemClick(item, $event)"
        >
          <task-item
            :task="item"
            :selected="isItemSelected(item)"
            :show-drag-handle="canDrag"
            :position="index + 1"
            :position-count="paginatedTaskList.length"
            @handle-down="onHandleDown"
            @keyboard-reorder="onKeyboardReorder"
          />
        </div>
      </template>
    </recycle-scroller>
    <drag-select
      v-else
      class="task-list"
      attribute="data-task-key"
      @change="handleDragSelectChange"
    >
      <motion-enter
        v-for="(item, index) in paginatedTaskList"
        :key="item._displayKey"
        preset="fadeInUp"
        :duration="0.4"
        :delay="getStaggerDelay(index)"
        :data-task-key="item._displayKey"
        :class="dropClass(item._displayKey)"
        @click="handleItemClick(item, $event)"
      >
        <task-item
          :task="item"
          :selected="isItemSelected(item)"
          :show-drag-handle="canDrag"
          :position="index + 1"
          :position-count="paginatedTaskList.length"
          @handle-down="onHandleDown"
          @keyboard-reorder="onKeyboardReorder"
        />
      </motion-enter>
    </drag-select>
    <footer class="task-pagination">
      <button
        class="task-pagination-btn"
        type="button"
        :disabled="currentPage <= 1"
        @click="onPrevPageClick"
      >
        {{ $t('task.pagination-prev') }}
      </button>
      <span class="task-pagination-text">{{ currentPage }} / {{ totalPages }}</span>
      <button
        class="task-pagination-btn"
        type="button"
        :disabled="currentPage >= totalPages"
        @click="onNextPageClick"
      >
        {{ $t('task.pagination-next') }}
      </button>
    </footer>
  </div>
  <motion-enter v-else preset="fade" class="no-task">
    <div class="no-task-inner">
      <span class="no-task-icon">
        <Inbox :size="22" />
      </span>
      <p>{{ $t('task.no-task') }}</p>
      <Button size="sm" variant="outline" @click="showAddTask">
        <Plus :size="14" />
        {{ $t('app.add-task') }}
      </Button>
    </div>
  </motion-enter>
</template>

<script lang="ts">
import { Inbox, Plus } from "@lucide/vue";
import { ADD_TASK_TYPE } from "@shared/constants";
import { checkTaskIsBT } from "@shared/utils";
import DragSelect from "@/components/DragSelect/Index.vue";
import { Button } from "@/components/ui/button";
import is from "@/shims/platform";
import { useAppStore } from "@/store/app";
import { usePreferenceStore } from "@/store/preference";
import { useTaskStore } from "@/store/task";
import TaskItem from "./TaskItem.vue";

const VIRTUAL_LIST_THRESHOLD = 120;

export default {
	name: "task-list",
	components: {
		[DragSelect.name]: DragSelect,
		[TaskItem.name]: TaskItem,
		Button,
		Inbox,
		Plus,
	},
	data() {
		const selectedList = [...(useTaskStore().selectedGidList || [])];
		return {
			selectedList,
			lastClickedKey: null as string | null,
			draggingKeys: [] as string[],
			dropTargetKey: null as string | null,
			dropAfter: false,
			dragScroller: null as HTMLElement | null,
			dragScrollerRect: null as DOMRect | null,
			reorderAnnouncement: "",
		};
	},
	computed: {
		taskList() {
			return useTaskStore().taskList;
		},
		paginatedTaskList() {
			return useTaskStore().paginatedTaskList;
		},
		selectedGidList() {
			return useTaskStore().selectedGidList;
		},
		currentPage() {
			return useTaskStore().currentPage;
		},
		totalPages() {
			return useTaskStore().totalPages;
		},
		virtualItemSize() {
			if (usePreferenceStore().taskListStyle === "card") {
				return is.android() ? 136 : 90;
			}
			return is.android() ? 98 : 74;
		},
		useVirtualList() {
			if (this.taskList.length < VIRTUAL_LIST_THRESHOLD) {
				return false;
			}
			return !this.paginatedTaskList.some((task) => {
				const files = Array.isArray(task.files) ? task.files : [];
				const mirrors = Array.isArray(task.mirrors) ? task.mirrors : [];
				return (checkTaskIsBT(task) && files.length > 1) || mirrors.length > 1;
			});
		},
		canDrag() {
			const store = useTaskStore();
			const listOk = ["all", "active", "waiting", "scheduled"].includes(
				store.currentList,
			);
			return (
				listOk &&
				store.sortBy === "default" &&
				!store.filterText &&
				!store.filterTag
			);
		},
	},
	mounted() {
		this._onKeyDown = this.onKeyDown.bind(this);
		window.addEventListener("keydown", this._onKeyDown);
	},
	beforeUnmount() {
		window.removeEventListener("keydown", this._onKeyDown);
		this.clearDragState();
	},
	methods: {
		clearDragState() {
			if (this._onDragMove) {
				window.removeEventListener("pointermove", this._onDragMove);
			}
			if (this._onDragUp) {
				window.removeEventListener("pointerup", this._onDragUp);
			}
			if (this._onDragCancel) {
				window.removeEventListener("pointercancel", this._onDragCancel);
			}
			document.body.classList.remove("task-dragging");
			this.draggingKeys = [];
			this.dropTargetKey = null;
			this.dragScroller = null;
			this.dragScrollerRect = null;
			this._onDragMove = null;
			this._onDragUp = null;
			this._onDragCancel = null;
		},
		showAddTask() {
			useAppStore().showAddTaskDialog(ADD_TASK_TYPE.URI);
		},
		getStaggerDelay(index: number): number {
			const MAX_DELAY = 0.6;
			return Math.min((index + 1) * 0.03, MAX_DELAY);
		},
		onPrevPageClick() {
			if (this.currentPage <= 1) {
				return;
			}
			useTaskStore().changeCurrentPage(this.currentPage - 1);
		},
		onNextPageClick() {
			if (this.currentPage >= this.totalPages) {
				return;
			}
			useTaskStore().changeCurrentPage(this.currentPage + 1);
		},
		onKeyDown(event: KeyboardEvent) {
			if ((event.metaKey || event.ctrlKey) && event.key === "a") {
				const tag = (event.target as HTMLElement)?.tagName;
				if (tag === "INPUT" || tag === "TEXTAREA") {
					return;
				}
				event.preventDefault();
				const allKeys = this.paginatedTaskList.map((t) => t._displayKey);
				this.selectedList = allKeys;
				useTaskStore().selectTasks([...allKeys]);
			}
		},
		handleItemClick(item, event) {
			const key: string = item._displayKey;
			const isMulti = event.metaKey || event.ctrlKey || is.android();
			const isShift = event.shiftKey;
			let newList: string[];

			if (isShift && this.lastClickedKey) {
				const keys = this.paginatedTaskList.map((t) => t._displayKey);
				const anchorIdx = keys.indexOf(this.lastClickedKey);
				const currentIdx = keys.indexOf(key);
				if (anchorIdx !== -1 && currentIdx !== -1) {
					const start = Math.min(anchorIdx, currentIdx);
					const end = Math.max(anchorIdx, currentIdx);
					const rangeKeys = keys.slice(start, end + 1);
					if (isMulti) {
						const set = new Set<string>(this.selectedList);
						for (const k of rangeKeys) {
							set.add(k);
						}
						newList = [...set];
					} else {
						newList = rangeKeys;
					}
				} else {
					newList = [key];
				}
			} else if (isMulti) {
				const idx = this.selectedList.indexOf(key);
				newList =
					idx === -1
						? [...this.selectedList, key]
						: this.selectedList.filter((id) => id !== key);
			} else {
				newList =
					this.selectedList.length === 1 && this.selectedList[0] === key
						? []
						: [key];
			}

			if (!isShift) {
				this.lastClickedKey = key;
			}

			this.selectedList = newList;
			useTaskStore().selectTasks([...newList]);
		},
		handleDragSelectChange(selectedList) {
			this.selectedList = selectedList;
			useTaskStore().selectTasks([...selectedList]);
		},
		isItemSelected(item): boolean {
			const key = item._displayKey;
			return this.selectedList.includes(key);
		},
		onHandleDown({ task, event }) {
			if (!this.canDrag) {
				return;
			}
			event.preventDefault?.();
			this.clearDragState();
			const key = task._displayKey || task.gid;
			const selected = new Set(this.selectedList);
			const keys = this.paginatedTaskList.map((t) => t._displayKey);
			this.draggingKeys = selected.has(key)
				? keys.filter((k) => selected.has(k))
				: [key];
			this.dragScroller = this.$el?.querySelector?.(
				".task-list",
			) as HTMLElement | null;
			this.dragScrollerRect =
				this.dragScroller?.getBoundingClientRect() ?? null;
			this._onDragMove = this.onDragMove.bind(this);
			this._onDragUp = this.onDragUp.bind(this);
			this._onDragCancel = this.onDragCancel.bind(this);
			window.addEventListener("pointermove", this._onDragMove);
			window.addEventListener("pointerup", this._onDragUp, { once: true });
			window.addEventListener("pointercancel", this._onDragCancel, {
				once: true,
			});
			document.body.classList.add("task-dragging");
		},
		onDragCancel() {
			this.clearDragState();
		},
		onDragMove(event: PointerEvent) {
			if (this.draggingKeys.length === 0) {
				return;
			}
			const scroller = this.dragScroller;
			const r = this.dragScrollerRect;
			if (scroller && r) {
				const EDGE = 44;
				if (event.clientY < r.top + EDGE) {
					scroller.scrollTop -= 14;
				} else if (event.clientY > r.bottom - EDGE) {
					scroller.scrollTop += 14;
				}
			}
			const el = document.elementFromPoint(
				event.clientX,
				event.clientY,
			) as HTMLElement | null;
			const rowEl = el?.closest?.("[data-task-key]") as HTMLElement | null;
			if (!rowEl) {
				this.dropTargetKey = null;
				return;
			}
			const targetKey = rowEl.getAttribute("data-task-key");
			if (!targetKey || this.draggingKeys.includes(targetKey)) {
				this.dropTargetKey = null;
				return;
			}
			const rect = rowEl.getBoundingClientRect();
			this.dropAfter = event.clientY > rect.top + rect.height / 2;
			this.dropTargetKey = targetKey;
		},
		async onDragUp() {
			const targetKey = this.dropTargetKey;
			const after = this.dropAfter;
			const gids = [...this.draggingKeys];
			this.clearDragState();
			if (!targetKey || gids.length === 0) {
				return;
			}
			await useTaskStore().reorderTasks(gids, targetKey, after);
		},
		async onKeyboardReorder({ task, direction }) {
			if (!this.canDrag) {
				return;
			}
			const list = this.paginatedTaskList;
			const key = task._displayKey || task.gid;
			const idx = list.findIndex((t) => (t._displayKey || t.gid) === key);
			if (idx < 0) {
				return;
			}
			let targetIdx = direction === "up" ? idx - 1 : idx + 1;
			let after = direction === "down";
			if (direction === "first") {
				targetIdx = 0;
				after = false;
			} else if (direction === "last") {
				targetIdx = list.length - 1;
				after = true;
			}
			if (targetIdx < 0 || targetIdx >= list.length || targetIdx === idx) {
				return;
			}
			const targetTask = list[targetIdx];
			await useTaskStore().reorderTasks([task.gid], targetTask.gid, after);
			this.reorderAnnouncement = `${this.$t("task.reorder-handle")}: ${
				targetIdx + 1
			} / ${list.length}`;
		},
		dropClass(key: string) {
			if (this.dropTargetKey !== key) {
				return "";
			}
			return this.dropAfter ? "drop-after" : "drop-before";
		},
	},
	watch: {
		selectedGidList(newVal: string[]) {
			this.selectedList = newVal;
		},
	},
};
</script>

<template>
  <div class="flyout-root">
    <header class="flyout-header" data-tauri-drag-region>
      <div class="flyout-title">
        <span class="flyout-title-text">{{ activeTabLabel }}</span>
        <span v-if="activeTabCount > 0" class="flyout-title-count">{{ activeTabCount }}</span>
      </div>
      <div class="flyout-summary" :aria-label="$t('app.total-transfer-speed')">
        <span class="flyout-summary-speed" :title="$t('task.task-download-speed')">
          <ArrowDown :size="13" aria-hidden="true" />
          {{ formatBytes(stat.downloadSpeed) }}/s
        </span>
        <span class="flyout-summary-speed" :title="$t('task.task-upload-speed')">
          <ArrowUp :size="13" aria-hidden="true" />
          {{ formatBytes(stat.uploadSpeed) }}/s
        </span>
      </div>
    </header>

    <nav
      class="flyout-tabs"
      role="tablist"
      :aria-label="$t('app.task-list')"
      @keydown="onTabKeydown"
    >
      <button
        v-for="tab in tabs"
        :id="`flyout-tab-${tab.key}`"
        :key="tab.key"
        ref="tabButtons"
        type="button"
        role="tab"
        class="flyout-tab"
        :class="{ active: currentList === tab.key }"
        :aria-selected="currentList === tab.key"
        :aria-label="`${tab.label}${countFor(tab.key) > 0 ? `, ${countFor(tab.key)}` : ''}`"
        :title="tab.label"
        :tabindex="currentList === tab.key ? 0 : -1"
        @click="selectTab(tab.key)"
      >
        <component :is="tab.icon" :size="16" aria-hidden="true" />
        <span v-if="countFor(tab.key) > 0" class="flyout-tab-badge">{{ countFor(tab.key) }}</span>
      </button>
    </nav>

    <div class="flyout-search">
      <Search :size="14" aria-hidden="true" />
      <input
        v-model="search"
        type="search"
        class="flyout-search-input"
        :placeholder="$t('task.filter-placeholder')"
        :aria-label="$t('task.filter-placeholder')"
      />
      <button
        v-if="search"
        type="button"
        class="flyout-search-clear"
        :aria-label="$t('app.cancel')"
        @click="search = ''"
      >
        <X :size="13" aria-hidden="true" />
      </button>
    </div>

    <main
      class="flyout-list"
      role="list"
      :aria-label="activeTabLabel"
      tabindex="0"
    >
      <Transition name="flyout-page" mode="out-in">
        <div :key="currentList" class="flyout-list-page">
          <recycle-scroller
            v-if="visibleTasks.length > 0"
            class="flyout-scroller"
            :items="visibleTasks"
            :item-size="ITEM_PITCH"
            key-field="_displayKey"
          >
            <template #default="{ item }">
              <flyout-task-item role="listitem" :task="item" />
            </template>
          </recycle-scroller>
          <div v-else class="flyout-empty">
            <Inbox :size="34" aria-hidden="true" />
            <span>{{ search ? $t('app.flyout-no-match') : $t('app.flyout-empty') }}</span>
          </div>
        </div>
      </Transition>
    </main>

    <Transition name="flyout-drop">
    <section v-if="addOpen" class="flyout-add">
      <input
        ref="addInput"
        v-model="addValue"
        type="text"
        class="flyout-add-input"
        :class="{ 'has-error': !!addError }"
        :placeholder="$t('task.uri-task-tips')"
        :aria-label="$t('app.add-task')"
        :disabled="adding"
        @keydown.enter.prevent="submitAdd"
        @keydown.esc.prevent="closeAdd"
        @input="addError = ''"
      />
      <button
        type="button"
        class="flyout-add-submit"
        :disabled="adding || !addValue.trim()"
        @click="submitAdd"
      >
        {{ $t('app.add-task') }}
      </button>
    </section>
    </Transition>
    <p v-if="addError" class="flyout-add-error" role="alert">{{ addError }}</p>

    <footer class="flyout-footer" role="toolbar" :aria-label="$t('menu.task')">
      <button type="button" class="flyout-action" :aria-label="$t('task.pause-all-task')" :title="$t('task.pause-all-task')" @click="pauseAll">
        <Pause :size="17" aria-hidden="true" />
      </button>
      <button type="button" class="flyout-action" :aria-label="$t('task.resume-all-task')" :title="$t('task.resume-all-task')" @click="resumeAll">
        <Play :size="17" aria-hidden="true" />
      </button>
      <button
        type="button"
        class="flyout-action"
        :class="{ active: addOpen }"
        :aria-label="$t('task.new-task')"
        :aria-expanded="addOpen"
        :title="$t('task.new-task')"
        @click="toggleAdd"
      >
        <Plus :size="17" aria-hidden="true" />
      </button>
      <div class="flyout-footer-spacer"></div>
      <button type="button" class="flyout-action" :aria-label="$t('app.show')" :title="$t('app.show')" @click="openMain">
        <ExternalLink :size="17" aria-hidden="true" />
      </button>
      <button type="button" class="flyout-action" :aria-label="$t('app.preferences')" :title="$t('app.preferences')" @click="openPreferences">
        <Settings :size="17" aria-hidden="true" />
      </button>
      <button type="button" class="flyout-action flyout-action-danger" :aria-label="$t('app.quit')" :title="$t('app.quit')" @click="quit">
        <Power :size="17" aria-hidden="true" />
      </button>
    </footer>
  </div>
</template>

<script lang="ts">
import {
	ArrowDown,
	ArrowUp,
	CircleCheck,
	ExternalLink,
	Inbox,
	LayoutList,
	Pause,
	Play,
	Plus,
	Power,
	Search,
	Settings,
	Square,
	X,
} from "@lucide/vue";
import { bytesToSize } from "@shared/utils";
import logger from "@shared/utils/logger";
import { invoke } from "@tauri-apps/api/core";
import { emit } from "@tauri-apps/api/event";
import { getCurrentWebviewWindow } from "@tauri-apps/api/webviewWindow";
import { type Component, markRaw } from "vue";
import { RecycleScroller } from "vue-virtual-scroller";
import "vue-virtual-scroller/dist/vue-virtual-scroller.css";
import { useAppStore } from "@/store/app";
import { useTaskStore } from "@/store/task";
import FlyoutTaskItem from "./FlyoutTaskItem.vue";

const ITEM_PITCH = 67;

interface FlyoutTab {
	key: string;
	icon: Component;
	label: string;
}

export default {
	name: "tray-flyout",
	components: {
		[FlyoutTaskItem.name as string]: FlyoutTaskItem,
		RecycleScroller,
		ArrowDown,
		ArrowUp,
		ExternalLink,
		Inbox,
		Pause,
		Play,
		Plus,
		Power,
		Search,
		Settings,
		X,
	},
	data() {
		return {
			addOpen: false,
			addValue: "",
			adding: false,
			addError: "",
			ITEM_PITCH,
		};
	},
	computed: {
		stat() {
			return useAppStore().stat;
		},
		currentList(): string {
			return useTaskStore().currentList;
		},
		visibleTasks() {
			return useTaskStore().filteredTaskList;
		},
		search: {
			get(): string {
				return useTaskStore().filterText;
			},
			set(value: string) {
				useTaskStore().setFilterText(value);
			},
		},
		tabs(): FlyoutTab[] {
			return [
				{ key: "all", icon: markRaw(LayoutList), label: this.$t("task.all") },
				{ key: "active", icon: markRaw(Play), label: this.$t("task.active") },
				{
					key: "waiting",
					icon: markRaw(Pause),
					label: this.$t("task.waiting"),
				},
				{
					key: "completed",
					icon: markRaw(CircleCheck),
					label: this.$t("task.completed"),
				},
				{
					key: "stopped",
					icon: markRaw(Square),
					label: this.$t("task.stopped"),
				},
			];
		},
		activeTabLabel(): string {
			return (
				this.tabs.find((tab: FlyoutTab) => tab.key === this.currentList)
					?.label || this.$t("task.all")
			);
		},
		activeTabCount(): number {
			return this.countFor(this.currentList);
		},
	},
	methods: {
		formatBytes(value: number): string {
			return bytesToSize(value);
		},
		countFor(key: string): number {
			return useTaskStore().taskCountMap[key] || 0;
		},
		selectTab(key: string): void {
			useTaskStore().changeCurrentList(key);
		},
		onTabKeydown(event: KeyboardEvent): void {
			const keys = this.tabs.map((tab: FlyoutTab) => tab.key);
			const current = keys.indexOf(this.currentList);
			let next = -1;
			if (event.key === "ArrowRight" || event.key === "ArrowDown") {
				next = (current + 1) % keys.length;
			} else if (event.key === "ArrowLeft" || event.key === "ArrowUp") {
				next = (current - 1 + keys.length) % keys.length;
			} else if (event.key === "Home") {
				next = 0;
			} else if (event.key === "End") {
				next = keys.length - 1;
			}
			if (next < 0) {
				return;
			}
			event.preventDefault();
			this.selectTab(keys[next]);
			this.$nextTick(() => {
				const buttons = this.$refs.tabButtons as
					| HTMLButtonElement[]
					| undefined;
				buttons?.[next]?.focus();
			});
		},
		toggleAdd(): void {
			this.addOpen = !this.addOpen;
			this.addError = "";
			if (this.addOpen) {
				this.$nextTick(() => {
					(this.$refs.addInput as HTMLInputElement | undefined)?.focus();
				});
			}
		},
		closeAdd(): void {
			this.addOpen = false;
			this.addValue = "";
			this.addError = "";
		},
		async submitAdd(): Promise<void> {
			if (this.adding) {
				return;
			}
			const uris = this.addValue
				.split(/\s+/)
				.map((value: string) => value.trim())
				.filter(Boolean);
			if (uris.length === 0) {
				return;
			}

			this.adding = true;
			this.addError = "";
			const taskStore = useTaskStore();
			const errors: string[] = [];
			try {
				for (const uri of uris) {
					try {
						await taskStore.addUri({ uris: [uri], outs: [], options: {} });
					} catch (err: unknown) {
						errors.push((err as Error)?.message || `${err}`);
						const uriPrefix = uri.slice(0, 8);
						logger.warn(
							"[Risuko] flyout add task failed for uri:",
							`${uriPrefix}...`,
						);
					}
				}
				if (errors.length > 0) {
					this.addError = errors.join("; ");
				} else {
					this.addValue = "";
					this.addOpen = false;
					taskStore.changeCurrentList("active");
				}
			} finally {
				this.adding = false;
			}
		},
		pauseAll(): void {
			useTaskStore().pauseAllTask();
		},
		resumeAll(): void {
			useTaskStore().resumeAllTask();
		},
		async hideFlyout(): Promise<void> {
			try {
				await getCurrentWebviewWindow().hide();
			} catch (err) {
				logger.warn("[Risuko] flyout hide failed:", err);
			}
		},
		async showMain(): Promise<void> {
			try {
				await invoke("show_window");
			} catch (err) {
				logger.warn("[Risuko] show_window failed:", err);
			}
		},
		async openMain(): Promise<void> {
			await this.showMain();
			await this.hideFlyout();
		},
		async openPreferences(): Promise<void> {
			try {
				await this.showMain();
				await emit("command", { command: "application:preferences" });
			} finally {
				await this.hideFlyout();
			}
		},
		async quit(): Promise<void> {
			try {
				await this.showMain();
				await emit("confirm-quit");
			} finally {
				await this.hideFlyout();
			}
		},
	},
};
</script>

<style>
.flyout-root .flyout-list {
  padding: 0;
  overflow: hidden;
}

.flyout-root .flyout-list-page {
  flex: 1 1 auto;
  min-height: 0;
}

.flyout-root .flyout-empty {
  margin: 6px 8px 8px;
}

.flyout-root .flyout-scroller {
  flex: 1 1 auto;
  min-height: 0;
  padding: 6px 8px 8px;
  box-sizing: border-box;
}

.flyout-root .flyout-scroller .flyout-task-item {
  height: 65px;
}

.flyout-root .flyout-scroller::-webkit-scrollbar {
  width: 8px;
}

.flyout-root .flyout-scroller::-webkit-scrollbar-thumb {
  background-color: var(--scrollbar-thumb);
  border-radius: 4px;
}

.flyout-root .flyout-scroller::-webkit-scrollbar-thumb:hover {
  background-color: var(--scrollbar-thumb-hover);
}
</style>

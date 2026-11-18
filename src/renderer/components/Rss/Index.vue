<template>
  <div class="main panel panel-layout panel-layout--v">
    <div class="rss-body">
      <aside class="rss-feed-column hidden-xs-only">
        <rss-feed-list @add-feed="showAddFeedDialog = true" />
      </aside>
      <div class="content panel panel-layout panel-layout--v relative">
        <motion-enter tag="header" preset="fadeInDown" class="panel-header">
        <h4 class="task-title rss-title">
          {{ currentFeed ? currentFeed.title : isDownloadedView ? $t('rss.downloaded') : $t('rss.all-items') }}
        </h4>
        <div class="task-actions">
          <button
            type="button"
            class="task-action"
            :class="{ disabled: selectedCount === 0 }"
            :disabled="selectedCount === 0"
            :title="$t('rss.delete-selected')"
            :aria-label="$t('rss.delete-selected')"
            @click="handleBatchDelete"
          >
            <Trash2 :size="14" />
          </button>
          <button
            type="button"
            class="task-action"
            :class="{ disabled: downloadableSelectedCount === 0 }"
            :disabled="downloadableSelectedCount === 0"
            :title="$t('rss.download-selected')"
            :aria-label="$t('rss.download-selected')"
            @click="handleBatchDownload"
          >
            <Download :size="14" />
          </button>
          <button
            type="button"
            class="task-action"
            :title="$t('rss.refresh')"
            :aria-label="$t('rss.refresh')"
            @click="handleRefreshAll"
          >
            <RefreshCw :size="14" :class="{ 'animate-spin': loading }" />
          </button>
          <button
            type="button"
            class="task-action"
            :title="$t('rss.auto-download-rules')"
            :aria-label="$t('rss.auto-download-rules')"
            @click="showRulesDialog = true"
          >
            <SlidersHorizontal :size="14" />
          </button>
          <button
            type="button"
            class="task-action"
            :title="$t('rss.add-feed')"
            :aria-label="$t('rss.add-feed')"
            @click="showAddFeedDialog = true"
          >
            <Plus :size="14" />
          </button>
        </div>
      </motion-enter>
      <div v-if="feeds.length > 0" class="task-toolbar">
        <div class="task-toolbar-left">
          <div class="task-filter-input">
            <Search :size="12" class="task-filter-icon" />
            <input
              type="text"
              class="task-filter-field"
              :placeholder="$t('rss.filter-placeholder')"
              :title="$t('rss.filter-placeholder')"
              :aria-label="$t('rss.filter-placeholder')"
              :value="filterText"
              @input="onFilterInput"
            />
            <button
              type="button"
              v-if="filterText"
              class="task-filter-clear"
              :title="$t('rss.clear-filter')"
              :aria-label="$t('rss.clear-filter')"
              @click="onFilterClear"
            >
              <X :size="12" />
            </button>
          </div>
          <button
            type="button"
            :class="['rss-chip', { active: unreadOnly }]"
            :aria-pressed="unreadOnly"
            :title="$t('rss.filter-unread')"
            @click="toggleUnreadOnly"
          >{{ $t('rss.filter-unread') }}</button>
          <button
            type="button"
            :class="['rss-chip', { active: matchedOnly }]"
            :aria-pressed="matchedOnly"
            :title="$t('rss.filter-matched')"
            @click="toggleMatchedOnly"
          >{{ $t('rss.filter-matched') }}</button>
          <Select :model-value="sortMode" @update:model-value="onSortChange">
            <SelectTrigger size="sm" class="rss-sort-trigger">
              <SelectValue />
            </SelectTrigger>
            <SelectContent align="end">
              <SelectItem value="newest">{{ $t('rss.sort-newest') }}</SelectItem>
              <SelectItem value="oldest">{{ $t('rss.sort-oldest') }}</SelectItem>
              <SelectItem value="size-desc">{{ $t('rss.sort-size-desc') }}</SelectItem>
              <SelectItem value="rule-match">{{ $t('rss.sort-rule-match') }}</SelectItem>
            </SelectContent>
          </Select>
          <button
            type="button"
            class="rss-chip"
            :title="$t('rss.mark-all-read')"
            :aria-label="$t('rss.mark-all-read')"
            @click="handleMarkAllRead"
          >
            <CheckCheck :size="12" />
          </button>
          <button
            type="button"
            :class="['rss-chip', { active: densityMode === 'compact' }]"
            :title="densityMode === 'compact' ? $t('rss.density-comfortable') : $t('rss.density-compact')"
            :aria-label="densityMode === 'compact' ? $t('rss.density-comfortable') : $t('rss.density-compact')"
            @click="toggleDensity"
          >
            <Rows3 :size="12" />
          </button>
        </div>
        <div class="task-toolbar-right">
          <div class="task-page-size">
            <Select :model-value="`${itemsPerPage}`" @update:model-value="onItemsPerPageChange">
              <SelectTrigger size="sm" class="task-page-size-trigger">
                <SelectValue />
              </SelectTrigger>
              <SelectContent align="end">
                <SelectItem v-for="size in itemsPerPageOptions" :key="size" :value="`${size}`">
                  {{ $t('rss.items-per-page', { count: size }) }}
                </SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div class="rss-select-all">
            <Checkbox
              :model-value="selectAllState"
              :aria-label="$t('rss.select-all')"
              @update:model-value="toggleSelectAll"
              @click.stop
            />
            <button
              type="button"
              class="rss-select-label"
              :title="$t('rss.select-all')"
              :aria-label="$t('rss.select-all')"
              @click="toggleSelectAll"
            >
              {{ selectedCount > 0 ? $t('rss.selected-count', { count: selectedCount }) : $t('rss.select-all') }}
            </button>
          </div>
        </div>
      </div>
      <main class="panel-content">
        <rss-item-list v-if="feeds.length > 0" />
        <motion-enter v-else preset="fadeInUp" class="no-task">
          <div class="rss-empty-inner">
            <Rss :size="48" class="rss-empty-icon" />
            <p>{{ $t('rss.no-feeds') }}</p>
            <Button size="sm" @click="showAddFeedDialog = true">
              <Plus :size="14" />
              {{ $t('rss.add-feed') }}
            </Button>
          </div>
        </motion-enter>
      </main>
      </div>
    </div>
    <rss-add-feed-dialog
      :visible="showAddFeedDialog"
      @close="showAddFeedDialog = false"
    />
    <rss-feed-settings-dialog
      :visible="showFeedSettingsDialog"
      :feed-id="editingFeedId"
      @close="showFeedSettingsDialog = false"
    />
    <rss-rules-dialog
      :visible="showRulesDialog"
      @close="showRulesDialog = false"
    />
  </div>
</template>

<script lang="ts">
import {
	CheckCheck,
	Download,
	Plus,
	RefreshCw,
	Rows3,
	Rss,
	Search,
	SlidersHorizontal,
	Trash2,
	X,
} from "@lucide/vue";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import {
	Select,
	SelectContent,
	SelectItem,
	SelectTrigger,
	SelectValue,
} from "@/components/ui/select";
import { useRssStore } from "@/store/rss";
import RssAddFeedDialog from "./AddFeedDialog.vue";
import RssFeedList from "./FeedList.vue";
import RssFeedSettingsDialog from "./FeedSettingsDialog.vue";
import RssItemList from "./ItemList.vue";
import RssRulesDialog from "./RulesDialog.vue";

export default {
	name: "rss-page",
	components: {
		Button,
		CheckCheck,
		Checkbox,
		Download,
		Plus,
		RefreshCw,
		Rows3,
		Rss,
		Search,
		Select,
		SelectContent,
		SelectItem,
		SelectTrigger,
		SelectValue,
		SlidersHorizontal,
		Trash2,
		X,
		"rss-feed-list": RssFeedList,
		"rss-item-list": RssItemList,
		"rss-add-feed-dialog": RssAddFeedDialog,
		"rss-feed-settings-dialog": RssFeedSettingsDialog,
		"rss-rules-dialog": RssRulesDialog,
	},
	data() {
		return {
			showAddFeedDialog: false,
			showFeedSettingsDialog: false,
			showRulesDialog: false,
			editingFeedId: "",
		};
	},
	computed: {
		feeds() {
			return useRssStore().feeds;
		},
		currentFeed() {
			return useRssStore().currentFeed;
		},
		isDownloadedView() {
			return useRssStore().currentFeedId === "__downloaded__";
		},
		loading() {
			return useRssStore().loading;
		},
		filterText() {
			return useRssStore().filterText;
		},
		itemsPerPage() {
			return useRssStore().itemsPerPage;
		},
		itemsPerPageOptions() {
			return useRssStore().itemsPerPageOptions;
		},
		selectedCount() {
			return useRssStore().selectedItemIds.length;
		},
		pageItemCount() {
			return useRssStore().paginatedItems.length;
		},
		allSelected() {
			return (
				this.pageItemCount > 0 && this.selectedCount === this.pageItemCount
			);
		},
		selectAllState(): boolean | "indeterminate" {
			if (this.allSelected) {
				return true;
			}
			if (this.selectedCount > 0) {
				return "indeterminate";
			}
			return false;
		},
		downloadableSelectedCount() {
			const store = useRssStore();
			const selected = new Set(store.selectedItemIds);
			return store.paginatedItems.filter(
				(i) =>
					selected.has(i.id) && !i.is_downloaded && (i.enclosure_url || i.link),
			).length;
		},
		unreadOnly() {
			return useRssStore().unreadOnly;
		},
		matchedOnly() {
			return useRssStore().matchedOnly;
		},
		sortMode() {
			return useRssStore().sortMode;
		},
		densityMode() {
			return useRssStore().densityMode;
		},
	},
	async created() {
		const store = useRssStore();
		store.initEventListeners();
		await store.fetchFeeds();
		await Promise.allSettled([
			...store.feeds.map((feed) => store.fetchItems(feed.id)),
			store.fetchRules(),
		]);
	},
	beforeUnmount() {
		useRssStore().cleanupEventListeners();
	},
	methods: {
		onFilterInput(event: Event) {
			useRssStore().setFilterText((event.target as HTMLInputElement).value);
		},
		onFilterClear() {
			useRssStore().setFilterText("");
		},
		onItemsPerPageChange(value: string) {
			useRssStore().setItemsPerPage(value);
		},
		toggleSelectAll() {
			const store = useRssStore();
			if (this.allSelected) {
				store.clearSelection();
			} else {
				store.selectAllItems();
			}
		},
		async handleRefreshAll() {
			await useRssStore().refreshAll();
		},
		async handleBatchDownload() {
			if (this.downloadableSelectedCount === 0) {
				return;
			}
			await useRssStore().batchDownload();
		},
		async handleBatchDelete() {
			if (this.selectedCount === 0) {
				return;
			}
			await useRssStore().batchDelete();
		},
		toggleUnreadOnly() {
			const store = useRssStore();
			store.setUnreadOnly(!store.unreadOnly);
		},
		toggleMatchedOnly() {
			const store = useRssStore();
			store.setMatchedOnly(!store.matchedOnly);
		},
		onSortChange(value: string) {
			useRssStore().setSortMode(
				value as "newest" | "oldest" | "size-desc" | "rule-match",
			);
		},
		toggleDensity() {
			const store = useRssStore();
			store.setDensityMode(
				store.densityMode === "compact" ? "comfortable" : "compact",
			);
		},
		async handleMarkAllRead() {
			const store = useRssStore();
			if (store.currentFeedId && store.currentFeedId !== "__downloaded__") {
				await store.markAllRead(store.currentFeedId);
			} else {
				await store.markItemsReadBulk(store.currentItems);
			}
		},
	},
};
</script>

<style scoped>
.rss-body {
  --rss-footer-height: 48px;
  flex: 1 1 auto;
  min-height: 0;
  display: flex;
  align-items: stretch;
  position: relative;
}

.rss-body::after {
  content: "";
  position: absolute;
  left: 0;
  right: 0;
  bottom: var(--rss-footer-height);
  height: 1px;
  background: var(--border);
  pointer-events: none;
  z-index: 2;
}

.rss-body > .content {
  flex: 1 1 auto;
  min-width: 0;
  min-height: 0;
  display: flex;
  flex-direction: column;
}

.rss-body > .content > .panel-content {
  flex: 1 1 auto;
  min-height: 0;
  height: auto;
  overflow: hidden;
}

.rss-feed-column {
  flex: 0 0 220px;
  min-width: 0;
  min-height: 0;
  display: flex;
  flex-direction: column;
  border-right: 1px solid var(--border);
}

.rss-feed-column :deep(.rss-feed-footer),
.rss-body :deep(.task-pagination) {
  height: var(--rss-footer-height);
  min-height: var(--rss-footer-height);
  max-height: var(--rss-footer-height);
  padding: 0 12px;
  border-top: none;
  display: flex;
  align-items: center;
  box-sizing: border-box;
  flex-shrink: 0;
}

.rss-body :deep(.task-pagination) {
  justify-content: center;
}

.rss-empty-inner {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 12px;
  color: var(--text-3);
}

.rss-empty-icon {
  opacity: 0.3;
}

.rss-select-all {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  cursor: pointer;
  user-select: none;
}

.rss-select-label {
  border: 0;
  padding: 0;
  background: transparent;
  font-size: 0.6875rem;
  color: var(--text-2);
  cursor: pointer;
  font: inherit;
}

.rss-select-label:hover {
  color: var(--text-1);
}

.rss-chip {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 2px 8px;
  margin-left: 4px;
  font-size: 11px;
  border-radius: 999px;
  border: 1px solid var(--border);
  background: transparent;
  color: var(--text-2);
  cursor: pointer;
  user-select: none;
  height: 22px;
}

.rss-chip:hover {
  background: var(--surface-2);
}

.rss-chip.active {
  background: var(--primary-soft);
  border-color: color-mix(in srgb, var(--primary) 40%, transparent);
  color: var(--primary);
}

.rss-sort-trigger {
  height: 22px;
  margin-left: 4px;
  font-size: 11px;
}
</style>

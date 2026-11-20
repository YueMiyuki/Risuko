<template>
  <Dialog :open="visible" @update:open="handleOpenChange">
    <DialogContent class="rss-rules-dialog sm:max-w-4xl" :show-close-button="true">
      <DialogHeader>
        <DialogTitle>{{ $t('rss.auto-download-rules') }}</DialogTitle>
      </DialogHeader>

      <div class="rss-rules-layout">
        <div class="rss-rules-master">
          <div class="rss-rules-master-toolbar">
            <Button size="sm" variant="outline" @click="createDraft">
              <Plus :size="14" />
              <span>{{ $t('rss.add-rule') }}</span>
            </Button>
          </div>
          <div v-if="rules.length === 0" class="rss-rules-empty">
            {{ $t('rss.no-rules') }}
          </div>
          <ul
            v-else
            class="rss-rules-list"
            role="listbox"
            :aria-label="$t('rss.auto-download-rules')"
          >
            <li
              v-for="rule in rules"
              :key="rule.id"
              :class="['rss-rule-row', { active: selectedId === rule.id }]"
              role="option"
              tabindex="0"
              :aria-selected="selectedId === rule.id"
              @click="selectRule(rule.id)"
              @keydown="onRuleRowKeydown($event, rule.id)"
            >
              <div class="rss-rule-row-main">
                <span class="rss-rule-row-name">{{ rule.name }}</span>
                <span class="rss-rule-row-meta">{{ ruleMeta(rule) }}</span>
              </div>
              <Switch
                :model-value="rule.is_active"
                :aria-label="$t('rss.rule-active')"
                class="rss-rule-row-switch"
                @click.stop
                @update:model-value="toggleActive(rule, $event)"
              />
            </li>
          </ul>
        </div>

        <div v-if="form" class="rss-rules-detail">
          <Tabs v-model="currentTab" class="rss-rules-tabs">
            <TabsList class="rss-rules-tabs-list">
              <TabsTrigger value="general">{{ $t('rss.rule-tab-general') }}</TabsTrigger>
              <TabsTrigger value="filters">{{ $t('rss.rule-tab-filters') }}</TabsTrigger>
              <TabsTrigger value="series">{{ $t('rss.rule-tab-series') }}</TabsTrigger>
              <TabsTrigger value="quality">{{ $t('rss.rule-tab-quality') }}</TabsTrigger>
              <TabsTrigger value="output">{{ $t('rss.rule-tab-output') }}</TabsTrigger>
              <TabsTrigger value="test">{{ $t('rss.rule-tab-test') }}</TabsTrigger>
            </TabsList>

            <TabsContent value="general" class="rss-tab-pane">
              <div class="rss-field">
                <label>{{ $t('rss.rule-name') }}</label>
                <Input v-model="form.name" />
              </div>
              <div class="rss-field">
                <label>{{ $t('rss.rule-feed-scope') }}</label>
                <div class="rss-feed-scope">
                  <label v-for="feed in feeds" :key="feed.id" class="rss-feed-checkbox">
                    <Checkbox
                      :model-value="form.feed_ids.includes(feed.id)"
                      @update:model-value="toggleFeed(feed.id, $event === true)"
                    />
                    <span>{{ feed.title || feed.url }}</span>
                  </label>
                </div>
              </div>
              <div class="rss-field-row">
                <div class="rss-field rss-field-grow">
                  <label>{{ $t('rss.rule-priority') }}</label>
                  <NumberInput v-model="form.priority" :min="0" />
                </div>
                <div class="rss-field rss-field-grow">
                  <label>{{ $t('rss.rule-mode') }}</label>
                  <select v-model="form.mode" class="rss-select">
                    <option value="any-match">{{ $t('rss.rule-mode-any') }}</option>
                    <option value="best-match">{{ $t('rss.rule-mode-best') }}</option>
                  </select>
                </div>
                <div class="rss-field rss-field-grow">
                  <label>{{ $t('rss.rule-cooldown') }}</label>
                  <NumberInput v-model="form.cooldown_secs" :min="0" />
                </div>
              </div>
              <div class="rss-field-row">
                <label class="rss-toggle">
                  <Switch v-model="form.is_active" />
                  <span>{{ $t('rss.rule-active') }}</span>
                </label>
                <label class="rss-toggle">
                  <Switch v-model="form.auto_download" />
                  <span>{{ $t('rss.rule-auto-download') }}</span>
                </label>
              </div>
            </TabsContent>

            <TabsContent value="filters" class="rss-tab-pane">
              <rss-pattern-list
                :title="$t('rss.rule-must')"
                :patterns="form.title_must"
                @update="form.title_must = $event"
              />
              <rss-pattern-list
                :title="$t('rss.rule-must-not')"
                :patterns="form.title_must_not"
                @update="form.title_must_not = $event"
              />
              <div class="rss-field-row">
                <div class="rss-field rss-field-grow">
                  <label>{{ $t('rss.rule-min-size') }}</label>
                  <NumberInput
                    :model-value="bytesToMb(form.min_size_bytes)"
                    :min="0"
                    @update:model-value="form.min_size_bytes = mbToBytes($event)"
                  />
                </div>
                <div class="rss-field rss-field-grow">
                  <label>{{ $t('rss.rule-max-size') }}</label>
                  <NumberInput
                    :model-value="bytesToMb(form.max_size_bytes)"
                    :min="0"
                    @update:model-value="form.max_size_bytes = mbToBytes($event)"
                  />
                </div>
                <div class="rss-field rss-field-grow">
                  <label>{{ $t('rss.rule-min-seeders') }}</label>
                  <NumberInput
                    :model-value="form.min_seeders ?? 0"
                    :min="0"
                    @update:model-value="form.min_seeders = $event > 0 ? $event : null"
                  />
                </div>
              </div>
            </TabsContent>

            <TabsContent value="series" class="rss-tab-pane">
              <div class="rss-field">
                <label>{{ $t('rss.rule-series-name') }}</label>
                <Input
                  :model-value="form.series_filter ?? ''"
                  @update:model-value="form.series_filter = String($event)"
                />
              </div>
              <div class="rss-field">
                <label>{{ $t('rss.rule-seasons') }}</label>
                <Input
                  :model-value="seasonsText"
                  :placeholder="$t('rss.rule-placeholder-seasons')"
                  @update:model-value="updateSeasons(String($event))"
                />
              </div>
              <div class="rss-field">
                <label>{{ $t('rss.rule-episode-selector') }}</label>
                <rss-episode-selector
                  :selector="form.episodes"
                  @update="form.episodes = $event"
                />
              </div>
            </TabsContent>

            <TabsContent value="quality" class="rss-tab-pane">
              <rss-chip-list
                :label="$t('rss.rule-quality-prefs')"
                :chips="form.quality_preferences"
                :placeholder="$t('rss.rule-placeholder-quality')"
                reorder
                @update="form.quality_preferences = $event"
              />
              <rss-chip-list
                :label="$t('rss.rule-quality-required')"
                :chips="form.required_qualities"
                :placeholder="$t('rss.rule-placeholder-quality')"
                @update="form.required_qualities = $event"
              />
              <rss-chip-list
                :label="$t('rss.rule-quality-forbidden')"
                :chips="form.forbidden_qualities"
                :placeholder="$t('rss.rule-placeholder-quality')"
                @update="form.forbidden_qualities = $event"
              />
              <label class="rss-toggle">
                <Switch v-model="form.upgrade_existing" />
                <span>{{ $t('rss.rule-upgrade-existing') }}</span>
              </label>
            </TabsContent>

            <TabsContent value="output" class="rss-tab-pane">
              <div class="rss-field">
                <label>{{ $t('rss.rule-download-dir') }}</label>
                <Input
                  :model-value="form.download_dir ?? ''"
                  @update:model-value="form.download_dir = String($event)"
                />
              </div>
              <div class="rss-field">
                <label>{{ $t('rss.rule-filename-template') }}</label>
                <Input
                  :model-value="form.filename_template ?? ''"
                  :placeholder="$t('rss.rule-placeholder-filename-template')"
                  @update:model-value="form.filename_template = String($event)"
                />
              </div>
            </TabsContent>

            <TabsContent value="test" class="rss-tab-pane">
              <div class="rss-dry-toolbar">
                <Button size="sm" variant="outline" :disabled="dryRunBusy" @click="runDryRun">
                  <RefreshCw v-if="dryRunBusy" :size="14" class="animate-spin" />
                  <Play v-else :size="14" />
                  {{ $t('rss.rule-dry-run') }}
                </Button>
                <span v-if="dryRunResults.length > 0" class="rss-dry-summary">
                  {{ $t('rss.rule-dry-run-summary', { matched: matchedCount, total: dryRunResults.length }) }}
                </span>
              </div>
              <div v-if="dryRunDone && dryRunResults.length === 0" class="rss-dry-empty">
                {{ $t('rss.rule-dry-run-empty') }}
              </div>
              <ul v-else-if="dryRunResults.length > 0" class="rss-dry-list">
                <li
                  v-for="result in dryRunResults"
                  :key="`${result.feed_id}:${result.item_id}`"
                  :class="['rss-dry-row', { matched: result.matched }]"
                >
                  <span
                    class="rss-dry-status"
                    role="img"
                    :title="result.matched ? $t('rss.rule-dry-run-matched') : $t('rss.rule-dry-run-unmatched')"
                    :aria-label="result.matched ? $t('rss.rule-dry-run-matched') : $t('rss.rule-dry-run-unmatched')"
                  >
                    <CircleCheck v-if="result.matched" :size="14" />
                    <CircleMinus v-else :size="14" />
                  </span>
                  <span class="rss-dry-title" :title="result.title">{{ result.title }}</span>
                  <span class="rss-dry-score" :title="$t('rss.rule-score')">{{ result.score }}</span>
                  <span class="rss-dry-reason" :title="result.reason">{{ result.reason }}</span>
                </li>
              </ul>
            </TabsContent>
          </Tabs>

          <div class="rss-rules-detail-footer">
            <Button
              v-if="form.id"
              size="icon-sm"
              variant="ghost"
              class="rss-rule-delete"
              :title="$t('rss.rule-delete-title')"
              :aria-label="$t('rss.rule-delete-title')"
              @click="removeCurrent"
            >
              <Trash2 :size="14" />
            </Button>
            <div class="rss-rules-footer-spacer" />
            <Button size="sm" :disabled="!isValid || saving" @click="save">
              {{ $t('rss.save') }}
            </Button>
          </div>
        </div>
        <div v-else class="rss-rules-detail rss-rules-detail-empty">
          {{ $t('rss.rule-select-hint') }}
        </div>
      </div>

      <DialogFooter>
        <Button variant="outline" @click="handleClose">{{ $t('rss.close') }}</Button>
      </DialogFooter>
    </DialogContent>
  </Dialog>
</template>

<script lang="ts">
import {
	CircleCheck,
	CircleMinus,
	Play,
	Plus,
	RefreshCw,
	Trash2,
} from "@lucide/vue";
import type { DryRunMatch, RssRule } from "@shared/types/rss";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { confirm } from "@/components/ui/confirm-dialog";
import {
	Dialog,
	DialogContent,
	DialogFooter,
	DialogHeader,
	DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import NumberInput from "@/components/ui/NumberInput.vue";
import { Switch } from "@/components/ui/switch";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useRssStore } from "@/store/rss";
import {
	bytesToMb,
	cloneRule,
	countMatched,
	emptyRule,
	hasEmptyEpisodeList,
	mbToBytes,
	parseSeasons,
	sanitizeRule,
	sortRulesByPriority,
	toggleFeedId,
} from "@/utils/rssRule";
import RssChipList from "./ChipList.vue";
import RssEpisodeSelector from "./EpisodeSelectorEditor.vue";
import RssPatternList from "./PatternList.vue";

const DRY_RUN_SAMPLE_SIZE = 50;

function errorText(error: unknown): string {
	if (typeof error === "string") {
		return error;
	}
	return error instanceof Error ? error.message : String(error);
}

export default {
	name: "rss-rules-dialog",
	components: {
		Button,
		Checkbox,
		CircleCheck,
		CircleMinus,
		Dialog,
		DialogContent,
		DialogFooter,
		DialogHeader,
		DialogTitle,
		Input,
		NumberInput,
		Play,
		Plus,
		RefreshCw,
		Switch,
		Tabs,
		TabsContent,
		TabsList,
		TabsTrigger,
		Trash2,
		"rss-chip-list": RssChipList,
		"rss-episode-selector": RssEpisodeSelector,
		"rss-pattern-list": RssPatternList,
	},
	props: {
		visible: { type: Boolean, default: false },
	},
	emits: ["close"],
	data() {
		return {
			selectedId: null as string | null,
			form: null as RssRule | null,
			seasonsText: "",
			currentTab: "general",
			dryRunResults: [] as DryRunMatch[],
			dryRunBusy: false,
			dryRunDone: false,
			saving: false,
		};
	},
	computed: {
		feeds() {
			return useRssStore().feeds;
		},
		rules(): RssRule[] {
			return sortRulesByPriority(useRssStore().rules);
		},
		isValid(): boolean {
			return (
				!!this.form &&
				this.form.name.trim().length > 0 &&
				!hasEmptyEpisodeList(this.form.episodes)
			);
		},
		matchedCount(): number {
			return countMatched(this.dryRunResults);
		},
	},
	watch: {
		async visible(open: boolean) {
			if (!open) {
				return;
			}
			try {
				await useRssStore().fetchRules();
			} catch (error) {
				this.$msg.error(errorText(error));
			}
			const first = this.rules[0];
			if (first) {
				this.selectRule(first.id);
			} else {
				this.createDraft();
			}
		},
	},
	methods: {
		bytesToMb,
		mbToBytes,
		loadForm(rule: RssRule, keepView = false) {
			this.form = cloneRule(rule);
			this.selectedId = rule.id || null;
			this.seasonsText = rule.seasons?.join(", ") ?? "";
			if (!keepView) {
				this.currentTab = "general";
				this.dryRunResults = [];
				this.dryRunDone = false;
			}
		},
		createDraft() {
			this.loadForm(emptyRule());
		},
		selectRule(id: string) {
			const found = useRssStore().rules.find((rule) => rule.id === id);
			if (found) {
				this.loadForm(found);
			}
		},
		ruleMeta(rule: RssRule): string {
			const parts = [
				rule.feed_ids.length === 0
					? this.$t("rss.all-feeds")
					: this.$t("rss.rule-feed-count", { count: rule.feed_ids.length }),
				this.$t("rss.rule-priority-value", { value: rule.priority }),
			];
			if (rule.stats.match_count > 0) {
				parts.push(
					this.$t("rss.rule-stats", {
						matched: rule.stats.match_count,
						downloaded: rule.stats.download_count,
					}),
				);
			}
			return parts.join(" · ");
		},
		onRuleRowKeydown(event: KeyboardEvent, id: string) {
			if (
				event.target === event.currentTarget &&
				(event.key === "Enter" || event.key === " ")
			) {
				event.preventDefault();
				this.selectRule(id);
			}
		},
		async toggleActive(rule: RssRule, value: boolean) {
			try {
				await useRssStore().updateRule({ ...rule, is_active: value });
			} catch (error) {
				this.$msg.error(errorText(error));
				return;
			}
			if (this.form && rule.id && this.form.id === rule.id) {
				this.form.is_active = value;
			}
		},
		toggleFeed(feedId: string, selected: boolean) {
			if (this.form) {
				this.form.feed_ids = toggleFeedId(this.form.feed_ids, feedId, selected);
			}
		},
		updateSeasons(text: string) {
			this.seasonsText = text;
			if (this.form) {
				this.form.seasons = parseSeasons(text);
			}
		},
		async save() {
			if (!this.form || !this.isValid) {
				return;
			}
			const store = useRssStore();
			const rule = sanitizeRule(this.form);
			this.saving = true;
			try {
				const saved = rule.id
					? await store.updateRule(rule)
					: await store.addRule(rule);
				this.loadForm(saved, true);
			} catch (error) {
				this.$msg.error(errorText(error));
			} finally {
				this.saving = false;
			}
		},
		async removeCurrent() {
			const target = this.form;
			if (!target?.id) {
				return;
			}
			const { confirmed } = await confirm({
				title: this.$t("rss.rule-delete-title") as string,
				message: this.$t("rss.rule-delete-confirm", {
					name: target.name,
				}) as string,
				kind: "warning",
				confirmText: this.$t("rss.delete-item") as string,
				cancelText: this.$t("rss.cancel") as string,
			});
			if (!confirmed) {
				return;
			}
			try {
				await useRssStore().removeRule(target.id);
			} catch (error) {
				this.$msg.error(errorText(error));
				return;
			}
			const first = this.rules[0];
			if (first) {
				this.selectRule(first.id);
			} else {
				this.createDraft();
			}
		},
		async runDryRun() {
			if (!this.form) {
				return;
			}
			this.dryRunBusy = true;
			try {
				this.dryRunResults = await useRssStore().dryRunRule(
					sanitizeRule(this.form),
					DRY_RUN_SAMPLE_SIZE,
				);
				this.dryRunDone = true;
			} catch (error) {
				this.$msg.error(errorText(error));
			} finally {
				this.dryRunBusy = false;
			}
		},
		handleOpenChange(open: boolean) {
			if (!open) {
				this.handleClose();
			}
		},
		handleClose() {
			this.$emit("close");
		},
	},
};
</script>

<style scoped>
.rss-rules-layout {
  display: grid;
  grid-template-columns: 240px minmax(0, 1fr);
  gap: 12px;
  height: min(520px, 70vh);
  min-height: 0;
}

.rss-rules-master,
.rss-rules-detail {
  display: flex;
  flex-direction: column;
  min-height: 0;
  border: 1px solid var(--border);
  border-radius: var(--radius);
  overflow: hidden;
}

.rss-rules-master-toolbar {
  padding: 8px;
  border-bottom: 1px solid var(--border);
}

.rss-rules-list {
  flex: 1;
  overflow-y: auto;
  list-style: none;
  margin: 0;
  padding: 4px;
}

.rss-rule-row {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px;
  border-radius: 4px;
  cursor: pointer;
  transition: background 0.1s;
}

.rss-rule-row:hover {
  background: var(--surface-2);
}

.rss-rule-row.active {
  background: var(--primary-soft);
}

.rss-rule-row-main {
  flex: 1;
  min-width: 0;
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.rss-rule-row-name {
  font-size: 13px;
  font-weight: 500;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.rss-rule-row-meta {
  font-size: 11px;
  color: var(--text-2);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.rss-rule-row-switch {
  flex-shrink: 0;
}

.rss-rules-empty {
  padding: 24px 12px;
  text-align: center;
  font-size: 12px;
  color: var(--text-2);
}

.rss-rules-detail-empty {
  align-items: center;
  justify-content: center;
  padding: 12px;
  font-size: 13px;
  text-align: center;
  color: var(--text-2);
}

.rss-rules-tabs {
  flex: 1;
  min-height: 0;
  gap: 0;
}

.rss-rules-tabs-list {
  align-self: flex-start;
  justify-content: flex-start;
  max-width: calc(100% - 16px);
  margin: 8px 8px 0;
  overflow-x: auto;
  scrollbar-width: none;
}

.rss-tab-pane {
  flex: 1;
  min-height: 0;
  display: flex;
  flex-direction: column;
  gap: 12px;
  padding: 12px;
  overflow-y: auto;
}

.rss-rules-detail-footer {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  border-top: 1px solid var(--border);
}

.rss-rules-footer-spacer {
  flex: 1;
}

.rss-rule-delete {
  color: var(--danger);
}

.rss-field {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.rss-field > label {
  font-size: 12px;
  font-weight: 500;
  color: var(--text-2);
}

.rss-field-row {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  align-items: flex-end;
}

.rss-field-grow {
  flex: 1 1 120px;
  min-width: 0;
}

.rss-feed-scope {
  display: flex;
  flex-direction: column;
  gap: 4px;
  max-height: 140px;
  overflow-y: auto;
  padding: 6px;
  border: 1px solid var(--border-strong);
  border-radius: var(--radius);
}

.rss-feed-checkbox,
.rss-toggle {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  cursor: pointer;
}

.rss-select {
  height: 36px;
  padding: 0 8px;
  font-size: 13px;
  border: 1px solid var(--border-strong);
  border-radius: var(--radius);
  background: transparent;
  color: inherit;
}

.rss-dry-toolbar {
  display: flex;
  align-items: center;
  gap: 12px;
}

.rss-dry-summary {
  font-size: 12px;
  color: var(--text-2);
}

.rss-dry-empty {
  font-size: 12px;
  color: var(--text-2);
}

.rss-dry-list {
  display: flex;
  flex-direction: column;
  gap: 2px;
  margin: 0;
  padding: 0;
  list-style: none;
}

.rss-dry-row {
  display: grid;
  grid-template-columns: 16px minmax(0, 1fr) 40px minmax(0, 120px);
  gap: 8px;
  align-items: center;
  padding: 6px 8px;
  border-radius: 4px;
  font-size: 12px;
  color: var(--text-2);
  background: var(--surface-2);
}

.rss-dry-row.matched {
  color: var(--text-1);
  background: var(--primary-soft);
}

.rss-dry-row.matched .rss-dry-status {
  color: var(--success);
}

.rss-dry-title,
.rss-dry-reason {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.rss-dry-score {
  text-align: right;
  font-variant-numeric: tabular-nums;
}

.rss-dry-reason {
  color: var(--text-2);
}

@media (max-width: 640px) {
  .rss-rules-layout {
    grid-template-columns: minmax(0, 1fr);
    grid-template-rows: minmax(0, 140px) minmax(0, 1fr);
    height: min(640px, 75vh);
  }

  .rss-dry-row {
    grid-template-columns: 16px minmax(0, 1fr) 40px;
  }

  .rss-dry-reason {
    display: none;
  }
}
</style>

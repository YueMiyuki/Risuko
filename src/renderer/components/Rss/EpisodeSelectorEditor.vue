<template>
  <div class="rss-episode-selector">
    <select
      :value="kind"
      class="rss-episode-select"
      :aria-label="$t('rss.rule-episode-selector')"
      @change="changeKind($event.target.value)"
    >
      <option value="none">{{ $t('rss.rule-episodes-all') }}</option>
      <option value="from">{{ $t('rss.rule-episodes-from') }}</option>
      <option value="range">{{ $t('rss.rule-episodes-range') }}</option>
      <option value="specific">{{ $t('rss.rule-episodes-specific') }}</option>
    </select>
    <NumberInput
      v-if="kind === 'from'"
      :model-value="from"
      :min="1"
      @update:model-value="publish({ type: 'from', start: $event })"
    />
    <template v-else-if="kind === 'range'">
      <NumberInput
        :model-value="rangeStart"
        :min="1"
        @update:model-value="publish({ type: 'range', start: $event, end: Math.max($event, rangeEnd) })"
      />
      <span class="rss-episode-dash" aria-hidden="true">-</span>
      <NumberInput
        :model-value="rangeEnd"
        :min="rangeStart"
        @update:model-value="publish({ type: 'range', start: rangeStart, end: $event })"
      />
    </template>
    <Input
      v-else-if="kind === 'specific'"
      :model-value="specific"
      :placeholder="$t('rss.rule-placeholder-episodes')"
      :aria-label="$t('rss.rule-episodes-specific')"
      @update:model-value="updateSpecific(String($event))"
    />
  </div>
</template>

<script lang="ts">
import type { EpisodeSelector } from "@shared/types/rss";
import type { PropType } from "vue";
import { Input } from "@/components/ui/input";
import NumberInput from "@/components/ui/NumberInput.vue";
import {
	defaultEpisodeSelector,
	type EpisodeKind,
	episodeKind,
	parseNumberList,
} from "@/utils/rssRule";

export default {
	name: "rss-episode-selector",
	components: { Input, NumberInput },
	props: {
		selector: {
			type: Object as PropType<EpisodeSelector | null>,
			default: null,
		},
	},
	emits: ["update"],
	data() {
		return { specific: "" };
	},
	computed: {
		kind(): EpisodeKind {
			return episodeKind(this.selector);
		},
		from(): number {
			return this.selector?.type === "from" ? this.selector.start : 1;
		},
		rangeStart(): number {
			return this.selector?.type === "range" ? this.selector.start : 1;
		},
		rangeEnd(): number {
			return this.selector?.type === "range" ? this.selector.end : 12;
		},
	},
	watch: {
		selector() {
			this.syncSpecific();
		},
	},
	created() {
		this.syncSpecific();
	},
	methods: {
		syncSpecific() {
			const values =
				this.selector?.type === "specific" ? this.selector.values : [];
			if (parseNumberList(this.specific).join(",") !== values.join(",")) {
				this.specific = values.join(", ");
			}
		},
		changeKind(kind: EpisodeKind) {
			this.publish(defaultEpisodeSelector(kind));
		},
		publish(selector: EpisodeSelector | null) {
			this.$emit("update", selector);
		},
		updateSpecific(text: string) {
			this.specific = text;
			this.publish({ type: "specific", values: parseNumberList(text) });
		},
	},
};
</script>

<style scoped>
.rss-episode-selector {
  display: flex;
  align-items: center;
  gap: 6px;
}

.rss-episode-select {
  height: 36px;
  padding: 0 8px;
  font-size: 13px;
  border: 1px solid var(--border-strong);
  border-radius: var(--radius);
  background: transparent;
  color: inherit;
}

.rss-episode-dash {
  color: var(--text-2);
}
</style>

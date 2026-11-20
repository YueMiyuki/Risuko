<template>
  <div class="rss-episode-selector">
    <div class="rss-episode-row">
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
        :aria-invalid="specificInvalid"
        :aria-describedby="specificInvalid ? 'rss-episode-specific-error' : undefined"
        @update:model-value="updateSpecific(String($event))"
      />
    </div>
    <p v-if="specificInvalid" id="rss-episode-specific-error" class="rss-episode-error">
      {{ $t('rss.rule-episodes-invalid') }}
    </p>
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
	hasEmptyEpisodeList,
	parseEpisodeList,
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
		specificInvalid(): boolean {
			return hasEmptyEpisodeList(this.selector);
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
			if (this.selector?.type !== "specific") {
				this.specific = "";
				return;
			}
			const values = this.selector.values;
			if (parseEpisodeList(this.specific).join(",") !== values.join(",")) {
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
			this.publish({ type: "specific", values: parseEpisodeList(text) });
		},
	},
};
</script>

<style scoped>
.rss-episode-selector {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.rss-episode-row {
  display: flex;
  align-items: center;
  gap: 6px;
}

.rss-episode-error {
  font-size: 12px;
  color: var(--destructive);
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

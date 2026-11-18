<template>
  <div class="rss-pattern-list">
    <div class="rss-pattern-list-header">
      <label class="rss-pattern-list-title">{{ title }}</label>
      <Button
        size="icon-sm"
        variant="ghost"
        :title="$t('rss.rule-add-pattern')"
        :aria-label="$t('rss.rule-add-pattern')"
        @click="addPattern"
      >
        <Plus :size="14" />
      </Button>
    </div>
    <div v-for="(pattern, idx) in patterns" :key="idx" class="rss-pattern-row">
      <select
        :value="pattern.kind"
        class="rss-pattern-select"
        :aria-label="$t('rss.rule-pattern-kind')"
        @change="updateKind(idx, $event.target.value)"
      >
        <option value="contains">{{ $t('rss.rule-pattern-contains') }}</option>
        <option value="glob">{{ $t('rss.rule-pattern-glob') }}</option>
        <option value="regex">{{ $t('rss.rule-pattern-regex') }}</option>
      </select>
      <Input
        :model-value="pattern.value"
        :aria-label="$t('rss.rule-pattern-value')"
        @update:model-value="updateValue(idx, String($event))"
      />
      <label class="rss-pattern-case" :title="$t('rss.rule-pattern-case')">
        <Checkbox
          :model-value="pattern.case_sensitive"
          :aria-label="$t('rss.rule-pattern-case')"
          @update:model-value="updateCase(idx, $event === true)"
        />
        <span aria-hidden="true">Aa</span>
      </label>
      <Button
        size="icon-sm"
        variant="ghost"
        :title="$t('rss.rule-pattern-remove')"
        :aria-label="$t('rss.rule-pattern-remove')"
        @click="removePattern(idx)"
      >
        <X :size="14" />
      </Button>
    </div>
  </div>
</template>

<script lang="ts">
import { Plus, X } from "@lucide/vue";
import type { Pattern, PatternKind } from "@shared/types/rss";
import type { PropType } from "vue";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";

export default {
	name: "rss-pattern-list",
	components: { Button, Checkbox, Input, Plus, X },
	props: {
		title: { type: String, required: true },
		patterns: { type: Array as PropType<Pattern[]>, required: true },
	},
	emits: ["update"],
	methods: {
		addPattern() {
			this.$emit("update", [
				...this.patterns,
				{ value: "", kind: "contains", case_sensitive: false },
			]);
		},
		removePattern(idx: number) {
			this.$emit(
				"update",
				this.patterns.filter((_, i) => i !== idx),
			);
		},
		patch(idx: number, change: Partial<Pattern>) {
			this.$emit(
				"update",
				this.patterns.map((pattern, i) =>
					i === idx ? { ...pattern, ...change } : pattern,
				),
			);
		},
		updateValue(idx: number, value: string) {
			this.patch(idx, { value });
		},
		updateKind(idx: number, kind: PatternKind) {
			this.patch(idx, { kind });
		},
		updateCase(idx: number, caseSensitive: boolean) {
			this.patch(idx, { case_sensitive: caseSensitive });
		},
	},
};
</script>

<style scoped>
.rss-pattern-list {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.rss-pattern-list-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.rss-pattern-list-title {
  font-size: 12px;
  font-weight: 500;
  color: var(--text-2);
}

.rss-pattern-row {
  display: grid;
  grid-template-columns: 96px minmax(0, 1fr) auto auto;
  gap: 6px;
  align-items: center;
}

.rss-pattern-select {
  height: 36px;
  padding: 0 6px;
  font-size: 12px;
  border: 1px solid var(--border-strong);
  border-radius: var(--radius);
  background: transparent;
  color: inherit;
}

.rss-pattern-case {
  display: flex;
  align-items: center;
  gap: 4px;
  font-size: 11px;
  color: var(--text-2);
  cursor: pointer;
}
</style>

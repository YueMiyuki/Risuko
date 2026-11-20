<template>
  <div class="rss-tag-list">
    <label class="rss-tag-list-label">{{ label }}</label>
    <div class="rss-tag-list-row">
      <span v-for="(chip, idx) in chips" :key="chip" class="rss-tag">
        <span>{{ chip }}</span>
        <button
          v-if="reorder && idx > 0"
          type="button"
          class="rss-tag-btn"
          :title="$t('rss.rule-chip-earlier')"
          :aria-label="$t('rss.rule-chip-earlier')"
          @click="move(idx, idx - 1)"
        >
          <ArrowLeft :size="10" />
        </button>
        <button
          v-if="reorder && idx < chips.length - 1"
          type="button"
          class="rss-tag-btn"
          :title="$t('rss.rule-chip-later')"
          :aria-label="$t('rss.rule-chip-later')"
          @click="move(idx, idx + 1)"
        >
          <ArrowRight :size="10" />
        </button>
        <button
          type="button"
          class="rss-tag-btn"
          :title="$t('rss.rule-chip-remove')"
          :aria-label="$t('rss.rule-chip-remove')"
          @click="remove(idx)"
        >
          <X :size="10" />
        </button>
      </span>
      <input
        v-model="draft"
        :placeholder="placeholder"
        :aria-label="label"
        class="rss-tag-input"
        @keydown.enter="onEnter"
        @keydown.backspace="onBackspace"
        @blur="commit"
      />
    </div>
  </div>
</template>

<script lang="ts">
import { ArrowLeft, ArrowRight, X } from "@lucide/vue";
import type { PropType } from "vue";

function isComposing(event: KeyboardEvent): boolean {
	return event.isComposing || event.keyCode === 229;
}

export default {
	name: "rss-chip-list",
	components: { ArrowLeft, ArrowRight, X },
	props: {
		label: { type: String, required: true },
		chips: { type: Array as PropType<string[]>, required: true },
		placeholder: { type: String, default: "" },
		reorder: { type: Boolean, default: false },
	},
	emits: ["update"],
	data() {
		return { draft: "" };
	},
	methods: {
		commit() {
			const value = this.draft.trim();
			this.draft = "";
			if (value && !this.chips.includes(value)) {
				this.$emit("update", [...this.chips, value]);
			}
		},
		remove(idx: number) {
			this.$emit(
				"update",
				this.chips.filter((_, i) => i !== idx),
			);
		},
		move(from: number, to: number) {
			const next = this.chips.slice();
			const [moved] = next.splice(from, 1);
			if (moved !== undefined) {
				next.splice(to, 0, moved);
			}
			this.$emit("update", next);
		},
		onEnter(event: KeyboardEvent) {
			if (isComposing(event)) {
				return;
			}
			event.preventDefault();
			this.commit();
		},
		onBackspace(event: KeyboardEvent) {
			if (isComposing(event)) {
				return;
			}
			if (this.draft === "" && this.chips.length > 0) {
				event.preventDefault();
				this.$emit("update", this.chips.slice(0, -1));
			}
		},
	},
};
</script>

<style scoped>
.rss-tag-list {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.rss-tag-list-label {
  font-size: 12px;
  font-weight: 500;
  color: var(--text-2);
}

.rss-tag-list-row {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
  padding: 6px;
  border: 1px solid var(--border-strong);
  border-radius: var(--radius);
  min-height: 36px;
}

.rss-tag-list-row:focus-within {
  border-color: var(--ring);
  box-shadow: 0 0 0 3px color-mix(in srgb, var(--ring) 50%, transparent);
}

.rss-tag {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 2px 4px 2px 8px;
  background: var(--surface-2);
  border-radius: 999px;
  font-size: 12px;
}

.rss-tag-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 2px;
  border: none;
  border-radius: 50%;
  background: transparent;
  color: var(--text-2);
  cursor: pointer;
}

.rss-tag-btn:hover {
  background: var(--border);
  color: var(--text-1);
}

.rss-tag-input {
  flex: 1;
  min-width: 100px;
  border: none;
  outline: none;
  background: transparent;
  font-size: 13px;
  color: inherit;
}
</style>

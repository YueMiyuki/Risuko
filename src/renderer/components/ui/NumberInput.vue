<template>
  <div class="number-input-wrap">
    <input
      :id="id"
      type="number"
      class="input-number"
      :value="modelValue"
      :min="min"
      :max="max"
      :step="step"
      @input="onInput"
      @blur="onBlur"
    />
    <div class="number-btns">
      <button
        type="button"
        class="number-btn"
        tabindex="-1"
        :title="$t('app.increase-value')"
        :aria-label="$t('app.increase-value')"
        @click="increment"
        @mousedown.prevent
      >
        <svg
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          stroke-linecap="round"
          stroke-linejoin="round"
        >
          <path d="m18 15-6-6-6 6" />
        </svg>
      </button>
      <button
        type="button"
        class="number-btn"
        tabindex="-1"
        :title="$t('app.decrease-value')"
        :aria-label="$t('app.decrease-value')"
        @click="decrement"
        @mousedown.prevent
      >
        <svg
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          stroke-linecap="round"
          stroke-linejoin="round"
        >
          <path d="m6 9 6 6 6-6" />
        </svg>
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
const props = withDefaults(
	defineProps<{
		modelValue?: number;
		min?: number;
		max?: number;
		step?: number;
		id?: string;
	}>(),
	{
		modelValue: 0,
		min: 0,
		max: Infinity,
		step: 1,
	},
);

const emit = defineEmits<(e: "update:modelValue", value: number) => void>();

function clamp(val: number): number {
	return Math.min(props.max, Math.max(props.min, val));
}

function onInput(e: Event) {
	const raw = (e.target as HTMLInputElement).value;
	if (raw === "") {
		return;
	}
	const num = parseFloat(raw);
	if (!Number.isNaN(num) && num >= props.min) {
		emit("update:modelValue", clamp(num));
	}
}

function onBlur(e: Event) {
	const input = e.target as HTMLInputElement;
	const num = parseFloat(input.value);
	const next = Number.isNaN(num) ? props.min : clamp(num);
	input.value = String(next);
	emit("update:modelValue", next);
}

function increment() {
	emit("update:modelValue", clamp((props.modelValue ?? 0) + props.step));
}

function decrement() {
	emit("update:modelValue", clamp((props.modelValue ?? 0) - props.step));
}
</script>

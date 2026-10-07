<script setup lang="ts">
import { computed, useAttrs, type HTMLAttributes } from "vue";
import { Checkbox } from "../checkbox";

defineOptions({ inheritAttrs: false });

const attrs = useAttrs();

const props = withDefaults(
	defineProps<{
		modelValue?: boolean;
		disabled?: boolean;
		class?: HTMLAttributes["class"];
	}>(),
	{
		modelValue: false,
		disabled: false,
	},
);

const emit = defineEmits<{
	"update:modelValue": [value: boolean];
	change: [value: boolean];
}>();

const checked = computed({
	get: () => props.modelValue,
	set: (val: boolean) => {
		emit("update:modelValue", val);
		emit("change", val);
	},
});

function onLabelClick(event: MouseEvent) {
	const target = event.target;
	if (target instanceof Element && target.closest("[data-slot='checkbox']")) {
		return;
	}
	// Reka-ui renders a button, so a native <label> click would toggle twice
	event.preventDefault();
	if (props.disabled) {
		return;
	}
	checked.value = !checked.value;
}
</script>

<template>
  <label
    class="ui-checkbox"
    :class="[props.class, { 'is-disabled': disabled }]"
    @click="onLabelClick"
  >
    <Checkbox
      v-bind="attrs"
      :model-value="checked"
      :disabled="disabled"
      @update:model-value="
        (val: boolean) => {
          checked = val;
        }
      "
    />
    <span v-if="$slots.default" class="ui-checkbox__label">
      <slot />
    </span>
  </label>
</template>

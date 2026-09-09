<script lang="ts" setup>
import {
	CircleCheckIcon,
	InfoIcon,
	Loader2Icon,
	OctagonXIcon,
	TriangleAlertIcon,
	XIcon,
} from "@lucide/vue";
import type { ToasterProps } from "vue-sonner";
import { Toaster as Sonner } from "vue-sonner";
import { cn } from "@/lib/utils";

const props = defineProps<ToasterProps>();
</script>

<template>
  <Sonner
    :class="cn('toaster group', props.class)"
    :style="{
      '--normal-bg': 'var(--popover)',
      '--normal-text': 'var(--popover-foreground)',
      '--normal-border': 'var(--border)',
      '--border-radius': 'var(--radius)',
    }"
    v-bind="props"
  >
    <template #success-icon>
      <CircleCheckIcon class="size-4" />
    </template>
    <template #info-icon>
      <InfoIcon class="size-4" />
    </template>
    <template #warning-icon>
      <TriangleAlertIcon class="size-4" />
    </template>
    <template #error-icon>
      <OctagonXIcon class="size-4" />
    </template>
    <template #loading-icon>
      <div>
        <Loader2Icon class="size-4 animate-spin" />
      </div>
    </template>
    <template #close-icon>
      <XIcon class="size-4" />
    </template>
  </Sonner>
</template>

<style>
[data-sonner-toast].error-reference-toast[data-styled="true"] {
	display: grid;
	grid-template-columns: auto minmax(0, 1fr);
	align-items: center;
}

[data-sonner-toast].error-reference-toast[data-styled="true"] [data-content] {
	grid-column: 2;
	grid-row: 1;
	min-width: 0;
}

[data-sonner-toast].error-reference-toast[data-styled="true"] [data-action] {
	grid-column: 2;
	grid-row: 2;
	margin: 8px 0 0;
	justify-self: start;
}
</style>

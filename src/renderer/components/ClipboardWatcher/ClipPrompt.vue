<template>
  <div class="clip-prompt">
    <div ref="card" class="clip-prompt-card" data-tauri-drag-region>
      <div class="clip-prompt-title">{{ $t('task.clipboard-download-prompt') }}</div>
      <div class="clip-prompt-uri" :title="uri">{{ uri }}</div>
      <div class="clip-prompt-actions">
        <button type="button" class="clip-btn clip-btn-ghost" @click="ignore">
          {{ $t('task.clipboard-download-ignore') }}
        </button>
        <button type="button" class="clip-btn clip-btn-primary" @click="download">
          {{ $t('task.clipboard-download-accept') }}
        </button>
      </div>
    </div>
  </div>
</template>

<script>
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import i18next from "i18next";

export default {
	name: "clip-prompt",
	data() {
		return { uri: "", unlisten: null };
	},
	async mounted() {
		await this.refresh();
		this.unlisten = await listen("clip-prompt:show", () => {
			this.refresh();
			this.playEntrance();
		});
		this.onLang = () => this.$forceUpdate();
		i18next.on("languageChanged", this.onLang);
		window.addEventListener("keydown", this.onKey);
		this.playEntrance();
		invoke("panel_ready").catch(() => {});
	},
	beforeUnmount() {
		this.unlisten?.();
		i18next.off("languageChanged", this.onLang);
		window.removeEventListener("keydown", this.onKey);
	},
	methods: {
		async refresh() {
			try {
				this.uri = (await invoke("get_clip_prompt_uri")) || "";
			} catch {
				this.uri = "";
			}
		},
		onKey(e) {
			if (e.key === "Escape") {
				this.ignore();
			} else if (e.key === "Enter") {
				if (document.activeElement?.tagName === "BUTTON") {
					return;
				}
				this.download();
			}
		},
		playEntrance() {
			if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) {
				return;
			}
			this.$refs.card?.animate(
				[
					{ opacity: 0, transform: "translateY(-6px) scale(0.98)" },
					{ opacity: 1, transform: "none" },
				],
				{ duration: 160, easing: "cubic-bezier(0.25, 1, 0.5, 1)" },
			);
		},
		download() {
			invoke("clip_prompt_accept").catch(() => {});
		},
		ignore() {
			invoke("clip_prompt_dismiss").catch(() => {});
		},
	},
};
</script>

<style scoped>
.clip-prompt {
	box-sizing: border-box;
	width: 100%;
	height: 100%;
	padding: 8px;
	background: transparent;
}

.clip-prompt-card {
	box-sizing: border-box;
	display: flex;
	flex-direction: column;
	gap: 8px;
	height: 100%;
	padding: 16px;
	border-radius: var(--radius);
	border: 1px solid var(--border);
	background: var(--bg);
	color: var(--text-1);
	box-shadow:
		0 12px 32px -8px rgba(0, 0, 0, 0.32),
		0 4px 12px -4px rgba(0, 0, 0, 0.18);
	font-family: inherit;
	user-select: none;
}

.clip-prompt-title {
	font-size: 15px;
	font-weight: 600;
	color: var(--text-1);
}

.clip-prompt-uri {
	flex: 1;
	overflow: hidden;
	text-overflow: ellipsis;
	white-space: nowrap;
	font-size: 12px;
	color: var(--text-2);
}

.clip-prompt-actions {
	display: flex;
	justify-content: flex-end;
	gap: 8px;
	margin-top: auto;
}

.clip-btn {
	appearance: none;
	border: none;
	border-radius: calc(var(--radius) - 2px);
	padding: 7px 16px;
	font-size: 13px;
	font-weight: 500;
	cursor: pointer;
	transition:
		background 0.12s ease,
		filter 0.12s ease;
}

.clip-btn-ghost {
	background: var(--surface-2);
	color: var(--text-1);
}

.clip-btn-ghost:hover {
	background: color-mix(in srgb, var(--surface-2) 88%, var(--text-1));
}

.clip-btn-primary {
	background: var(--primary);
	color: var(--primary-foreground);
}

.clip-btn-primary:hover {
	background: var(--primary-hover);
}

.clip-btn-primary:active {
	background: var(--primary-active);
}
</style>

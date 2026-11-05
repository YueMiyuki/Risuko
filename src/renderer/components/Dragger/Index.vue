<template>
  <div style="display: none"></div>
</template>

<script lang="ts">
import { ADD_TASK_TYPE } from "@shared/constants";
import { useAppStore } from "@/store/app";
import { batchItemForFilePath } from "@/store/batchQueue";

export default {
	name: "drop-zone",
	data() {
		return {
			unlistenWindowDragDrop: null as null | (() => void),
			dragPreviewOpened: false,
		};
	},
	mounted() {
		this.preventDefault = (ev) => ev.preventDefault();
		this.injectTorrentPaths = async (paths: string[] = []) => {
			if (useAppStore().createTorrentVisible) {
				return;
			}
			const items = paths
				.map((path) => {
					const segs = `${path}`.split(/[/\\]/);
					return batchItemForFilePath(path, segs[segs.length - 1] || undefined);
				})
				.filter(Boolean);
			if (!items.length) {
				this.$msg.error(this.$t("task.new-task-file-required"));
				return;
			}

			useAppStore().showAddTaskDialog(ADD_TASK_TYPE.TORRENT);
			useAppStore().enqueueBatchItems(items);
		};
		this.handleFileList = (files) => {
			if (useAppStore().createTorrentVisible) {
				return;
			}
			const items = (files || [])
				.map((item) => batchItemForFilePath(item.path ?? item.name, item.name))
				.filter(Boolean);
			if (!items.length) {
				this.$msg.error(this.$t("task.new-task-file-required"));
				return;
			}
			useAppStore().showAddTaskDialog(ADD_TASK_TYPE.TORRENT);
			useAppStore().enqueueBatchItems(items);
		};
		let count = 0;
		this.onDragEnter = (_ev) => {
			const appStore = useAppStore();
			if (count === 0 && !appStore.createTorrentVisible) {
				this.dragPreviewOpened = !appStore.addTaskVisible;
				appStore.showAddTaskDialog(ADD_TASK_TYPE.TORRENT);
			}
			count++;
		};

		this.onDragLeave = (_ev) => {
			count = Math.max(0, count - 1);
			if (count === 0 && this.dragPreviewOpened) {
				useAppStore().hideAddTaskDialog();
				this.dragPreviewOpened = false;
			}
		};

		this.onDrop = (ev) => {
			count = 0;
			this.dragPreviewOpened = false;

			this.handleFileList([...(ev.dataTransfer?.files || [])]);
		};

		document.addEventListener("dragover", this.preventDefault);
		document.body.addEventListener("dragenter", this.onDragEnter);
		document.body.addEventListener("dragleave", this.onDragLeave);
		document.body.addEventListener("drop", this.onDrop);

		import("@tauri-apps/api/webviewWindow")
			.then(({ getCurrentWebviewWindow }) => {
				const webview = getCurrentWebviewWindow();
				webview
					.onDragDropEvent((event) => {
						if (event?.payload?.type !== "drop") {
							return;
						}
						count = 0;
						this.dragPreviewOpened = false;
						const paths = event?.payload?.paths || [];
						if (!Array.isArray(paths) || paths.length === 0) {
							return;
						}
						this.injectTorrentPaths(paths);
					})
					.then((unlisten) => {
						this.unlistenWindowDragDrop = unlisten;
					})
					.catch(() => {});
			})
			.catch(() => {});
	},
	beforeUnmount() {
		document.removeEventListener("dragover", this.preventDefault);
		document.body.removeEventListener("dragenter", this.onDragEnter);
		document.body.removeEventListener("dragleave", this.onDragLeave);
		document.body.removeEventListener("drop", this.onDrop);
		this.unlistenWindowDragDrop?.();
		this.unlistenWindowDragDrop = null;
	},
};
</script>

<template>
  <div class="content panel panel-layout panel-layout--v stats-page">
    <main class="panel-content stats-main">
      <div class="stats-toolbar">
        <div class="stats-title">
          <h2>Stats</h2>
          <span>{{ totalReceivedText }}</span>
        </div>

        <div class="stats-range">
          <div class="stats-picker">
            <span>Start</span>
            <DateTimePicker v-model="startAt" allow-past placeholder="Start" />
          </div>
          <div class="stats-picker">
            <span>End</span>
            <DateTimePicker v-model="endAt" allow-past placeholder="End" />
          </div>
          <div class="stats-presets">
            <button
              v-for="preset in presets"
              :key="preset.label"
              type="button"
              class="stats-chip"
              @click="applyPreset(preset.seconds)"
            >
              {{ preset.label }}
            </button>
          </div>
        </div>
      </div>

      <p v-if="error" class="stats-error">{{ error }}</p>

      <section class="stats-section">
        <div class="stats-section-head">
          <div>
            <h3>Received</h3>
            <p>{{ monthCount }} months</p>
          </div>
          <button
            v-if="extraProtocolCount > 0"
            type="button"
            class="stats-link"
            @click="expandedProtocols = !expandedProtocols"
          >
            {{ expandedProtocols ? 'Collapse' : `+ Expand ${extraProtocolCount}` }}
          </button>
        </div>

        <div v-if="loading" class="stats-empty">Loading...</div>
        <div v-else-if="!monthlyRows.length" class="stats-empty">No stats in this range</div>
        <div v-else class="monthly-layout">
          <div class="monthly-chart" role="img" aria-label="Monthly received bytes by protocol">
            <div
              v-for="month in monthlyRows"
              :key="month.month"
              class="month-row"
            >
              <span class="month-label">{{ month.month }}</span>
              <div class="month-bar">
                <span
                  v-for="segment in monthlySegments(month)"
                  :key="segment.key"
                  class="month-segment"
                  :style="{ width: segment.width, backgroundColor: segment.color }"
                  :title="`${segment.label}: ${formatBytes(segment.value)}`"
                />
              </div>
              <span class="month-total">{{ formatBytes(month.total) }}</span>
            </div>
          </div>

          <div class="protocol-legend">
            <span
              v-for="protocol in legendProtocols"
              :key="protocol"
              class="legend-item"
            >
              <i :style="{ backgroundColor: colorForProtocol(protocol) }" />
              {{ protocolLabel(protocol) }}
            </span>
          </div>

          <table class="stats-table">
            <thead>
              <tr>
                <th>Month</th>
                <th>Protocol</th>
                <th>Received</th>
              </tr>
            </thead>
            <tbody>
              <tr
                v-for="row in tableRows"
                :key="`${row.month}:${row.protocol}`"
              >
                <td>{{ row.month }}</td>
                <td>{{ protocolLabel(row.protocol) }}</td>
                <td>{{ formatBytes(row.receivedBytes) }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>

      <section class="stats-section">
        <div class="stats-section-head stats-section-head--controls">
          <div>
            <h3>Speed</h3>
            <p>{{ speedPointCount }} minute buckets</p>
          </div>
          <div class="stats-controls">
            <div class="stats-segmented">
              <button
                type="button"
                :class="{ active: splitMode === 'overall' }"
                @click="splitMode = 'overall'"
              >
                Overall
              </button>
              <button
                type="button"
                :class="{ active: splitMode === 'protocol' }"
                @click="splitMode = 'protocol'"
              >
                Protocol
              </button>
            </div>
            <div class="stats-segmented">
              <button
                type="button"
                :class="{ active: seriesMode === 'download' }"
                @click="seriesMode = 'download'"
              >
                Down
              </button>
              <button
                type="button"
                :class="{ active: seriesMode === 'upload' }"
                @click="seriesMode = 'upload'"
              >
                Up
              </button>
              <button
                type="button"
                :class="{ active: seriesMode === 'both' }"
                @click="seriesMode = 'both'"
              >
                Both
              </button>
            </div>
            <button
              type="button"
              class="stats-chip stats-toggle"
              :class="{ active: showTickLabels }"
              @click="showTickLabels = !showTickLabels"
            >
              Labels
            </button>
          </div>
        </div>

        <div v-if="!speedLines.length" class="stats-empty">No speed samples in this range</div>
        <div v-else class="speed-chart-wrap">
          <div
            class="speed-chart-stage"
            @mousemove="onSpeedChartMove"
            @mouseleave="clearSpeedHover"
          >
            <svg
              class="speed-chart"
              :viewBox="`0 0 ${chartWidth} ${chartHeight}`"
              preserveAspectRatio="none"
            >
              <line
                v-for="tick in speedTicks"
                :key="tick.y"
                :x1="CHART_PAD_X"
                :x2="chartWidth - CHART_PAD_RIGHT"
                :y1="tick.y"
                :y2="tick.y"
                class="speed-grid"
              />
              <g v-if="showTickLabels">
                <text
                  v-for="tick in speedTicks"
                  :key="`y-label-${tick.y}`"
                  class="speed-tick-label speed-tick-label--y"
                  :x="CHART_PAD_X - 8"
                  :y="tick.y + 4"
                >{{ tick.label }}</text>
                <text
                  v-for="tick in speedXTicks"
                  :key="`x-label-${tick.minute}`"
                  class="speed-tick-label speed-tick-label--x"
                  :x="tick.x"
                  :y="CHART_X_TICK_Y"
                  :transform="`rotate(-45 ${tick.x} ${CHART_X_TICK_Y})`"
                >{{ tick.label }}</text>
              </g>
              <text
                class="speed-axis-label speed-axis-label--y"
                :transform="`translate(18 ${CHART_PAD_TOP + CHART_PLOT_HEIGHT / 2}) rotate(-90)`"
              >Speed</text>
              <text
                class="speed-axis-label speed-axis-label--x"
                :x="chartWidth / 2"
                :y="chartHeight - 7"
              >Time</text>
              <path
                v-for="line in speedLines"
                :key="line.key"
                :d="line.path"
                class="speed-line"
                :stroke="line.color"
                :stroke-dasharray="line.metric === 'upload' ? '5 4' : undefined"
              />
              <line
                v-if="speedHover"
                :x1="speedHover.x"
                :x2="speedHover.x"
                :y1="CHART_PAD_TOP"
                :y2="CHART_PLOT_BOTTOM"
                class="speed-hover-guide"
              />
              <circle
                v-for="point in speedHover?.points || []"
                :key="`hover-${point.key}`"
                :cx="speedHover?.x"
                :cy="point.y"
                r="3.5"
                :fill="point.color"
                class="speed-hover-dot"
              />
            </svg>
            <div
              v-if="speedHover"
              class="speed-tooltip"
              :style="speedTooltipStyle"
            >
              <div class="speed-tooltip-time">{{ speedHover.label }}</div>
              <div
                v-for="point in speedHover.points"
                :key="point.key"
                class="speed-tooltip-row"
              >
                <i :style="{ backgroundColor: point.color }" />
                <span>{{ point.label }}</span>
                <strong>{{ formatBytes(point.value) }}/s</strong>
              </div>
            </div>
          </div>
          <div class="speed-legend">
            <span
              v-for="line in speedLines"
              :key="line.key"
              class="legend-item"
            >
              <i
                :style="{
                  backgroundColor: line.color,
                  borderRadius: line.metric === 'upload' ? '0' : '50%',
                }"
              />
              {{ line.label }}
            </span>
          </div>
          <div v-if="showTickLabels" class="speed-scale">
            <span>0/s</span>
            <span>{{ formatBytes(speedMax) }}/s</span>
          </div>
        </div>
      </section>
    </main>
  </div>
</template>

<script setup lang="ts">
import type {
	MonthlyProtocolTotal,
	ProtocolTotal,
	SpeedPoint,
} from "@shared/types/stats";
import { bytesToSize } from "@shared/utils";
import { computed, ref, watch } from "vue";
import api from "@/api";
import DateTimePicker from "@/components/ui/date-time-picker/DateTimePicker.vue";
import { flushDownloadStatsMinute } from "@/store/task";
import { downsampleSeries } from "@/utils/downsample";

defineOptions({ name: "StatsPage" });

const TOP_PROTOCOL_COUNT = 5;
const MAX_SPEED_POINTS = 1000;
const CHART_WIDTH = 760;
const CHART_HEIGHT = 300;
const CHART_PAD_X = 108;
const CHART_PAD_RIGHT = 16;
const CHART_PAD_TOP = 22;
const CHART_PAD_BOTTOM = 98;
const CHART_PLOT_BOTTOM = CHART_HEIGHT - CHART_PAD_BOTTOM;
const CHART_PLOT_HEIGHT = CHART_PLOT_BOTTOM - CHART_PAD_TOP;
const CHART_X_TICK_Y = CHART_PLOT_BOTTOM + 18;
const TOOLTIP_EDGE_GAP = 12;
const TOOLTIP_CURSOR_GAP = 14;
const TOOLTIP_WIDTH = 240;
const TOOLTIP_MAX_HEIGHT = 360;
const TOOLTIP_HEADER_HEIGHT = 30;
const TOOLTIP_ROW_HEIGHT = 22;

const presets = [
	{ label: "1d", seconds: 24 * 60 * 60 },
	{ label: "7d", seconds: 7 * 24 * 60 * 60 },
	{ label: "30d", seconds: 30 * 24 * 60 * 60 },
	{ label: "6mo", seconds: 183 * 24 * 60 * 60 },
	{ label: "1yr", seconds: 365 * 24 * 60 * 60 },
];

type SeriesMode = "download" | "upload" | "both";
type SplitMode = "overall" | "protocol";
type SpeedMetric = "download" | "upload";
type SpeedLine = {
	key: string;
	label: string;
	metric: SpeedMetric;
	protocol: string;
	color: string;
	path: string;
	values: number[];
};

type SpeedHoverPoint = {
	key: string;
	label: string;
	color: string;
	value: number;
	y: number;
};

type SpeedHover = {
	index: number;
	x: number;
	label: string;
	points: SpeedHoverPoint[];
	clientX: number;
	clientY: number;
};

const nowSeconds = () => Math.floor(Date.now() / 1000);
const startAt = ref(nowSeconds() - presets[2].seconds);
const endAt = ref(nowSeconds());
const loading = ref(false);
const error = ref("");
const stats = ref<{
	monthly: MonthlyProtocolTotal[];
	speed: SpeedPoint[];
	protocolTotals: ProtocolTotal[];
} | null>(null);
const expandedProtocols = ref(false);
const splitMode = ref<SplitMode>("overall");
const seriesMode = ref<SeriesMode>("both");
const showTickLabels = ref(true);
const speedHover = ref<SpeedHover | null>(null);
let loadStatsId = 0;

const chartWidth = CHART_WIDTH;
const chartHeight = CHART_HEIGHT;

const formatBytes = (value: number) =>
	bytesToSize(Math.max(0, Number(value) || 0));

const toMonth = (seconds: number) => {
	const date = new Date(seconds * 1000);
	return `${date.getFullYear()}-${`${date.getMonth() + 1}`.padStart(2, "0")}`;
};

const protocolLabel = (protocol: string) => {
	const labels: Record<string, string> = {
		http: "HTTP",
		torrent: "BitTorrent",
		usenet: "Usenet",
		ftp: "FTP",
		ed2k: "ED2K",
		m3u8: "M3U8",
		media: "Media",
		adc: "ADC",
		g2: "G2",
		gift: "GiFT",
		gnutella: "Gnutella",
		unknown: "Unknown",
		other: "Other",
	};
	return labels[protocol] || protocol.toUpperCase();
};

const totalReceived = computed(() =>
	(stats.value?.protocolTotals || []).reduce(
		(sum, item) => sum + Number(item.receivedBytes || 0),
		0,
	),
);
const totalReceivedText = computed(() => formatBytes(totalReceived.value));
const monthlyRows = computed(() => stats.value?.monthly || []);
const protocolTotals = computed(() => stats.value?.protocolTotals || []);
const monthCount = computed(() => monthlyRows.value.length);
const speedPointCount = computed(() => stats.value?.speed.length || 0);
const speedPoints = computed(() =>
	downsampleSeries(stats.value?.speed || [], MAX_SPEED_POINTS, (point) =>
		point.protocols.reduce(
			(sum, item) =>
				sum + Number(item.downloadSpeed) + Number(item.uploadSpeed),
			0,
		),
	),
);
const extraProtocolCount = computed(() =>
	Math.max(0, protocolTotals.value.length - TOP_PROTOCOL_COUNT),
);
const visibleProtocolTotals = computed(() =>
	expandedProtocols.value
		? protocolTotals.value
		: protocolTotals.value.slice(0, TOP_PROTOCOL_COUNT),
);
const visibleProtocols = computed(() =>
	visibleProtocolTotals.value.map((item) => item.protocol),
);
const legendProtocols = computed(() => {
	const protocols = [...visibleProtocols.value];
	if (
		!expandedProtocols.value &&
		extraProtocolCount.value > 0 &&
		!protocols.includes("other")
	) {
		protocols.push("other");
	}
	return protocols;
});

const protocolColorOrder = computed(() =>
	[...new Set(visibleProtocols.value)].sort((left, right) =>
		left.localeCompare(right),
	),
);

const protocolHue = (protocol: string) => {
	const protocols = protocolColorOrder.value;
	const index = protocols.indexOf(protocol);
	if (index >= 0) {
		return (210 + (index * 360) / Math.max(1, protocols.length)) % 360;
	}

	let hash = 0;
	for (const char of protocol) {
		hash = (hash * 31 + char.charCodeAt(0)) >>> 0;
	}
	return hash % 360;
};

const colorForProtocol = (
	protocol: string,
	metric: SpeedMetric = "download",
) => {
	if (protocol === "overall") {
		return metric === "upload" ? "#fb923c" : "#14b8a6";
	}
	if (protocol === "other") {
		return metric === "upload" ? "#94a3b8" : "#64748b";
	}
	const hue = Number(protocolHue(protocol).toFixed(3));
	const lightness = metric === "upload" ? 62 : 43;
	return `hsl(${hue}, 72%, ${lightness}%)`;
};

const colorForSpeedLine = (protocol: string, metric: SpeedMetric) =>
	colorForProtocol(protocol, metric);

const monthlySegments = (month: MonthlyProtocolTotal) => {
	const protocolMap = new Map(
		month.protocols.map((item) => [item.protocol, item.receivedBytes]),
	);
	let visibleTotal = 0;
	const segments = visibleProtocols.value
		.map((protocol) => {
			const value = Number(protocolMap.get(protocol) || 0);
			visibleTotal += value;
			return {
				key: protocol,
				label: protocolLabel(protocol),
				value,
				color: colorForProtocol(protocol),
				width: month.total > 0 ? `${(value / month.total) * 100}%` : "0%",
			};
		})
		.filter((segment) => segment.value > 0);

	if (!expandedProtocols.value) {
		const other = Math.max(0, month.total - visibleTotal);
		if (other > 0) {
			segments.push({
				key: "other:aggregate",
				label: protocolLabel("other"),
				value: other,
				color: colorForProtocol("other"),
				width: month.total > 0 ? `${(other / month.total) * 100}%` : "0%",
			});
		}
	}
	return segments;
};

const tableRows = computed(() =>
	monthlyRows.value.flatMap((month) =>
		month.protocols.map((protocol) => ({
			month: month.month,
			...protocol,
		})),
	),
);

const speedMax = computed(() => {
	let max = 1;
	for (const line of speedLines.value) {
		for (const value of line.values) {
			if (value > max) {
				max = value;
			}
		}
	}
	return max;
});

const speedTicks = computed(() => {
	const top = CHART_PAD_TOP;
	const bottom = CHART_PLOT_BOTTOM;
	return [0, 0.5, 1].map((ratio) => {
		const value = speedMax.value * ratio;
		return {
			y: bottom - (bottom - top) * ratio,
			label: `${formatBytes(value)}/s`,
		};
	});
});

const formatTimeTick = (minute: number) => {
	const date = new Date(minute * 1000);
	return `${date.toLocaleDateString(undefined, {
		month: "short",
		day: "numeric",
	})} ${date.toLocaleTimeString(undefined, {
		hour: "2-digit",
		minute: "2-digit",
	})}`;
};

const speedXTicks = computed(() => {
	const points = speedPoints.value;
	if (!points.length) {
		return [];
	}
	const last = points.length - 1;
	const indexes =
		points.length < 4
			? points.map((_, index) => index)
			: [0, Math.floor(last / 3), Math.floor((last * 2) / 3), last];
	const plotWidth = CHART_WIDTH - CHART_PAD_X - CHART_PAD_RIGHT;
	return [...new Set(indexes)].map((index) => ({
		minute: points[index].minute,
		label: formatTimeTick(points[index].minute),
		x: CHART_PAD_X + (last === 0 ? 0 : (index / last) * plotWidth),
	}));
});

const pointValue = (
	point: SpeedPoint,
	protocol: string,
	metric: SpeedMetric,
) => {
	const protocols =
		protocol === "overall"
			? point.protocols
			: point.protocols.filter((item) => item.protocol === protocol);
	return protocols.reduce(
		(sum, item) =>
			sum +
			Number(metric === "download" ? item.downloadSpeed : item.uploadSpeed),
		0,
	);
};

const makePath = (values: number[], max: number) => {
	const plotWidth = CHART_WIDTH - CHART_PAD_X - CHART_PAD_RIGHT;
	const plotHeight = CHART_PLOT_HEIGHT;
	const bottom = CHART_PLOT_BOTTOM;
	if (values.length === 1) {
		const y = bottom - (values[0] / max) * plotHeight;
		return `M ${CHART_PAD_X} ${y} L ${CHART_PAD_X + 2} ${y}`;
	}
	return values
		.map((value, index) => {
			const x = CHART_PAD_X + (index / (values.length - 1)) * plotWidth;
			const y = bottom - (value / max) * plotHeight;
			return `${index === 0 ? "M" : "L"} ${x.toFixed(2)} ${y.toFixed(2)}`;
		})
		.join(" ");
};

const speedLines = computed<SpeedLine[]>(() => {
	const points = speedPoints.value;
	if (!points.length) {
		return [];
	}

	const protocols =
		splitMode.value === "overall" ? ["overall"] : visibleProtocols.value;
	const metrics: SpeedMetric[] =
		seriesMode.value === "both" ? ["download", "upload"] : [seriesMode.value];
	const raw = protocols.flatMap((protocol) =>
		metrics.map((metric) => ({
			key: `${protocol}:${metric}`,
			label: `${protocolLabel(protocol)} ${metric === "download" ? "Down" : "Up"}`,
			metric,
			protocol,
			color: colorForSpeedLine(protocol, metric),
			values: points.map((point) => pointValue(point, protocol, metric)),
			path: "",
		})),
	);
	let max = 1;
	for (const line of raw) {
		for (const value of line.values) {
			if (value > max) {
				max = value;
			}
		}
	}
	return raw
		.filter((line) => line.values.some((value) => value > 0))
		.map((line) => ({ ...line, path: makePath(line.values, max) }));
});

const speedTooltipStyle = computed(() => {
	const hover = speedHover.value;
	if (!hover) {
		return {};
	}
	const viewportWidth = window.innerWidth;
	const viewportHeight = window.innerHeight;
	const estimatedHeight = Math.min(
		TOOLTIP_MAX_HEIGHT,
		TOOLTIP_HEADER_HEIGHT + hover.points.length * TOOLTIP_ROW_HEIGHT,
	);
	const preferredLeft = hover.clientX + TOOLTIP_CURSOR_GAP;
	const fallbackLeft = hover.clientX - TOOLTIP_WIDTH - TOOLTIP_CURSOR_GAP;
	const left = Math.min(
		Math.max(
			TOOLTIP_EDGE_GAP,
			preferredLeft + TOOLTIP_WIDTH + TOOLTIP_EDGE_GAP <= viewportWidth
				? preferredLeft
				: fallbackLeft,
		),
		Math.max(
			TOOLTIP_EDGE_GAP,
			viewportWidth - TOOLTIP_WIDTH - TOOLTIP_EDGE_GAP,
		),
	);
	const preferredTop = hover.clientY + TOOLTIP_CURSOR_GAP;
	const fallbackTop = hover.clientY - estimatedHeight - TOOLTIP_CURSOR_GAP;
	const top = Math.min(
		Math.max(
			TOOLTIP_EDGE_GAP,
			preferredTop + estimatedHeight + TOOLTIP_EDGE_GAP <= viewportHeight
				? preferredTop
				: fallbackTop,
		),
		Math.max(
			TOOLTIP_EDGE_GAP,
			viewportHeight - estimatedHeight - TOOLTIP_EDGE_GAP,
		),
	);
	return {
		left: `${left}px`,
		top: `${top}px`,
	};
});

function clearSpeedHover() {
	speedHover.value = null;
}

function onSpeedChartMove(event: MouseEvent) {
	const points = speedPoints.value;
	const lines = speedLines.value;
	if (!points.length || !lines.length) {
		clearSpeedHover();
		return;
	}
	const target = event.currentTarget as HTMLElement | null;
	if (!target) {
		return;
	}
	const rect = target.getBoundingClientRect();
	if (rect.width <= 0) {
		clearSpeedHover();
		return;
	}
	const plotWidth = CHART_WIDTH - CHART_PAD_X - CHART_PAD_RIGHT;
	const relX = ((event.clientX - rect.left) / rect.width) * CHART_WIDTH;
	const clamped = Math.min(
		CHART_WIDTH - CHART_PAD_RIGHT,
		Math.max(CHART_PAD_X, relX),
	);
	const ratio = plotWidth <= 0 ? 0 : (clamped - CHART_PAD_X) / plotWidth;
	const index = Math.round(ratio * (points.length - 1));
	const max = speedMax.value;
	const bottom = CHART_PLOT_BOTTOM;
	const hoverPoints = lines
		.map((line) => {
			const value = line.values[index] || 0;
			return {
				key: line.key,
				label: line.label,
				color: line.color,
				value,
				y: bottom - (value / max) * CHART_PLOT_HEIGHT,
			};
		})
		.sort((a, b) => b.value - a.value);
	speedHover.value = {
		index,
		x:
			CHART_PAD_X +
			(points.length === 1 ? 0 : (index / (points.length - 1)) * plotWidth),
		label: formatTimeTick(points[index].minute),
		points: hoverPoints,
		clientX: event.clientX,
		clientY: event.clientY,
	};
}

async function loadStats() {
	const loadId = ++loadStatsId;
	loading.value = true;
	error.value = "";
	const start = Math.min(startAt.value, endAt.value);
	const end = Math.max(startAt.value, endAt.value);
	try {
		await flushDownloadStatsMinute();
		const nextStats = await api.getDownloadStats({
			start,
			end,
			startMonth: toMonth(start),
			endMonth: toMonth(end),
		});
		if (loadId !== loadStatsId) {
			return;
		}
		stats.value = nextStats;
	} catch (err) {
		if (loadId !== loadStatsId) {
			return;
		}
		error.value = (err as Error).message || "Failed to load stats";
		stats.value = null;
	} finally {
		if (loadId === loadStatsId) {
			loading.value = false;
		}
	}
}

function applyPreset(seconds: number) {
	const end = nowSeconds();
	endAt.value = end;
	startAt.value = end - seconds;
}

watch([startAt, endAt], loadStats, { immediate: true });
watch([splitMode, seriesMode, expandedProtocols], clearSpeedHover);
</script>

<style scoped>
.stats-page {
	flex: 1 1 auto;
	min-height: 0;
	min-width: 0;
	overflow: hidden;
}

.stats-main {
	display: flex;
	flex-direction: column;
	gap: 18px;
	min-height: 0;
	overflow-x: hidden;
	overflow-y: auto;
	padding: 18px;
	-webkit-overflow-scrolling: touch;
}

.stats-toolbar {
	display: grid;
	grid-template-columns: minmax(180px, 1fr) minmax(360px, 680px);
	gap: 16px;
	align-items: start;
}

.stats-title h2,
.stats-section-head h3 {
	margin: 0;
	font-size: 18px;
	font-weight: 650;
	letter-spacing: 0;
}

.stats-title span,
.stats-section-head p,
.stats-picker span,
.stats-empty,
.speed-scale,
.month-total {
	color: var(--text-2);
	font-size: 12px;
}

.stats-range {
	display: grid;
	grid-template-columns: repeat(2, minmax(160px, 1fr));
	gap: 8px;
}

.stats-picker {
	display: grid;
	gap: 4px;
}

.stats-presets {
	grid-column: 1 / -1;
	display: flex;
	flex-wrap: wrap;
	gap: 6px;
}

.stats-chip,
.stats-link,
.stats-segmented button {
	border: 1px solid var(--color-border);
	background: var(--color-background);
	color: var(--color-foreground);
	font-size: 12px;
	line-height: 1;
	transition: background 0.16s ease, border-color 0.16s ease;
}

.stats-chip {
	height: 28px;
	border-radius: 6px;
	padding: 0 10px;
}

.stats-link {
	height: 28px;
	border-radius: 6px;
	padding: 0 10px;
}

.stats-chip:hover,
.stats-chip.active,
.stats-link:hover,
.stats-segmented button:hover,
.stats-segmented button.active {
	background: var(--color-primary);
	border-color: var(--color-primary);
	color: var(--color-primary-foreground);
}

.stats-section {
	display: flex;
	flex-direction: column;
	gap: 12px;
	border-top: 1px solid var(--color-border);
	padding-top: 16px;
}

.stats-section-head {
	display: flex;
	align-items: center;
	justify-content: space-between;
	gap: 12px;
}

.stats-section-head--controls {
	align-items: flex-start;
}

.stats-controls {
	display: flex;
	flex-wrap: wrap;
	gap: 8px;
	justify-content: flex-end;
}

.stats-segmented {
	display: inline-flex;
	overflow: hidden;
	border-radius: 6px;
	border: 1px solid var(--color-border);
}

.stats-segmented button {
	height: 30px;
	border: 0;
	border-right: 1px solid var(--color-border);
	padding: 0 10px;
}

.stats-segmented button:last-child {
	border-right: 0;
}

.stats-error {
	margin: 0;
	color: var(--color-danger);
	font-size: 13px;
}

.stats-empty {
	display: flex;
	min-height: 120px;
	align-items: center;
	justify-content: center;
	border: 1px dashed var(--color-border);
	border-radius: 8px;
}

.monthly-layout {
	display: grid;
	gap: 12px;
}

.monthly-chart {
	display: grid;
	gap: 8px;
}

.month-row {
	display: grid;
	grid-template-columns: 78px minmax(140px, 1fr) 92px;
	align-items: center;
	gap: 10px;
}

.month-label,
.stats-table th,
.stats-table td {
	font-size: 12px;
}

.month-bar {
	display: flex;
	overflow: hidden;
	height: 18px;
	border-radius: 5px;
	background: var(--color-muted);
}

.month-segment {
	min-width: 2px;
}

.protocol-legend,
.speed-legend {
	display: flex;
	flex-wrap: wrap;
	gap: 8px 14px;
}

.legend-item {
	display: inline-flex;
	align-items: center;
	gap: 6px;
	color: var(--text-2);
	font-size: 12px;
}

.legend-item i {
	display: inline-block;
	width: 9px;
	height: 9px;
	border-radius: 50%;
}

.stats-table {
	width: 100%;
	border-collapse: collapse;
}

.stats-table th,
.stats-table td {
	border-top: 1px solid var(--color-border);
	padding: 8px 6px;
	text-align: left;
}

.stats-table th:last-child,
.stats-table td:last-child {
	text-align: right;
}

.speed-chart-wrap {
	display: grid;
	gap: 8px;
}

.speed-chart-stage {
	position: relative;
}

.speed-chart {
	width: 100%;
	height: 300px;
	border: 1px solid var(--color-border);
	border-radius: 8px;
	background: var(--color-background);
}

.speed-hover-guide {
	stroke: var(--color-foreground);
	stroke-width: 1;
	stroke-dasharray: 3 3;
	opacity: 0.35;
	pointer-events: none;
}

.speed-hover-dot {
	stroke: var(--color-background);
	stroke-width: 1.5;
	pointer-events: none;
}

.speed-tooltip {
	box-sizing: border-box;
	position: fixed;
	z-index: 20;
	min-width: 160px;
	width: min(240px, calc(100vw - 24px));
	max-height: min(360px, calc(100vh - 24px));
	overflow-y: auto;
	padding: 8px 10px;
	border: 1px solid var(--color-border);
	border-radius: 8px;
	background: var(--color-background);
	box-shadow: 0 8px 24px color-mix(in srgb, #000 16%, transparent);
	pointer-events: none;
}

.speed-tooltip-time {
	margin-bottom: 6px;
	color: var(--text-2);
	font-size: 11px;
}

.speed-tooltip-row {
	display: grid;
	grid-template-columns: 9px minmax(0, 1fr) auto;
	gap: 8px;
	align-items: center;
	font-size: 12px;
}

.speed-tooltip-row + .speed-tooltip-row {
	margin-top: 4px;
}

.speed-tooltip-row i {
	display: inline-block;
	width: 9px;
	height: 9px;
	border-radius: 50%;
}

.speed-tooltip-row strong {
	font-weight: 600;
	font-variant-numeric: tabular-nums;
}

.speed-grid {
	stroke: var(--color-border);
	stroke-width: 1;
	opacity: 0.65;
}

.speed-axis-label {
	fill: var(--text-2);
	font-size: 11px;
	font-weight: 600;
}

.speed-axis-label--x,
.speed-axis-label--y {
	text-anchor: middle;
}

.speed-tick-label {
	fill: var(--text-2);
	font-size: 10px;
}

.speed-tick-label--x {
	text-anchor: end;
}

.speed-tick-label--y {
	text-anchor: end;
}

.speed-line {
	fill: none;
	stroke-width: 2.2;
	stroke-linecap: round;
	stroke-linejoin: round;
}

.speed-scale {
	display: flex;
	justify-content: space-between;
}

@media (max-width: 760px) {
	.stats-toolbar {
		grid-template-columns: 1fr;
	}

	.stats-range {
		grid-template-columns: 1fr;
	}

	.month-row {
		grid-template-columns: 64px minmax(90px, 1fr);
	}

	.month-total {
		grid-column: 2;
	}

	.stats-section-head,
	.stats-section-head--controls {
		align-items: stretch;
		flex-direction: column;
	}

	.stats-controls {
		justify-content: flex-start;
	}
}
</style>

<template>
  <div class="content panel panel-layout panel-layout--v">
    <main class="panel-content">
      <form class="form-preference" ref="advancedForm" @submit.prevent>
        <div class="settings-section">
          <div class="settings-section-header">
            <div class="section-icon"><Terminal :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('preferences.completion-script') }}</h3>
            </div>
          </div>
          <div class="settings-section-content">
            <div
              class="settings-row"
              data-preference-search-target="preferences.completion-script-enabled preferences.completion-script-command preferences.completion-script-args preferences.completion-script-timeout"
            >
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.completion-script-enabled') }}
                </div>
                <div class="settings-row-description">
                  {{ $t('preferences.completion-script-tips') }}
                </div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.completionScriptEnabled"
                  @change="(val) => setAdvancedBoolean('completionScriptEnabled', val)"
                />
              </div>
            </div>
            <div v-if="form.completionScriptEnabled" style="margin-top: 14px">
              <div class="form-item-sub" style="margin-bottom: 10px">
                <label class="settings-select-item-label">{{
                  $t('preferences.completion-script-command')
                }}</label>
                <Input
                  v-model="form.completionScriptCommand"
                  :placeholder="
                    $t('preferences.completion-script-command-placeholder')
                  "
                />
              </div>
              <div class="form-item-sub" style="margin-bottom: 10px">
                <label class="settings-select-item-label">{{
                  $t('preferences.completion-script-args')
                }}</label>
                <Input
                  v-model="form.completionScriptArgs"
                  :placeholder="
                    $t('preferences.completion-script-args-placeholder')
                  "
                />
                <div class="form-info" style="margin-top: 6px">
                  {{ $t('preferences.completion-script-args-tips') }}
                </div>
              </div>
              <div class="form-item-sub" style="margin-bottom: 10px">
                <label class="settings-select-item-label">{{
                  $t('preferences.completion-script-timeout')
                }}</label>
                <NumberInput
                  v-model="form.completionScriptTimeoutMs"
                  :min="1000"
                  :max="300000"
                  :step="1000"
                />
              </div>
              <div class="form-item-sub">
                <ui-button
                  variant="outline"
                  size="sm"
                  :disabled="completionScriptTesting"
                  @click="onTestCompletionScript"
                >
                  {{ $t('preferences.completion-script-test') }}
                </ui-button>
                <div
                  v-if="completionScriptTestResult"
                  class="form-info"
                  style="margin-top: 8px; white-space: pre-wrap"
                >
                  {{ completionScriptTestResult }}
                </div>
              </div>
            </div>
          </div>
        </div>

        <div class="settings-section">
          <div class="settings-section-header">
            <div class="section-icon"><Globe :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('preferences.proxy') }}</h3>
            </div>
          </div>
          <div class="settings-section-content">
            <div class="proxy-profile-title">
              {{ $t('preferences.proxy-http-profile') }}
            </div>
            <div
              class="settings-row"
              data-preference-search-target="preferences.enable-proxy preferences.proxy-http-profile preferences.proxy-scope-label preferences.proxy-scope-download preferences.proxy-scope-update-app preferences.proxy-scope-update-trackers"
            >
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.enable-proxy') }}
                </div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="form.proxy.http.enable"
                  :title="$t('preferences.enable-proxy')"
                  :aria-label="$t('preferences.enable-proxy')"
                  @change="onHttpProxyEnableChange"
                />
              </div>
            </div>
            <div v-if="form.proxy.http.enable" style="margin-top: 14px">
              <div class="form-item-sub" style="margin-bottom: 10px">
                <Input
                  :placeholder="$t('task.task-proxy-placeholder')"
                  v-model="form.proxy.http.server"
                />
              </div>
              <div class="form-item-sub" style="margin-bottom: 10px">
                <Textarea
                  :rows="2"
                  autocomplete="off"
                  :placeholder="`${$t('preferences.proxy-bypass-input-tips')}`"
                  v-model="form.proxy.http.bypass"
                />
              </div>
              <div class="form-item-sub proxy-scope-group">
                <label class="settings-select-item-label">{{
                  $t('preferences.proxy-scope-label')
                }}</label>
                <div v-for="item in proxyScopeOptions" :key="item" class="proxy-scope-item">
                  <ui-checkbox
                    :model-value="form.proxy.http.scope.includes(item)"
                    @change="(val) => onProxyScopeToggle(item, val)"
                  >
                    {{ $t(`preferences.proxy-scope-${item}`) }}
                  </ui-checkbox>
                  <div class="proxy-scope-desc">
                    {{ $t(`preferences.proxy-scope-${item}-desc`) }}
                  </div>
                </div>
                <div class="form-info" style="margin-top: 8px">
                  <a
                    target="_blank"
                    href="https://risuko.app/docs/guides/proxy"
                    rel="noopener noreferrer"
                  >
                    {{ $t('preferences.proxy-tips') }}
                    <ExternalLink :size="12" />
                  </a>
                </div>
              </div>
            </div>
            <div class="proxy-profile-title" style="margin-top: 18px">
              {{ $t('preferences.proxy-p2p-profile') }}
            </div>
            <div
              class="settings-row"
              data-preference-search-target="preferences.proxy-p2p-profile preferences.enable-p2p-proxy preferences.proxy-p2p-tcp-server preferences.proxy-p2p-tcp-bypass preferences.proxy-p2p-udp-server preferences.proxy-p2p-udp-bypass preferences.proxy-p2p-udp-tips"
            >
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.enable-p2p-proxy') }}
                </div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="form.proxy.p2p.enable"
                  :title="$t('preferences.enable-p2p-proxy')"
                  :aria-label="$t('preferences.enable-p2p-proxy')"
                  @change="onP2pProxyEnableChange"
                />
              </div>
            </div>
            <div v-if="form.proxy.p2p.enable" style="margin-top: 14px">
              <div
                class="form-item-sub"
                style="margin-bottom: 10px"
                data-preference-search-target="preferences.proxy-p2p-tcp-server"
              >
                <label class="settings-select-item-label" style="margin-bottom: 6px; display: block">
                  {{ $t('preferences.proxy-p2p-tcp-server') }}
                </label>
                <Input
                  :placeholder="$t('task.task-proxy-placeholder')"
                  v-model="form.proxy.p2p.server"
                />
              </div>
              <div
                class="form-item-sub"
                style="margin-bottom: 10px"
                data-preference-search-target="preferences.proxy-p2p-tcp-bypass"
              >
                <label class="settings-select-item-label" style="margin-bottom: 6px; display: block">
                  {{ $t('preferences.proxy-p2p-tcp-bypass') }}
                </label>
                <Textarea
                  :rows="2"
                  autocomplete="off"
                  :placeholder="`${$t('preferences.proxy-bypass-input-tips')}`"
                  v-model="form.proxy.p2p.bypass"
                />
              </div>
              <div
                class="form-item-sub"
                style="margin-bottom: 10px"
                data-preference-search-target="preferences.proxy-p2p-udp-server"
              >
                <label class="settings-select-item-label" style="margin-bottom: 6px; display: block">
                  {{ $t('preferences.proxy-p2p-udp-server') }}
                </label>
                <Input
                  :placeholder="$t('preferences.proxy-p2p-udp-server-placeholder')"
                  v-model="form.proxy.p2p.udp.server"
                />
              </div>
              <div
                class="form-item-sub"
                style="margin-bottom: 10px"
                data-preference-search-target="preferences.proxy-p2p-udp-bypass"
              >
                <label class="settings-select-item-label" style="margin-bottom: 6px; display: block">
                  {{ $t('preferences.proxy-p2p-udp-bypass') }}
                </label>
                <Textarea
                  :rows="2"
                  autocomplete="off"
                  :placeholder="`${$t('preferences.proxy-bypass-input-tips')}`"
                  v-model="form.proxy.p2p.udp.bypass"
                />
              </div>
              <div class="form-info" style="margin-bottom: 8px">
                {{ $t('preferences.proxy-p2p-udp-tips') }}
              </div>
              <div class="form-info">
                {{ $t('preferences.proxy-p2p-tips') }}
              </div>
            </div>
          </div>
        </div>

        <div class="settings-section">
          <div class="settings-section-header">
            <div class="section-icon"><Globe :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('preferences.doh') }}</h3>
              <p>{{ $t('preferences.doh-tips') }}</p>
            </div>
          </div>
          <div class="settings-section-content">
            <div
              class="settings-row"
	              data-preference-search-target="preferences.doh-enable preferences.doh preferences.doh-provider preferences.doh-url preferences.doh-bootstrap preferences.doh-fallback"
            >
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.doh-enable') }}
                </div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.dohEnable"
                  @change="(val) => setAdvancedBoolean('dohEnable', val)"
                />
              </div>
            </div>
            <div v-if="form.dohEnable" style="margin-top: 14px">
              <div
                class="settings-select-item"
                style="margin-bottom: 10px"
                data-preference-search-target="preferences.doh-provider"
              >
                <label class="settings-select-item-label">{{
                  $t('preferences.doh-provider')
                }}</label>
                <Select v-model="form.dohProvider" class="settings-select-control">
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem
                      v-for="p in dohProviderOptions"
                      :key="p"
                      :value="p"
                    >
                      {{ $t(`preferences.doh-provider-${p}`) }}
                    </SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div
                v-if="form.dohProvider === 'custom'"
                class="form-item-sub"
                style="margin-bottom: 10px"
                data-preference-search-target="preferences.doh-url"
              >
                <label class="settings-select-item-label" for="doh-url-input" style="margin-bottom: 6px; display: block;">
                  {{ $t('preferences.doh-url') }}
                </label>
                <Input
                  id="doh-url-input"
                  :placeholder="$t('preferences.doh-url-placeholder')"
                  v-model="form.dohUrl"
                  aria-label="DoH endpoint URL"
                />
              </div>
              <div
                class="form-item-sub"
                style="margin-bottom: 10px"
                data-preference-search-target="preferences.doh-bootstrap"
              >
                <label class="settings-select-item-label" style="margin-bottom: 6px">{{
                  $t('preferences.doh-bootstrap')
                }}</label>
                <Input
                  :placeholder="$t('preferences.doh-bootstrap-placeholder')"
                  v-model="form.dohBootstrap"
                />
              </div>
              <div
                class="settings-row"
                data-preference-search-target="preferences.doh-fallback"
              >
                <div class="settings-row-content">
                  <div class="settings-row-title">
                    {{ $t('preferences.doh-fallback') }}
                  </div>
                  <div class="settings-row-description">
                    {{ $t('preferences.doh-fallback-tips') }}
                  </div>
                </div>
                <div class="settings-row-action">
                  <ui-checkbox
                    :model-value="!!form.dohFallback"
                    @change="(val) => setAdvancedBoolean('dohFallback', val)"
                  />
                </div>
              </div>
            </div>
          </div>
        </div>

        <div class="settings-section">
          <div class="settings-section-header">
            <div class="section-icon"><Radio :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('preferences.bt-tracker') }}</h3>
            </div>
          </div>
          <div class="settings-section-content">
            <div class="bt-tracker">
              <label class="settings-select-item-label" style="margin-bottom: 6px"
                >{{ $t('preferences.bt-tracker') }} Source</label
              >
              <div class="bt-tracker-source-row">
                <Popover v-model:open="trackerSourceOpen">
                  <PopoverTrigger as-child>
                    <button
                      type="button"
                      class="tracker-multi-select-trigger"
                      role="combobox"
                      :title="$t('preferences.bt-tracker')"
                      :aria-label="$t('preferences.bt-tracker')"
                      :aria-expanded="trackerSourceOpen"
                    >
                      <div class="tracker-multi-select-tags">
                        <span v-for="val in form.trackerSource" :key="val" class="tracker-tag">
                          {{ getTrackerLabel(val) }}
                          <X
                            :size="12"
                            class="tracker-tag-remove"
                            @click.stop="toggleTrackerSource(val)"
                          />
                        </span>
                        <span
                          v-if="form.trackerSource.length === 0"
                          class="tracker-multi-select-placeholder"
                          >Select sources...</span
                        >
                      </div>
                      <ChevronDown :size="16" class="tracker-multi-select-chevron" />
                    </button>
                  </PopoverTrigger>
                  <PopoverContent class="tracker-source-popover" align="start" :side-offset="4">
                    <div class="tracker-source-list">
                      <template v-for="group in trackerSourceOptions" :key="group.label">
                        <div class="tracker-source-group-label">
                          {{ group.label }}
                        </div>
                        <div
                          v-for="item in group.options"
                          :key="item.value"
                          class="tracker-source-option"
                          :class="{
                            'is-selected': form.trackerSource.includes(item.value),
                          }"
                          @click="toggleTrackerSource(item.value)"
                        >
                          <span class="tracker-source-option-label">{{ item.label }}</span>
                          <span v-if="item.cdn" class="tracker-cdn-badge">CDN</span>
                          <Check
                            v-if="form.trackerSource.includes(item.value)"
                            :size="14"
                            class="tracker-source-check"
                          />
                        </div>
                      </template>
                    </div>
                  </PopoverContent>
                </Popover>
                <ui-tooltip :content="$t('preferences.sync-tracker-tips')">
                  <ui-button
                    variant="outline"
                    size="sm"
                    @click="syncTrackerFromSource"
                    class="sync-tracker-btn"
                    :title="$t('preferences.sync-tracker-tips')"
                    :aria-label="$t('preferences.sync-tracker-tips')"
                  >
                    <RefreshCw :size="12" class="animate-spin" v-if="trackerSyncing" />
                    <RefreshCcw width="12" height="12" v-else />
                  </ui-button>
                </ui-tooltip>
              </div>
              <Textarea
                :rows="3"
                autocomplete="off"
                :placeholder="`${$t('preferences.bt-tracker-input-tips')}`"
                v-model="form.btTracker"
                style="margin-top: 10px; max-height: 5lh"
              />
              <div class="form-info" style="margin-top: 8px">
                {{ $t('preferences.bt-tracker-tips') }}
                <a
                  target="_blank"
                  href="https://github.com/ngosang/trackerslist"
                  rel="noopener noreferrer"
                >
                  ngosang/trackerslist
                  <ExternalLink :size="12" />
                </a>
                <a
                  target="_blank"
                  href="https://github.com/XIU2/TrackersListCollection"
                  rel="noopener noreferrer"
                >
                  XIU2/TrackersListCollection
                  <ExternalLink :size="12" />
                </a>
              </div>
            </div>
            <div class="settings-row" style="margin-top: 8px">
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.auto-sync-tracker') }}
                </div>
                <div class="settings-row-description" v-if="form.lastSyncTrackerTime > 0">
                  {{ new Date(form.lastSyncTrackerTime).toLocaleString() }}
                </div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.autoSyncTracker"
                  @change="(val) => setAdvancedBoolean('autoSyncTracker', val)"
                />
              </div>
            </div>

            <div class="settings-row" style="margin-top: 8px">
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.bt-max-peers-per-torrent') }}
                </div>
                <div class="settings-row-description">
                  {{ $t('preferences.bt-max-peers-per-torrent-tips') }}
                </div>
              </div>
              <div class="settings-row-action">
                <NumberInput
                  v-model="form.btMaxPeersPerTorrent"
                  :min="10"
                  :max="500"
                  :step="1"
                />
              </div>
            </div>

            <div class="settings-row" style="margin-top: 8px">
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.bt-max-outstanding-per-peer') }}
                </div>
                <div class="settings-row-description">
                  {{ $t('preferences.bt-max-outstanding-per-peer-tips') }}
                </div>
              </div>
              <div class="settings-row-action">
                <NumberInput
                  v-model="form.btMaxOutstandingPerPeer"
                  :min="0"
                  :max="500"
                  :step="1"
                />
              </div>
            </div>

            <div class="settings-row" style="margin-top: 8px">
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.bt-max-connections') }}
                </div>
                <div class="settings-row-description">
                  {{ $t('preferences.bt-max-connections-tips') }}
                </div>
              </div>
              <div class="settings-row-action">
                <NumberInput
                  v-model="form.btMaxConnections"
                  :min="20"
                  :max="5000"
                  :step="10"
                />
              </div>
            </div>

            <div class="settings-row" style="margin-top: 8px">
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.bt-ban-corrupt-peers') }}
                </div>
                <div class="settings-row-description">
                  {{ $t('preferences.bt-ban-corrupt-peers-tips') }}
                </div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.btBanCorruptPeers"
                  @change="(val) => setAdvancedBoolean('btBanCorruptPeers', val)"
                />
              </div>
            </div>

            <div
              v-if="form.btBanCorruptPeers"
              class="settings-row"
              style="margin-top: 8px"
            >
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.bt-ban-corrupt-strikes') }}
                </div>
                <div class="settings-row-description">
                  {{ $t('preferences.bt-ban-corrupt-strikes-tips') }}
                </div>
              </div>
              <div class="settings-row-action">
                <NumberInput
                  v-model="form.btBanCorruptStrikes"
                  :min="1"
                  :max="100"
                  :step="1"
                />
              </div>
            </div>

            <div class="settings-row" style="margin-top: 8px">
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.bt-enable-upnp') }}
                </div>
                <div class="settings-row-description">
                  {{ $t('preferences.bt-enable-upnp-tips') }}
                </div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.btEnableUpnp"
                  @change="(val) => setAdvancedBoolean('btEnableUpnp', val)"
                />
              </div>
            </div>

            <div class="settings-row" style="margin-top: 8px">
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.bt-upnp-lease') }}
                </div>
                <div class="settings-row-description">
                  {{ $t('preferences.bt-upnp-lease-tips') }}
                </div>
              </div>
              <div class="settings-row-action">
                <NumberInput
                  v-model="form.btUpnpLease"
                  :min="60"
                  :max="86400"
                  :step="1"
                />
              </div>
            </div>

            <div class="settings-row" style="margin-top: 8px">
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.bt-enable-lsd') }}
                </div>
                <div class="settings-row-description">
                  {{ $t('preferences.bt-enable-lsd-tips') }}
                </div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.btEnableLsd"
                  @change="(val) => setAdvancedBoolean('btEnableLsd', val)"
                />
              </div>
            </div>

            <div class="settings-row" style="margin-top: 8px">
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.bt-create-subfolder') }}
                </div>
                <div class="settings-row-description">
                  {{ $t('preferences.bt-create-subfolder-tips') }}
                </div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.btCreateSubfolder"
                  @change="(val) => setAdvancedBoolean('btCreateSubfolder', val)"
                />
              </div>
            </div>

            <div class="settings-row" style="margin-top: 8px">
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.bt-encryption-policy') }}
                </div>
                <div class="settings-row-description">
                  {{ $t('preferences.bt-encryption-policy-tips') }}
                </div>
              </div>
              <div class="settings-row-action">
                <Select v-model="form.btEncryptionPolicy">
                  <SelectTrigger style="width: 180px">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="plaintext">
                      {{ $t('preferences.bt-encryption-plaintext') }}
                    </SelectItem>
                    <SelectItem value="prefer">
                      {{ $t('preferences.bt-encryption-prefer') }}
                    </SelectItem>
                    <SelectItem value="require">
                      {{ $t('preferences.bt-encryption-require') }}
                    </SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>

            <div class="settings-row" style="margin-top: 8px">
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.bt-listen-v6') }}
                </div>
                <div class="settings-row-description">
                  {{ $t('preferences.bt-listen-v6-tips') }}
                </div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.btListenV6"
                  @change="(val) => setAdvancedBoolean('btListenV6', val)"
                />
              </div>
            </div>

          </div>
        </div>

        <div class="settings-section">
          <div class="settings-section-header">
            <div class="section-icon"><Globe :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('preferences.http-reliability') }}</h3>
              <p>{{ $t('preferences.http-reliability-tips') }}</p>
            </div>
          </div>
          <div class="settings-section-content">
            <div class="settings-select-group settings-select-group--stack">
              <div class="settings-select-item">
                <label class="settings-select-item-label">
                  {{ $t('preferences.connect-timeout') }} ({{ $t('preferences.unit-seconds') }})
                </label>
                <NumberInput v-model="form.connectTimeout" :min="1" :max="600" :step="1" />
              </div>
              <div class="settings-select-item">
                <label class="settings-select-item-label">
                  {{ $t('preferences.nzb-body-timeout') }} ({{ $t('preferences.unit-seconds') }})
                </label>
                <NumberInput v-model="form.nzbBodyTimeout" :min="1" :max="3600" :step="1" />
              </div>
              <div class="settings-select-item">
                <label class="settings-select-item-label">
                  {{ $t('preferences.lowest-speed-limit') }} ({{ $t('preferences.unit-kib-per-sec') }})
                </label>
                <NumberInput v-model="form.lowestSpeedLimit" :min="0" :max="1048576" :step="1" />
              </div>
              <div class="settings-select-item">
                <label class="settings-select-item-label">
                  {{ $t('preferences.lowest-speed-limit-timeout') }} ({{ $t('preferences.unit-seconds') }})
                </label>
                <NumberInput v-model="form.lowestSpeedLimitTimeout" :min="1" :max="3600" :step="1" />
              </div>
              <div class="settings-select-item">
                <label class="settings-select-item-label">{{
                  $t('preferences.uri-selector')
                }}</label>
                <Select v-model="form.uriSelector" class="settings-select-control">
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="feedback">
                      {{ $t('preferences.uri-selector-feedback') }}
                    </SelectItem>
                    <SelectItem value="inorder">
                      {{ $t('preferences.uri-selector-inorder') }}
                    </SelectItem>
                    <SelectItem value="adaptive">
                      {{ $t('preferences.uri-selector-adaptive') }}
                    </SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
            <div class="form-info" style="margin-top: 8px">
              {{ $t('preferences.lowest-speed-limit-help') }}
            </div>
          </div>
        </div>

        <div class="settings-section">
          <div class="settings-section-header">
            <div class="section-icon"><FileText :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('preferences.storage') }}</h3>
              <p>{{ $t('preferences.storage-tips') }}</p>
            </div>
          </div>
          <div class="settings-section-content">
            <div class="settings-select-group">
              <div class="settings-select-item">
                <label class="settings-select-item-label">{{
                  $t('preferences.file-allocation')
                }}</label>
                <Select v-model="form.fileAllocation" class="settings-select-control">
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="falloc">
                      {{ $t('preferences.file-allocation-falloc') }}
                    </SelectItem>
                    <SelectItem value="trunc">
                      {{ $t('preferences.file-allocation-trunc') }}
                    </SelectItem>
                    <SelectItem value="none">
                      {{ $t('preferences.file-allocation-none') }}
                    </SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
            <div class="form-info" style="margin-top: 8px">
              {{ $t('preferences.file-allocation-tips') }}
            </div>
          </div>
        </div>

        <div class="settings-section">
          <div class="settings-section-header">
            <div class="section-icon"><Server :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('preferences.ed2k-server') }}</h3>
            </div>
          </div>
          <div class="settings-section-content">
            <Textarea
              :rows="3"
              autocomplete="off"
              :placeholder="`${$t('preferences.ed2k-server-input-tips')}`"
              v-model="form.ed2kServer"
              style="max-height: 5lh"
            />
            <div class="form-info" style="margin-top: 8px">
              {{ $t('preferences.ed2k-server-tips') }}
            </div>
            <div
              class="settings-row"
              style="margin-top: 16px"
              data-preference-search-target="preferences.ed2k-kad preferences.ed2k-enable-kad preferences.ed2k-kad-port"
            >
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.ed2k-enable-kad') }}
                </div>
                <div class="settings-row-description">
                  {{ $t('preferences.ed2k-enable-kad-tips') }}
                </div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.ed2kEnableKad"
                  :title="$t('preferences.ed2k-enable-kad')"
                  :aria-label="$t('preferences.ed2k-enable-kad')"
                  @change="(val) => setAdvancedBoolean('ed2kEnableKad', val)"
                />
              </div>
            </div>
            <div class="settings-select-group" style="margin-top: 12px">
              <div class="settings-select-item">
                <label class="settings-select-item-label">
                  {{ $t('preferences.ed2k-kad-port') }}
                </label>
                <Input
                  v-model="form.ed2kKadPort"
                  inputmode="numeric"
                  maxlength="5"
                  placeholder="4672"
                  @blur="onEd2kKadPortBlur"
                />
              </div>
            </div>
            <div class="form-info" style="margin-top: 8px">
              {{ $t('preferences.ed2k-kad-port-tips') }}
            </div>
          </div>
        </div>

        <div class="settings-section">
          <div class="settings-section-header">
            <div class="section-icon"><FileKey :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('preferences.ftp-sftp-settings') }}</h3>
              <p>{{ $t('preferences.ftp-sftp-settings-tips') }}</p>
            </div>
          </div>
          <div class="settings-section-content">
            <div class="settings-select-group">
              <div class="settings-select-item">
                <label class="settings-select-item-label">{{ $t('preferences.ftp-username') }}</label>
                <Input placeholder="anonymous" v-model="form.ftpUser" />
              </div>
              <div class="settings-select-item">
                <label class="settings-select-item-label">{{ $t('preferences.ftp-password') }}</label>
                <Input type="password" placeholder="risuko@" v-model="form.ftpPasswd" />
              </div>
            </div>
            <div class="settings-select-group" style="margin-top: 10px">
              <div class="settings-select-item">
                <label class="settings-select-item-label">{{ $t('preferences.sftp-private-key') }}</label>
                <Input placeholder="~/.ssh/id_rsa" v-model="form.sftpPrivateKey" />
              </div>
              <div class="settings-select-item">
                <label class="settings-select-item-label">{{ $t('preferences.sftp-key-passphrase') }}</label>
                <Input type="password" :placeholder="$t('preferences.sftp-key-passphrase-tips')" v-model="form.sftpKeyPassphrase" />
              </div>
            </div>
          </div>
        </div>

        <div class="settings-section">
          <div class="settings-section-header">
            <div class="section-icon"><KeyRound :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('preferences.saved-credentials') }}</h3>
              <p>{{ $t('preferences.saved-credentials-description') }}</p>
            </div>
          </div>
          <div class="settings-section-content">
            <credential-manager />
          </div>
        </div>

        <div v-if="isDesktopUpdater" class="settings-section">
          <div class="settings-section-header">
            <div class="section-icon"><Download :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('preferences.auto-update') }}</h3>
            </div>
          </div>
          <div class="settings-section-content">
            <div class="settings-row">
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.auto-check-update') }}
                </div>
                <div class="settings-row-description">
                  {{ $t('preferences.last-check-update-time') }}<span v-if="lastCheckedUpdateLabel">: {{ lastCheckedUpdateLabel }}</span>
                </div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.autoCheckUpdate"
                  :title="$t('preferences.auto-check-update')"
                  :aria-label="$t('preferences.auto-check-update')"
                  @change="(val) => setAdvancedBoolean('autoCheckUpdate', val)"
                />
              </div>
            </div>
            <div class="settings-row" style="margin-top: 10px">
              <div class="settings-row-content">
                <div class="settings-row-description" v-if="updaterStatusLabel">
                  {{ updaterStatusLabel }}<span v-if="updaterState.version"> ({{ updaterState.version }})</span>
                </div>
              </div>
              <div class="settings-row-action">
                <ui-button
                  variant="outline"
                  size="sm"
                  :disabled="updaterBusy"
                  @click="checkForUpdatesNow"
                >
                  {{ $t('app.check-updates-now') }}
                </ui-button>
              </div>
            </div>
            <div v-if="updaterState.progress !== null" class="form-info" style="margin-top: 6px">
              {{ $t('app.update-progress', { progress: Math.round(updaterState.progress) }) }}
            </div>
          </div>
        </div>

        <div class="settings-section">
          <div class="settings-section-header">
            <div class="section-icon"><FileText :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('preferences.m3u8-output-format') }}</h3>
            </div>
          </div>
          <div class="settings-section-content">
            <div class="settings-select-group">
              <div class="settings-select-item">
                <label class="settings-select-item-label">{{ $t('preferences.m3u8-output-format-label') }}</label>
                <Select v-model="form.m3u8OutputFormat">
                  <SelectTrigger class="w-30">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="ts">.ts</SelectItem>
                    <SelectItem value="mp4">.mp4 (ffmpeg)</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
          </div>
        </div>

        <div class="settings-section">
          <div class="settings-section-header">
            <div class="section-icon"><Video :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('preferences.media-settings') }}</h3>
              <p>{{ $t('preferences.media-settings-tips') }}</p>
            </div>
          </div>
          <div class="settings-section-content">
            <div class="settings-select-group settings-select-group--stack">
              <div class="settings-select-item">
                <label class="settings-select-item-label">{{ $t('preferences.media-format') }}</label>
                <Input
                  :placeholder="$t('preferences.media-format-placeholder')"
                  v-model="form.mediaFormat"
                />
              </div>
              <div class="form-info" style="margin-top: 4px">
                {{ $t('preferences.media-format-tips') }}
              </div>
            </div>
          </div>
        </div>

        <div class="settings-section">
          <div class="settings-section-header">
            <div class="section-icon"><Cable :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('preferences.rpc') }}</h3>
            </div>
          </div>
          <div class="settings-section-content">
            <div class="settings-row" style="margin-bottom: 8px">
              <div class="settings-row-content">
                <div class="settings-row-title">{{ $t('preferences.external-engine-enable') }}</div>
                <div class="settings-row-description">
                  {{ $t('preferences.external-engine-enable-tips') }}
                </div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="form.externalEngineEnabled"
                  @change="(val) => setAdvancedBoolean('externalEngineEnabled', val)"
                />
              </div>
            </div>

            <div v-if="form.externalEngineEnabled" class="settings-select-group" style="margin-bottom: 8px">
              <div class="settings-select-item">
                <label class="settings-select-item-label">{{
                  $t('preferences.external-engine-ip')
                }}</label>
                <Input
                  :placeholder="externalRpcDefaultHost"
                  v-model="form.externalEngineHost"
                  @blur="onExternalEngineHostBlur"
                />
              </div>
              <div class="settings-select-item">
                <label class="settings-select-item-label">{{
                  $t('preferences.external-engine-port')
                }}</label>
                <Input
                  :placeholder="String(rpcDefaultPort)"
                  :maxlength="8"
                  v-model="form.externalEnginePort"
                  @blur="onExternalEnginePortBlur"
                />
              </div>
              <div class="settings-select-item">
                <label class="settings-select-item-label">{{ $t('preferences.external-engine-secret') }}</label>
                <Input
                  :type="hideExternalRpcSecret ? 'password' : 'text'"
                  placeholder="RPC Secret"
                  :maxlength="64"
                  v-model="form.externalEngineSecret"
                />
              </div>
            </div>

            <div class="settings-select-group">
              <div class="settings-select-item">
                <label class="settings-select-item-label">{{
                  $t('preferences.rpc-listen-port')
                }}</label>
                <div class="input-group">
                  <Input
                    :placeholder="String(rpcDefaultPort)"
                    :maxlength="8"
                    :disabled="form.externalEngineEnabled"
                    v-model="form.rpcListenPort"
                    @blur="onRpcListenPortBlur"
                  />
                  <span class="input-append" v-if="!form.externalEngineEnabled">
                    <button
                      type="button"
                      class="input-append-action"
                      :title="$t('preferences.randomize-port')"
                      :aria-label="$t('preferences.randomize-port')"
                      @click.prevent="rollPort('rpcListenPort', rpcDefaultPort, 20000)"
                    >
                      <Dices :size="12" />
                    </button>
                  </span>
                </div>
              </div>
              <div class="settings-select-item">
                <label class="settings-select-item-label">{{ $t('preferences.rpc-secret') }}</label>
                <div class="input-group">
                  <Input
                    :type="hideRpcSecret ? 'password' : 'text'"
                    placeholder="RPC Secret"
                    :maxlength="256"
                    :disabled="form.externalEngineEnabled"
                    v-model="form.rpcSecret"
                  />
                  <span class="input-append" v-if="!form.externalEngineEnabled">
                    <button
                      type="button"
                      class="input-append-action"
                      :title="$t('preferences.generate-rpc-secret')"
                      :aria-label="$t('preferences.generate-rpc-secret')"
                      @click.prevent="onRpcSecretDiceClick"
                    >
                      <Dices :size="12" />
                    </button>
                  </span>
                </div>
              </div>
            </div>
            <div class="form-info" style="margin-top: 8px; display: flex; align-items: center; gap: 8px">
              <a
                target="_blank"
                href="https://github.com/YueMiyuki/Risuko/wiki/RPC"
                rel="noopener noreferrer"
              >
                {{ $t('preferences.rpc-secret-tips') }}
                <ExternalLink :size="12" />
              </a>
              <ui-button size="sm" variant="outline" @click="copyRpcUrlToClipboard">
                <Copy :size="12" />
                {{ $t('preferences.copy-rpc-url') }}
              </ui-button>
            </div>
          </div>
        </div>

        <div
          class="settings-section"
          data-preference-search-target="preferences.pbh preferences.pbh-enable preferences.pbh-listen-port preferences.pbh-rpc-secret"
        >
          <div class="settings-section-header">
            <div class="section-icon"><Shield :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('preferences.pbh') }}</h3>
            </div>
          </div>
          <div class="settings-section-content">
            <div class="settings-row" style="margin-bottom: 8px">
              <div class="settings-row-content">
                <div class="settings-row-title">{{ $t('preferences.pbh-enable') }}</div>
                <div class="settings-row-description">
                  {{ $t('preferences.pbh-enable-tips') }}
                </div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.pbhEnable"
                  :disabled="form.externalEngineEnabled"
                  @change="(val) => setAdvancedBoolean('pbhEnable', val)"
                />
              </div>
            </div>
            <div v-if="form.pbhEnable && !form.externalEngineEnabled" class="settings-select-group">
              <div class="settings-select-item">
                <label class="settings-select-item-label">{{
                  $t('preferences.pbh-listen-port')
                }}</label>
                <div class="input-group">
                  <Input
                    :placeholder="String(pbhDefaultPort)"
                    :maxlength="8"
                    v-model="form.pbhListenPort"
                    @blur="onPbhListenPortBlur"
                  />
                  <span class="input-append">
                    <button
                      type="button"
                      class="input-append-action"
                      :title="$t('preferences.randomize-port')"
                      :aria-label="$t('preferences.randomize-port')"
                      @click.prevent="rollPort('pbhListenPort', pbhDefaultPort, 20000)"
                    >
                      <Dices :size="12" />
                    </button>
                  </span>
                </div>
              </div>
              <div class="settings-select-item">
                <label class="settings-select-item-label">{{ $t('preferences.pbh-rpc-secret') }}</label>
                <div class="input-group">
                  <Input
                    :type="hidePbhRpcSecret ? 'password' : 'text'"
                    placeholder="RPC Token"
                    :maxlength="64"
                    v-model="form.pbhRpcSecret"
                  />
                  <span class="input-append">
                    <button
                      type="button"
                      class="input-append-action"
                      :title="$t('preferences.generate-pbh-rpc-secret')"
                      :aria-label="$t('preferences.generate-pbh-rpc-secret')"
                      @click.prevent="onPbhRpcSecretDiceClick"
                    >
                      <Dices :size="12" />
                    </button>
                  </span>
                </div>
              </div>
            </div>
            <div class="form-info" style="margin-top: 8px">
              {{ $t('preferences.pbh-endpoint-hint', { port: form.pbhListenPort || pbhDefaultPort }) }}
            </div>
          </div>
        </div>

        <div class="settings-section">
          <div class="settings-section-header">
            <div class="section-icon"><Network :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('preferences.port') }}</h3>
            </div>
          </div>
          <div class="settings-section-content">
            <div class="settings-select-group">
              <div class="settings-select-item">
                <label class="settings-select-item-label">{{ $t('preferences.bt-port') }}</label>
                <div class="input-group">
                  <Input placeholder="BT Port" :maxlength="8" v-model="form.listenPort" />
                  <span class="input-append">
                    <button
                      type="button"
                      class="input-append-action"
                      :title="$t('preferences.randomize-port')"
                      :aria-label="$t('preferences.randomize-port')"
                      @click.prevent="rollPort('listenPort', 20000, 24999)"
                    >
                      <Dices :size="12" />
                    </button>
                  </span>
                </div>
              </div>
              <div class="settings-select-item">
                <label class="settings-select-item-label">{{ $t('preferences.dht-port') }}</label>
                <div class="input-group">
                  <Input placeholder="DHT Port" :maxlength="8" v-model="form.dhtListenPort" />
                  <span class="input-append">
                    <button
                      type="button"
                      class="input-append-action"
                      :title="$t('preferences.randomize-port')"
                      :aria-label="$t('preferences.randomize-port')"
                      @click.prevent="rollPort('dhtListenPort', 25000, 29999)"
                    >
                      <Dices :size="12" />
                    </button>
                  </span>
                </div>
              </div>
              <div class="settings-select-item">
                <label class="settings-select-item-label">{{ $t('preferences.ed2k-port') }}</label>
                <div class="input-group">
                  <Input placeholder="4662" :maxlength="8" v-model="form.ed2kPort" />
                  <span class="input-append">
                    <button
                      type="button"
                      class="input-append-action"
                      :title="$t('preferences.randomize-port')"
                      :aria-label="$t('preferences.randomize-port')"
                      @click.prevent="rollPort('ed2kPort', 30000, 34999)"
                    >
                      <Dices :size="12" />
                    </button>
                  </span>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div class="settings-section">
          <div class="settings-section-header">
            <div class="section-icon"><Link :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('preferences.download-protocol') }}</h3>
              <p>{{ $t('preferences.protocols-default-client') }}</p>
            </div>
          </div>
          <div class="settings-section-content">
            <div class="settings-row">
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.protocols-magnet') }}
                </div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.protocols.magnet"
                  @change="(val) => onProtocolsChange('magnet', val)"
                />
              </div>
            </div>
            <div class="settings-row">
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.protocols-thunder') }}
                </div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.protocols.thunder"
                  @change="(val) => onProtocolsChange('thunder', val)"
                />
              </div>
            </div>
            <div class="settings-row">
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.protocols-ed2k') }}
                </div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.protocols.ed2k"
                  @change="(val) => onProtocolsChange('ed2k', val)"
                />
              </div>
            </div>
            <div class="settings-row">
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.protocols-adc') }}
                </div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.protocols.adc"
                  @change="(val) => onProtocolsChange('adc', val)"
                />
              </div>
            </div>
            <div class="settings-row">
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.protocols-gnutella') }}
                </div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.protocols.gnutella"
                  @change="(val) => onProtocolsChange('gnutella', val)"
                />
              </div>
            </div>
            <div class="settings-row">
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.protocols-g2') }}
                </div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.protocols.g2"
                  @change="(val) => onProtocolsChange('g2', val)"
                />
              </div>
            </div>
            <div
              class="settings-row"
              data-preference-search-target="preferences.gift-integration preferences.gift-enabled preferences.gift-host preferences.gift-port"
            >
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.gift-enabled') }}
                </div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.giftEnabled"
                  :title="$t('preferences.gift-enabled')"
                  :aria-label="$t('preferences.gift-enabled')"
                  @change="(val) => setAdvancedBoolean('giftEnabled', val)"
                />
              </div>
            </div>
            <div v-if="form.giftEnabled" class="settings-select-group" style="margin-top: 12px">
              <div class="settings-select-item">
                <label class="settings-select-item-label">
                  {{ $t('preferences.gift-host') }}
                </label>
                <Input
                  v-model="form.giftHost"
                  autocomplete="off"
                  placeholder="127.0.0.1"
                  @blur="onGiftHostBlur"
                />
              </div>
              <div class="settings-select-item">
                <label class="settings-select-item-label">
                  {{ $t('preferences.gift-port') }}
                </label>
                <Input
                  v-model="form.giftPort"
                  inputmode="numeric"
                  maxlength="5"
                  placeholder="1213"
                  @blur="onGiftPortBlur"
                />
              </div>
            </div>
          </div>
        </div>

        <div class="settings-section">
          <div class="settings-section-header">
            <div class="section-icon"><UserCircle :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('preferences.user-agent') }}</h3>
              <p>{{ $t('preferences.mock-user-agent') }}</p>
            </div>
          </div>
          <div class="settings-section-content">
            <Textarea
              :rows="2"
              autocomplete="off"
              placeholder="User-Agent"
              v-model="form.userAgent"
            />
            <div class="ua-group">
              <ui-button size="sm" variant="outline" @click="() => changeUA('aria2')"
                >Aria2</ui-button
              >
              <ui-button size="sm" variant="outline" @click="() => changeUA('transmission')"
                >Transmission</ui-button
              >
              <ui-button size="sm" variant="outline" @click="() => changeUA('chrome')"
                >Chrome</ui-button
              >
              <ui-button size="sm" variant="outline" @click="() => changeUA('firefox')"
                >Firefox</ui-button
              >
              <ui-button size="sm" variant="outline" @click="() => changeUA('du')">du</ui-button>
            </div>
          </div>
        </div>

        <div class="settings-section">
          <div class="settings-section-header">
            <div class="section-icon"><Cookie :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('preferences.cookies') }}</h3>
              <p>{{ $t('preferences.cookies-tips') }}</p>
            </div>
          </div>
          <div class="settings-section-content">
            <Input
              v-model="form.loadCookies"
              :placeholder="$t('preferences.load-cookies-placeholder')"
              autocomplete="off"
            />
          </div>
        </div>

        <div class="settings-section">
          <div class="settings-section-header">
            <div class="section-icon"><Globe :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('preferences.saved-cookies') }}</h3>
              <p>{{ $t('preferences.saved-cookies-tips') }}</p>
            </div>
            <ui-button
              v-if="cookieEntries.length > 0"
              size="sm"
              variant="ghost"
              class="ml-auto"
              @click="handleClearAllCookieEntries"
            >
              {{ $t('preferences.saved-cookies-clear') }}
            </ui-button>
          </div>
          <div class="settings-section-content">
            <div
              v-if="cookieEntries.length === 0"
              class="text-xs text-muted-foreground"
            >
              {{ $t('preferences.saved-cookies-empty') }}
            </div>
            <div v-else class="overflow-hidden rounded-md border border-border/60">
              <table class="w-full text-left text-[11px]">
                <thead class="bg-muted/40 text-muted-foreground">
                  <tr>
                    <th class="px-2 py-1.5 font-medium">{{ $t('preferences.saved-cookies-host') }}</th>
                    <th class="px-2 py-1.5 font-medium">{{ $t('preferences.saved-cookies-browser') }}</th>
                    <th class="px-2 py-1.5 font-medium">{{ $t('preferences.saved-cookies-count') }}</th>
                    <th class="px-2 py-1.5 font-medium">{{ $t('preferences.saved-cookies-imported') }}</th>
                    <th class="px-2 py-1.5"></th>
                  </tr>
                </thead>
                <tbody>
                  <tr
                    v-for="entry in cookieEntries"
                    :key="entry.host"
                    class="border-t border-border/60"
                  >
                    <td class="px-2 py-1.5 font-mono">{{ entry.host }}</td>
                    <td class="px-2 py-1.5">{{ entry.browserId }}</td>
                    <td class="px-2 py-1.5">{{ entry.cookieCount }}</td>
                    <td class="px-2 py-1.5 text-muted-foreground">{{ formatTimestamp(entry.importedAt) }}</td>
                    <td class="px-2 py-1.5 text-right">
                      <ui-button
                        size="sm"
                        variant="ghost"
                        class="h-6 px-2 text-[11px]"
                        @click="() => handleDeleteCookieEntry(entry.host)"
                      >
                        {{ $t('preferences.saved-cookies-delete') }}
                      </ui-button>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </div>

        <div class="settings-section">
          <div class="settings-section-header">
            <div class="section-icon"><KeyRound :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('preferences.netrc') }}</h3>
              <p>{{ $t('preferences.netrc-tips') }}</p>
            </div>
          </div>
          <div class="settings-section-content">
            <Input
              v-model="form.netrcPath"
              :placeholder="$t('preferences.netrc-path-placeholder')"
              autocomplete="off"
            />
            <div class="settings-row" style="margin-top: 8px">
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.no-netrc') }}
                </div>
                <div class="settings-row-description">
                  {{ $t('preferences.no-netrc-tips') }}
                </div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.noNetrc"
                  @change="(val) => setAdvancedBoolean('noNetrc', val)"
                />
              </div>
            </div>
          </div>
        </div>

        <div class="settings-section">
          <div class="settings-section-header">
            <div class="section-icon"><Code :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('preferences.developer') }}</h3>
            </div>
          </div>
          <div class="settings-section-content">
            <div class="dev-paths-grid">
              <div class="dev-path-card">
                <div class="dev-path-card-header">
                  <ScrollText :size="13" class="dev-path-card-icon" />
                  <span class="dev-path-card-label">{{ $t('preferences.app-log-path') }}</span>
                </div>
                <div class="dev-path-card-body">
                  <div class="input-group input-group--bordered dev-log-path-group">
                    <textarea
                      :value="visibleLogPath"
                      :placeholder="$t('preferences.log-dir-override-placeholder')"
                      autocomplete="off"
                      class="dev-log-path-input"
                      readonly
                      rows="1"
                      wrap="off"
                    />
                    <span class="input-append dev-log-path-actions" v-if="isRenderer">
                      <select-directory @selected="handleLogDirSelected" />
                      <show-in-folder :path="visibleLogPath" :size="14" />
                    </span>
                  </div>
                  <div class="form-info" style="margin-top: 4px">
                    {{ $t('preferences.log-dir-override-tips', { path: logPath }) }}
                  </div>
                </div>
              </div>
              <div class="dev-path-card">
                <div class="dev-path-card-header">
                  <Settings :size="13" class="dev-path-card-icon" />
                  <span class="dev-path-card-label">Log Level</span>
                </div>
                <div class="dev-path-card-body">
                  <Select v-model="form.logLevel" class="dev-log-level-select">
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem v-for="item in logLevels" :key="item" :value="item">
                        {{ item }}
                      </SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
            </div>

            <div class="settings-select-group settings-select-group--stack" style="margin-top: 12px">
              <div class="settings-select-item">
                <label class="settings-select-item-label">{{ $t('preferences.engine-overrides') }}</label>
                <Textarea
                  :rows="8"
                  autocomplete="off"
                  :placeholder="$t('preferences.engine-overrides-placeholder')"
                  v-model="form.engineOverridesText"
                  style="max-height: 16lh"
                />
              </div>
              <div class="form-info" style="margin-top: 4px">
                {{ $t('preferences.engine-overrides-tips') }}
              </div>
            </div>

            <div class="dev-danger-zone">
              <div class="dev-danger-zone-label">
                <AlertTriangle :size="13" />
                Danger Zone
              </div>
              <div class="dev-danger-zone-actions">
                <div class="dev-danger-action">
                  <div class="dev-danger-action-info">
                    <span class="dev-danger-action-title">{{
                      $t('preferences.session-reset')
                    }}</span>
                  </div>
                  <ui-button variant="outline" size="sm" @click="() => onSessionResetClick()">
                    {{ $t('preferences.session-reset') }}
                  </ui-button>
                </div>
                <div class="dev-danger-action dev-danger-action--destructive">
                  <div class="dev-danger-action-info">
                    <span class="dev-danger-action-title">{{
                      $t('preferences.factory-reset')
                    }}</span>
                  </div>
                  <ui-button variant="destructive" size="sm" @click="() => onFactoryResetClick()">
                    {{ $t('preferences.factory-reset') }}
                  </ui-button>
                </div>
              </div>
            </div>
          </div>
        </div>
      </form>
      <div class="form-actions">
        <ui-button @click="resetForm('advancedForm')">
          {{ $t('preferences.discard') }}
        </ui-button>
        <ui-button variant="primary" @click="submitForm('advancedForm')">
          {{ $t('preferences.save') }}
        </ui-button>
      </div>
    </main>
  </div>
</template>

<script lang="ts">
import {
	AlertTriangle,
	Cable,
	Check,
	ChevronDown,
	Code,
	Cookie,
	Copy,
	Dices,
	Download,
	ExternalLink,
	FileKey,
	FileText,
	Globe,
	KeyRound,
	Link,
	Network,
	Radio,
	RefreshCcw,
	RefreshCw,
	ScrollText,
	Server,
	Settings,
	Shield,
	Terminal,
	UserCircle,
	Video,
	X,
} from "@lucide/vue";
import {
	DEFAULT_ED2K_SERVERS,
	DOH_PROVIDER_OPTIONS,
	DOH_PROVIDERS,
	ENGINE_PBH_RPC_PORT,
	ENGINE_RPC_HOST,
	ENGINE_RPC_PORT,
	LOG_LEVELS,
	PROXY_SCOPE_OPTIONS,
	TRACKER_SOURCE_OPTIONS,
} from "@shared/constants";
import {
	normalizeProxyConfig,
	redactProxySettings,
} from "@shared/types/config";
import userAgentMap from "@shared/ua";
import {
	buildRpcUrl,
	changedConfig,
	convertCommaToLine,
	convertLineToComma,
	diffConfig,
	generateRandomInt,
	parseBooleanConfig,
} from "@shared/utils";
import logger from "@shared/utils/logger";
import {
	convertTrackerDataToLine,
	reduceTrackerString,
} from "@shared/utils/tracker";
import { invoke } from "@tauri-apps/api/core";
import { cloneDeep, isEmpty } from "lodash";
import api, { type CookieEntryView } from "@/api";
import SelectDirectory from "@/components/Native/SelectDirectory.vue";
import ShowInFolder from "@/components/Native/ShowInFolder.vue";
import CredentialManager from "@/components/Preference/CredentialManager.vue";
import UiButton from "@/components/ui/compat/UiButton.vue";
import UiTooltip from "@/components/ui/compat/UiTooltip.vue";
import { confirm } from "@/components/ui/confirm-dialog";
import { Input } from "@/components/ui/input";
import NumberInput from "@/components/ui/NumberInput.vue";
import {
	Popover,
	PopoverContent,
	PopoverTrigger,
} from "@/components/ui/popover";
import {
	Select,
	SelectContent,
	SelectItem,
	SelectTrigger,
	SelectValue,
} from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import is from "@/shims/platform";
import { usePreferenceStore } from "@/store/preference";
import { useTaskStore } from "@/store/task";
import { copyText } from "@/utils/clipboard";
import {
	checkForUpdates,
	isDesktopUpdaterAvailable,
	updaterState,
} from "@/utils/updater";

const resolveDohProvider = (storedProvider, storedUrl) => {
	const url = `${storedUrl || ""}`.trim();
	if (url) {
		for (const [name, preset] of Object.entries(DOH_PROVIDERS)) {
			if (preset.url && preset.url === url) {
				return name;
			}
		}
		if (
			storedProvider &&
			storedProvider in DOH_PROVIDERS &&
			storedProvider !== "custom"
		) {
			return storedProvider;
		}
		return "custom";
	}
	if (storedProvider && storedProvider in DOH_PROVIDERS) {
		return storedProvider;
	}
	return "cloudflare";
};

const initForm = (config) => {
	const {
		autoCheckUpdate,
		autoSyncTracker,
		engineOverridesText,
		engineOverrides,
		externalEngineEnabled,
		externalEngineHost,
		externalEnginePort,
		externalEngineSecret,
		btTracker,
		btMaxPeersPerTorrent,
		btMaxOutstandingPerPeer,
		btMaxConnections,
		btBanCorruptPeers,
		btBanCorruptStrikes,
		btEnableUpnp,
		btUpnpLease,
		btEnableLsd,
		btCreateSubfolder,
		btEncryptionPolicy,
		btListenV6,
		connectTimeout,
		nzbBodyTimeout,
		lowestSpeedLimit,
		lowestSpeedLimitTimeout,
		fileAllocation,
		uriSelector,
		loadCookies,
		netrcPath,
		noNetrc,
		dhtListenPort,
		ed2KEnableKad,
		ed2KKadPort,
		ed2kEnableKad,
		ed2kKadPort,
		dohEnable,
		dohUrl,
		dohBootstrap,
		dohFallback,
		dohProvider,
		ed2kPort,
		ed2kServer,
		giftEnabled,
		giftHost,
		giftPort,
		lastSyncTrackerTime,
		listenPort,
		logDirOverride,
		logLevel,
		protocols,
		proxy,
		rpcListenPort,
		rpcSecret,
		pbhEnable,
		pbhListenPort,
		pbhRpcSecret,
		trackerSource,
		userAgent,
		completionScriptEnabled,
		completionScriptCommand,
		completionScriptArgs,
		completionScriptTimeoutMs,
	} = config;
	const m3u8OutputFormat = config.m3U8OutputFormat ?? config.m3u8OutputFormat;
	const pendingEngineOverridesText =
		typeof engineOverridesText === "string"
			? engineOverridesText
			: JSON.stringify(engineOverrides || {}, null, 2);
	const result = {
		autoCheckUpdate: parseBooleanConfig(autoCheckUpdate, false),
		autoSyncTracker: parseBooleanConfig(autoSyncTracker),
		engineOverridesText: pendingEngineOverridesText,
		externalEngineEnabled: parseBooleanConfig(externalEngineEnabled, false),
		externalEngineHost: externalEngineHost || ENGINE_RPC_HOST,
		externalEnginePort: externalEnginePort || ENGINE_RPC_PORT,
		externalEngineSecret: externalEngineSecret || "",
		btTracker: convertCommaToLine(btTracker),
		btMaxPeersPerTorrent: btMaxPeersPerTorrent ?? 100,
		btMaxOutstandingPerPeer: btMaxOutstandingPerPeer ?? 0,
		btMaxConnections: btMaxConnections ?? 400,
		btBanCorruptPeers: parseBooleanConfig(btBanCorruptPeers, true),
		btBanCorruptStrikes: btBanCorruptStrikes ?? 3,
		btEnableUpnp: parseBooleanConfig(btEnableUpnp, true),
		btUpnpLease: btUpnpLease ?? 300,
		btEnableLsd: parseBooleanConfig(btEnableLsd, true),
		btCreateSubfolder: parseBooleanConfig(btCreateSubfolder, true),
		btEncryptionPolicy: btEncryptionPolicy || "prefer",
		btListenV6: parseBooleanConfig(btListenV6, false),
		connectTimeout: connectTimeout ?? 60,
		nzbBodyTimeout: nzbBodyTimeout ?? 30,
		lowestSpeedLimit: Math.round((Number(lowestSpeedLimit) || 0) / 1024),
		lowestSpeedLimitTimeout: lowestSpeedLimitTimeout ?? 30,
		fileAllocation: fileAllocation || "falloc",
		uriSelector: uriSelector || "feedback",
		loadCookies: loadCookies || "",
		netrcPath: netrcPath || "",
		noNetrc: parseBooleanConfig(noNetrc, false),
		dhtListenPort,
		ed2kEnableKad: parseBooleanConfig(ed2KEnableKad ?? ed2kEnableKad, true),
		ed2kKadPort: normalizePortValue(ed2KKadPort ?? ed2kKadPort, 4672),
		dohEnable: parseBooleanConfig(dohEnable, false),
		dohUrl: dohUrl || "",
		dohBootstrap: dohBootstrap || "",
		dohFallback: parseBooleanConfig(dohFallback, true),
		dohProvider: resolveDohProvider(dohProvider, dohUrl),
		giftEnabled: parseBooleanConfig(giftEnabled, false),
		giftHost: `${giftHost ?? ""}`.trim() || "127.0.0.1",
		giftPort: normalizePortValue(giftPort, 1213),
		ed2kPort: (ed2kPort ?? config.ed2KPort) || 4662,
		ed2kServer: convertCommaToLine(
			(ed2kServer ?? config.ed2KServer) || DEFAULT_ED2K_SERVERS,
		),
		ftpUser: config.ftpUser || "",
		ftpPasswd: config.ftpPasswd || "",
		sftpPrivateKey: config.sftpPrivateKey || "",
		sftpKeyPassphrase: config.sftpPrivateKeyPassphrase || "",
		mediaFormat:
			config.mediaFormat ??
			config["media-format"] ??
			config.youtubeFormat ??
			config["youtube-format"] ??
			"",
		lastSyncTrackerTime,
		listenPort,
		logDirOverride: typeof logDirOverride === "string" ? logDirOverride : "",
		logLevel,
		m3u8OutputFormat: m3u8OutputFormat || "ts",
		proxy: normalizeProxyConfig(proxy),
		protocols: {
			magnet: parseBooleanConfig(protocols?.magnet, true),
			thunder: parseBooleanConfig(protocols?.thunder, false),
			ed2k: parseBooleanConfig(protocols?.ed2k, true),
			adc: parseBooleanConfig(protocols?.adc, false),
			gnutella: parseBooleanConfig(protocols?.gnutella, false),
			g2: parseBooleanConfig(protocols?.g2, false),
		},
		rpcListenPort,
		rpcSecret,
		pbhEnable: parseBooleanConfig(pbhEnable, false),
		pbhListenPort: pbhListenPort || ENGINE_PBH_RPC_PORT,
		pbhRpcSecret: pbhRpcSecret || "",
		trackerSource: Array.isArray(trackerSource) ? [...trackerSource] : [],
		userAgent,
		completionScriptEnabled: parseBooleanConfig(completionScriptEnabled, false),
		completionScriptCommand: completionScriptCommand || "",
		completionScriptArgs: completionScriptArgs || "",
		completionScriptTimeoutMs: completionScriptTimeoutMs ?? 30000,
	};
	return result;
};

const ENGINE_OVERRIDES_MAX_KEYS = 64;
const ENGINE_OVERRIDES_MAX_KEY_LENGTH = 64;
const ENGINE_OVERRIDES_MAX_VALUE_LENGTH = 2048;

const isEngineOverrideValue = (value) =>
	value === null ||
	typeof value === "boolean" ||
	(typeof value === "number" && Number.isFinite(value)) ||
	(typeof value === "string" &&
		value.length <= ENGINE_OVERRIDES_MAX_VALUE_LENGTH);

const validateEngineOverrideBounds = (input) => {
	const entries = Object.entries(input || {});
	if (entries.length > ENGINE_OVERRIDES_MAX_KEYS) {
		return false;
	}
	return entries.every(
		([key, value]) =>
			key.length <= ENGINE_OVERRIDES_MAX_KEY_LENGTH &&
			isEngineOverrideValue(value),
	);
};

const sanitizeEngineOverrides = (input) => {
	const reservedKeys = new Set(["rpc-host"]);
	const filtered = {};
	const droppedKeys = [];
	for (const [key, value] of Object.entries(input || {})) {
		const normalized = `${key}`.toLowerCase();
		if (
			reservedKeys.has(normalized) ||
			normalized.startsWith("rpc-") ||
			normalized.endsWith("-port") ||
			normalized.endsWith("-secret")
		) {
			droppedKeys.push(key);
			continue;
		}
		filtered[key] = value;
	}
	return { filtered, droppedKeys };
};

const normalizePortValue = (value, fallback) => {
	const normalized = `${value ?? ""}`.trim();
	if (!normalized) {
		return fallback;
	}

	if (!/^\d+$/.test(normalized)) {
		return fallback;
	}

	const parsed = Number.parseInt(normalized, 10);
	if (!Number.isInteger(parsed) || parsed < 1 || parsed > 65535) {
		return fallback;
	}

	return parsed;
};

const ADVANCED_BOOLEAN_KEYS = [
	"autoCheckUpdate",
	"autoSyncTracker",
	"externalEngineEnabled",
	"completionScriptEnabled",
	"ed2kEnableKad",
	"giftEnabled",
	"pbhEnable",
];

const ADVANCED_NUMERIC_KEYS = [
	"ed2kKadPort",
	"giftPort",
	"btMaxPeersPerTorrent",
	"btMaxOutstandingPerPeer",
	"btMaxConnections",
	"btBanCorruptStrikes",
	"btUpnpLease",
	"connectTimeout",
	"nzbBodyTimeout",
	"lowestSpeedLimit",
	"lowestSpeedLimitTimeout",
	"pbhListenPort",
];

const effectiveP2pProxyProfile = (proxy: unknown) => {
	const { p2p } = normalizeProxyConfig(proxy);
	if (!p2p.enable) {
		return {
			server: "",
			bypass: "",
			udp: { server: "", bypass: "" },
		};
	}
	const server = p2p.server;
	const bypass = server ? p2p.bypass : "";
	const udpServer = p2p.udp.server || server;
	const udpBypass = p2p.udp.server ? p2p.udp.bypass : bypass;
	return {
		server,
		bypass,
		udp: { server: udpServer, bypass: udpBypass },
	};
};

const p2pProxyProfileChanged = (before: unknown, after: unknown): boolean =>
	JSON.stringify(effectiveP2pProxyProfile(before)) !==
	JSON.stringify(effectiveP2pProxyProfile(after));

const normalizeAdvancedConfig = (data, rpcDefaultPort) => {
	for (const key of ADVANCED_BOOLEAN_KEYS) {
		if (key in data) {
			data[key] = !!data[key];
		}
	}

	if ("protocols" in data) {
		const protocols = data.protocols || {};
		data.protocols = {
			magnet: !!protocols.magnet,
			thunder: !!protocols.thunder,
			ed2k: !!protocols.ed2k,
			adc: !!protocols.adc,
			gnutella: !!protocols.gnutella,
			g2: !!protocols.g2,
		};
	}

	if ("sftpKeyPassphrase" in data) {
		data.sftpPrivateKeyPassphrase = data.sftpKeyPassphrase;
		delete data.sftpKeyPassphrase;
	}

	if ("mediaFormat" in data) {
		data["media-format"] = data.mediaFormat;
		data["youtube-format"] = data.mediaFormat;
		delete data.mediaFormat;
	}

	if (data.btTracker) {
		data.btTracker = reduceTrackerString(convertLineToComma(data.btTracker));
	}

	for (const key of ADVANCED_NUMERIC_KEYS) {
		if (key in data) {
			const raw = data[key];
			if (raw === "" || raw === null || raw === undefined) {
				if (key === "ed2kKadPort") {
					data[key] = 4672;
					continue;
				}
				if (key === "giftPort") {
					data[key] = 1213;
					continue;
				}
				if (key === "pbhListenPort") {
					data[key] = ENGINE_PBH_RPC_PORT;
					continue;
				}
				delete data[key];
				continue;
			}
			const n = Number(raw);
			let ok: boolean;
			if (key === "ed2kKadPort" || key === "giftPort") {
				ok = Number.isInteger(n) && n >= 1 && n <= 65535;
			} else if (key === "pbhListenPort") {
				ok = Number.isInteger(n) && n >= 1 && n <= 65535;
			} else if (key === "btMaxConnections") {
				ok = Number.isInteger(n) && n >= 20 && n <= 5000;
			} else if (key === "btBanCorruptStrikes") {
				ok = Number.isInteger(n) && n >= 1 && n <= 100;
			} else if (key === "btUpnpLease") {
				ok = Number.isFinite(n) && n >= 60 && n <= 86400;
			} else if (key === "connectTimeout") {
				ok = Number.isFinite(n) && n >= 1 && n <= 600;
			} else if (key === "nzbBodyTimeout") {
				ok = Number.isFinite(n) && n >= 1 && n <= 3600;
			} else if (key === "lowestSpeedLimitTimeout") {
				ok = Number.isFinite(n) && n >= 1 && n <= 3600;
			} else if (key === "lowestSpeedLimit") {
				ok = Number.isFinite(n) && n >= 0 && n <= 1048576;
			} else {
				ok = Number.isFinite(n) && n >= 0;
			}
			if (ok) {
				data[key] = n;
			} else if (key === "ed2kKadPort") {
				data[key] = 4672;
			} else if (key === "giftPort") {
				data[key] = 1213;
			} else if (key === "pbhListenPort") {
				data[key] = ENGINE_PBH_RPC_PORT;
			} else {
				delete data[key];
			}
		}
	}

	if ("lowestSpeedLimit" in data && typeof data.lowestSpeedLimit === "number") {
		data.lowestSpeedLimit = data.lowestSpeedLimit * 1024;
	}

	if (data.ed2kServer !== undefined) {
		data.ed2kServer = convertLineToComma(data.ed2kServer);
	}

	if ("giftHost" in data) {
		data.giftHost = `${data.giftHost ?? ""}`.trim() || "127.0.0.1";
	}

	if (data.rpcListenPort === "") {
		data.rpcListenPort = rpcDefaultPort;
	}

	if (data.externalEnginePort !== undefined) {
		data.externalEnginePort = normalizePortValue(
			data.externalEnginePort,
			rpcDefaultPort,
		);
	}

	if ("externalEngineHost" in data) {
		const host = `${data.externalEngineHost || ""}`.trim();
		data.externalEngineHost = host || ENGINE_RPC_HOST;
	}

	return data;
};

export default {
	name: "preference-advanced",
	components: {
		[UiButton.name]: UiButton,
		"ui-tooltip": UiTooltip,
		[ShowInFolder.name]: ShowInFolder,
		[SelectDirectory.name]: SelectDirectory,
		[CredentialManager.name]: CredentialManager,
		Input,
		NumberInput,
		Textarea,
		Select,
		SelectContent,
		SelectItem,
		SelectTrigger,
		SelectValue,
		Popover,
		PopoverContent,
		PopoverTrigger,
		RefreshCw,
		Radio,
		Cable,
		Network,
		Link,
		UserCircle,
		Code,
		Cookie,
		FileKey,
		Globe,
		FileText,
		KeyRound,
		ScrollText,
		AlertTriangle,
		Settings,
		Server,
		Shield,
		Terminal,
		X,
		ChevronDown,
		Check,
		Download,
		RefreshCcw,
		Dices,
		ExternalLink,
		Copy,
		Video,
	},
	data() {
		const preferenceStore = usePreferenceStore();
		if (!updaterState.lastCheckedAt) {
			updaterState.lastCheckedAt = Number(
				preferenceStore.config.lastCheckUpdateTime || 0,
			);
		}
		const formOriginal = initForm(preferenceStore.config);
		const form = initForm({ ...formOriginal, ...changedConfig.advanced });

		return {
			form,
			formOriginal,
			updaterState,
			hideRpcSecret: true,
			hidePbhRpcSecret: true,
			hideExternalRpcSecret: true,
			proxyScopeOptions: PROXY_SCOPE_OPTIONS,
			dohProviderOptions: DOH_PROVIDER_OPTIONS,
			trackerSourceOptions: TRACKER_SOURCE_OPTIONS,
			trackerSourceOpen: false,
			trackerSyncing: false,
			completionScriptTesting: false,
			completionScriptTestResult: "",
			cookieEntries: [] as CookieEntryView[],
			rpcSecretTimer: null as ReturnType<typeof setTimeout> | null,
			pbhRpcSecretTimer: null as ReturnType<typeof setTimeout> | null,
		};
	},
	watch: {
		"form.dohProvider"(provider: string) {
			if (provider && provider !== "custom" && provider in DOH_PROVIDERS) {
				this.form.dohBootstrap = DOH_PROVIDERS[provider].bootstrap || "";
			}
		},
	},
	computed: {
		isRenderer: () => is.renderer(),
		isDesktopUpdater: () => isDesktopUpdaterAvailable(),
		updaterBusy() {
			return ["checking", "downloading", "installing"].includes(
				this.updaterState.status,
			);
		},
		updaterStatusLabel() {
			if (this.updaterState.status === "error") {
				return this.$t("app.update-error", {
					error: this.updaterState.error || this.$t("app.update-error-message"),
				});
			}
			const keyByStatus = {
				checking: "app.update-checking",
				downloading: "app.update-download",
				installing: "app.update-install",
				"ready-to-install": "app.update-install",
				"up-to-date": "app.update-unavailable",
				available: "app.update-available",
				cancelled: "app.update-cancelled",
				idle: "",
			};
			const key = keyByStatus[this.updaterState.status] || "";
			return key ? this.$t(key) : "";
		},
		lastCheckedUpdateLabel() {
			const value = Number(
				this.updaterState.lastCheckedAt ||
					this.formOriginal?.lastCheckUpdateTime ||
					0,
			);
			return value > 0 ? new Date(value).toLocaleString() : "";
		},
		rpcDefaultPort() {
			return ENGINE_RPC_PORT;
		},
		pbhDefaultPort() {
			return ENGINE_PBH_RPC_PORT;
		},
		externalRpcDefaultHost() {
			return ENGINE_RPC_HOST;
		},
		logLevels() {
			return LOG_LEVELS;
		},
		logPath() {
			return usePreferenceStore().config.appLogPath;
		},
		visibleLogPath() {
			return `${this.form.logDirOverride || this.logPath || ""}`.trim();
		},
		currentRpcUrl() {
			const host = this.form.externalEngineEnabled
				? this.form.externalEngineHost
				: ENGINE_RPC_HOST;
			const port = this.form.externalEngineEnabled
				? this.form.externalEnginePort
				: this.form.rpcListenPort;
			const secret = this.form.externalEngineEnabled
				? this.form.externalEngineSecret
				: this.form.rpcSecret;
			return buildRpcUrl({ host, port, secret });
		},
	},
	methods: {
		checkForUpdatesNow() {
			void checkForUpdates("manual");
		},
		setAdvancedBoolean(key, enable) {
			this.form[key] = !!enable;
		},
		handleLogDirSelected(dir: string) {
			this.form.logDirOverride = `${dir || ""}`.trim();
		},
		copyRpcUrlToClipboard() {
			copyText(this.currentRpcUrl).catch(() => {});
		},
		onExternalEngineHostBlur() {
			const host = `${this.form.externalEngineHost || ""}`.trim();
			this.form.externalEngineHost = host || ENGINE_RPC_HOST;
		},
		onExternalEnginePortBlur() {
			this.form.externalEnginePort = normalizePortValue(
				this.form.externalEnginePort,
				this.rpcDefaultPort,
			);
		},
		onGiftHostBlur() {
			this.form.giftHost = `${this.form.giftHost ?? ""}`.trim() || "127.0.0.1";
		},
		onGiftPortBlur() {
			this.form.giftPort = normalizePortValue(this.form.giftPort, 1213);
		},
		onEd2kKadPortBlur() {
			this.form.ed2kKadPort = normalizePortValue(this.form.ed2kKadPort, 4672);
		},
		getTrackerLabel(value) {
			for (const group of this.trackerSourceOptions) {
				for (const item of group.options) {
					if (item.value === value) {
						return item.label;
					}
				}
			}
			return value;
		},
		toggleTrackerSource(value) {
			const idx = this.form.trackerSource.indexOf(value);
			if (idx >= 0) {
				this.form.trackerSource.splice(idx, 1);
			} else {
				this.form.trackerSource.push(value);
			}
		},
		async onTestCompletionScript() {
			const command = `${this.form.completionScriptCommand || ""}`.trim();
			if (!command) {
				this.$msg.error(this.$t("preferences.completion-script-empty"));
				return;
			}
			this.completionScriptTesting = true;
			this.completionScriptTestResult = "";
			try {
				const result = await invoke<{
					success: boolean;
					exit_code: number | null;
					stdout: string;
					stderr: string;
					duration_ms: number;
					timed_out: boolean;
					message: string | null;
				}>("test_completion_script", {
					command,
					args: this.form.completionScriptArgs || "",
					timeoutMs: Number(this.form.completionScriptTimeoutMs) || 30000,
				});
				const lines = [
					`exit=${result.exit_code ?? "n/a"} duration=${result.duration_ms}ms timed_out=${result.timed_out}`,
				];
				if (result.message) {
					lines.push(`message: ${result.message}`);
				}
				if (result.stdout) {
					lines.push(`stdout:\n${result.stdout}`);
				}
				if (result.stderr) {
					lines.push(`stderr:\n${result.stderr}`);
				}
				this.completionScriptTestResult = lines.join("\n");
				if (result.success) {
					this.$msg.success(this.$t("preferences.completion-script-test-ok"));
				} else {
					this.$msg.error(this.$t("preferences.completion-script-test-fail"));
				}
			} catch (err: unknown) {
				this.completionScriptTestResult = `${(err as Error)?.message || err}`;
				this.$msg.error(this.$t("preferences.completion-script-test-fail"));
			} finally {
				this.completionScriptTesting = false;
			}
		},
		syncTrackerFromSource() {
			this.trackerSyncing = true;
			const { trackerSource } = this.form;
			usePreferenceStore()
				.fetchBtTracker(trackerSource)
				.then((data) => {
					const tracker = convertTrackerDataToLine(data);
					this.form.lastSyncTrackerTime = Date.now();
					this.form.btTracker = tracker;
					this.trackerSyncing = false;
				})
				.catch((_) => {
					this.trackerSyncing = false;
				});
		},
		onProtocolsChange(protocol, enabled) {
			const { protocols } = this.form;
			this.form.protocols = {
				...protocols,
				[protocol]: !!enabled,
			};
		},
		onHttpProxyEnableChange(enable) {
			this.form.proxy = {
				...this.form.proxy,
				http: { ...this.form.proxy.http, enable: !!enable },
			};
		},
		onP2pProxyEnableChange(enable) {
			this.form.proxy = {
				...this.form.proxy,
				p2p: { ...this.form.proxy.p2p, enable: !!enable },
			};
		},
		onProxyScopeToggle(item, checked) {
			const isChecked = !!checked;
			const scope = [...this.form.proxy.http.scope];
			const idx = scope.indexOf(item);
			if (isChecked && idx < 0) {
				scope.push(item);
			} else if (!isChecked && idx >= 0) {
				scope.splice(idx, 1);
			}
			this.form.proxy = {
				...this.form.proxy,
				http: { ...this.form.proxy.http, scope },
			};
		},
		changeUA(type) {
			const ua = userAgentMap[type];
			if (!ua) {
				return;
			}
			this.form.userAgent = ua;
		},
		rollPort(field, min, max) {
			this.form[field] = generateRandomInt(min, max);
		},
		onRpcListenPortBlur() {
			if ("" === this.form.rpcListenPort || !this.form.rpcListenPort) {
				this.form.rpcListenPort = this.rpcDefaultPort;
			}
		},
		onRpcSecretDiceClick() {
			this.hideRpcSecret = false;
			this.form.rpcSecret = crypto.randomUUID().replaceAll("-", "");

			if (this.rpcSecretTimer) {
				clearTimeout(this.rpcSecretTimer);
			}
			this.rpcSecretTimer = setTimeout(() => {
				this.hideRpcSecret = true;
				this.rpcSecretTimer = null;
			}, 2000);
		},
		onPbhListenPortBlur() {
			if ("" === this.form.pbhListenPort || !this.form.pbhListenPort) {
				this.form.pbhListenPort = this.pbhDefaultPort;
			}
		},
		onPbhRpcSecretDiceClick() {
			this.hidePbhRpcSecret = false;
			this.form.pbhRpcSecret = crypto.randomUUID().replaceAll("-", "");

			if (this.pbhRpcSecretTimer) {
				clearTimeout(this.pbhRpcSecretTimer);
			}
			this.pbhRpcSecretTimer = setTimeout(() => {
				this.hidePbhRpcSecret = true;
				this.pbhRpcSecretTimer = null;
			}, 2000);
		},
		async onSessionResetClick() {
			const { confirmed } = await confirm({
				message: this.$t("preferences.session-reset-confirm"),
				title: this.$t("preferences.session-reset"),
				kind: "warning",
				confirmText: this.$t("app.yes"),
				cancelText: this.$t("app.no"),
			});
			if (confirmed) {
				const taskStore = useTaskStore();
				taskStore.purgeTaskRecord();
				taskStore.pauseAllTask().then(() => {
					invoke("reset_session").catch(() => {});
				});
			}
		},
		async onFactoryResetClick() {
			const { confirmed } = await confirm({
				message: this.$t("preferences.factory-reset-confirm"),
				title: this.$t("preferences.factory-reset"),
				kind: "warning",
				confirmText: this.$t("app.yes"),
				cancelText: this.$t("app.no"),
			});
			if (confirmed) {
				invoke("factory_reset").catch(() => {});
			}
		},
		syncFormConfig() {
			usePreferenceStore()
				.fetchPreference()
				.then((config) => {
					this.form = initForm(config);
					this.formOriginal = cloneDeep(this.form);
				});
		},
		async submitForm(_formName) {
			const data = {
				...diffConfig(this.formOriginal, this.form),
				...changedConfig.basic,
			};
			const p2pProxyChanged =
				"proxy" in data &&
				p2pProxyProfileChanged(this.formOriginal.proxy, this.form.proxy);
			if (p2pProxyChanged && data.proxy && typeof data.proxy === "object") {
				data.proxy = {
					...(data.proxy as Record<string, unknown>),
					"p2p-profile-explicit": true,
				};
			}

			if ("engineOverridesText" in data) {
				const raw = `${this.form.engineOverridesText || ""}`.trim();
				if (!raw) {
					data.engineOverrides = {};
				} else {
					try {
						const parsed = JSON.parse(raw);
						if (
							!parsed ||
							typeof parsed !== "object" ||
							Array.isArray(parsed)
						) {
							this.$msg.error(this.$t("preferences.engine-overrides-invalid"));
							return;
						}
						const { filtered, droppedKeys } = sanitizeEngineOverrides(parsed);
						if (!validateEngineOverrideBounds(filtered)) {
							this.$msg.error(
								this.$t("preferences.engine-overrides-too-large"),
							);
							return;
						}
						data.engineOverrides = filtered;
						if (droppedKeys.length) {
							this.$msg.warning(
								this.$t("preferences.engine-overrides-reserved-keys", {
									keys: droppedKeys.join(", "),
								}),
							);
						}
					} catch {
						this.$msg.error(this.$t("preferences.engine-overrides-invalid"));
						return;
					}
				}
				delete data.engineOverridesText;
			}

			if (
				"dohProvider" in data ||
				"dohEnable" in data ||
				"dohUrl" in data ||
				"dohBootstrap" in data
			) {
				const provider = this.form.dohProvider;
				if (provider && provider !== "custom" && provider in DOH_PROVIDERS) {
					const preset = DOH_PROVIDERS[provider];
					data.dohUrl = preset.url;
					const userBootstrap = `${this.form.dohBootstrap || ""}`.trim();
					if (!userBootstrap || userBootstrap === preset.bootstrap) {
						data.dohBootstrap = preset.bootstrap;
					} else {
						data.dohBootstrap = userBootstrap;
					}
				} else {
					data.dohUrl = `${this.form.dohUrl || ""}`.trim();
					data.dohBootstrap = `${this.form.dohBootstrap || ""}`.trim();
				}
				if (
					this.form.dohEnable &&
					provider === "custom" &&
					!/^https:\/\//i.test(data.dohUrl)
				) {
					this.$msg.error(this.$t("preferences.doh-url-invalid"));
					return;
				}
			}

			if ("ed2kKadPort" in data) {
				const raw = data.ed2kKadPort;
				if (raw !== "" && raw !== null && raw !== undefined) {
					const n = Number(raw);
					if (!(Number.isInteger(n) && n >= 1 && n <= 65535)) {
						this.$msg.error(this.$t("preferences.ed2k-kad-port-invalid"));
						return;
					}
				}
			}

			normalizeAdvancedConfig(data, this.rpcDefaultPort);

			if (p2pProxyChanged) {
				const { confirmed } = await confirm({
					title: this.$t("preferences.proxy-p2p-restart-title"),
					message: this.$t("preferences.proxy-p2p-restart-confirm"),
					kind: "warning",
					confirmText: this.$t("app.yes"),
					cancelText: this.$t("app.no"),
				});
				if (!confirmed) {
					return;
				}
			}

			{
				const safeData = { ...data };
				if ("externalEngineSecret" in safeData) {
					safeData.externalEngineSecret = "[REDACTED]";
				}
				if (
					safeData.engineOverrides &&
					typeof safeData.engineOverrides === "object"
				) {
					safeData.engineOverrides = Object.fromEntries(
						Object.keys(safeData.engineOverrides).map((k) => [k, "[REDACTED]"]),
					);
				}
				logger.log(
					"[Risuko] preference changed data:",
					redactProxySettings(safeData as Record<string, unknown>),
				);
			}

			usePreferenceStore()
				.save(data)
				.then(() => {
					this.syncFormConfig();
					this.$msg.success(this.$t("preferences.save-success-message"));
					changedConfig.basic = {};
					changedConfig.advanced = {};
				})
				.catch((_e) => {
					this.$msg.error(this.$t("preferences.save-fail-message"));
				});
		},
		resetForm(_formName) {
			this.syncFormConfig();
		},
		async refreshCookieEntries() {
			try {
				this.cookieEntries = await api.listCookieEntries();
			} catch (_e) {
				this.cookieEntries = [];
			}
		},
		async handleDeleteCookieEntry(host: string) {
			try {
				await api.deleteCookieEntry(host);
				await this.refreshCookieEntries();
			} catch (e) {
				this.$msg.error(String(e));
			}
		},
		async handleClearAllCookieEntries() {
			const { confirmed } = await confirm({
				message: this.$t("preferences.saved-cookies-confirm-clear"),
				title: this.$t("preferences.saved-cookies-clear"),
				kind: "warning",
				confirmText: this.$t("app.yes"),
				cancelText: this.$t("app.no"),
			});
			if (!confirmed) {
				return;
			}
			try {
				await api.clearCookieEntries();
				await this.refreshCookieEntries();
			} catch (e) {
				this.$msg.error(String(e));
			}
		},
		formatTimestamp(seconds: number): string {
			if (!seconds) {
				return "-";
			}
			const d = new Date(seconds * 1000);
			return d.toLocaleString();
		},
	},
	mounted() {
		this.refreshCookieEntries();
	},
	beforeUnmount() {
		if (this.rpcSecretTimer) {
			clearTimeout(this.rpcSecretTimer);
			this.rpcSecretTimer = null;
		}
		if (this.pbhRpcSecretTimer) {
			clearTimeout(this.pbhRpcSecretTimer);
			this.pbhRpcSecretTimer = null;
		}
	},
	async beforeRouteLeave(to, _from) {
		const advancedChanges = normalizeAdvancedConfig(
			diffConfig(this.formOriginal, this.form),
			this.rpcDefaultPort,
		);
		if (
			"proxy" in advancedChanges &&
			p2pProxyProfileChanged(this.formOriginal.proxy, this.form.proxy) &&
			advancedChanges.proxy &&
			typeof advancedChanges.proxy === "object"
		) {
			advancedChanges.proxy = {
				...(advancedChanges.proxy as Record<string, unknown>),
				"p2p-profile-explicit": true,
			};
		}
		changedConfig.advanced = advancedChanges;
		if (to.path === "/preference/basic") {
			return true;
		}
		if (isEmpty(changedConfig.basic) && isEmpty(changedConfig.advanced)) {
			return true;
		}
		const { confirmed } = await confirm({
			message: this.$t("preferences.not-saved-confirm"),
			title: this.$t("preferences.not-saved"),
			kind: "warning",
			confirmText: this.$t("app.yes"),
			cancelText: this.$t("app.no"),
		});
		if (confirmed) {
			changedConfig.basic = {};
			changedConfig.advanced = {};
			return true;
		}
		return false;
	},
};
</script>

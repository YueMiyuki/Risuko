# Agent guide for Risuko

Risuko is a cross-platform download manager: a Rust engine with a Tauri v2 shell and a Vue 3 renderer. Desktop (macOS, Windows, Linux) and Android share the renderer.

## Layout

- `src/renderer/` — Vue 3 + Pinia UI. `src/shared/` — TypeScript shared by the renderer and its tests, including locales under `src/shared/locales/<tag>/`.
- `src-tauri/` — Cargo workspace.
  - `risuko-engine` — download engine, task manager, JSON-RPC server.
  - `risuko-bt` — BitTorrent. `risuko-ed2k` — eD2K and Kad. `risuko-http` — HTTP client. `risuko-util` — rate limiter and filename helpers.
  - `risuko-tauri` — the desktop and Android shell: Tauri commands, tray, menus. New application code goes here.
  - `risuko-cli` — standalone CLI. `risuko-napi` — Node.js bindings. `risuko-cookies` — browser cookie import. `risuko-share` — peer-to-peer file sharing. `risuko-webview-upgrade` — Android WebView helper.
  - `src-tauri/src` — the `risuko` package: entry points only.
- `packages/` — npm wrapper packages. `scripts/` — release and maintenance scripts.
- `.github/workflows/` — CI and release automation.

## Commands

From the repository root:

| Purpose | Command |
|---|---|
| Install | `pnpm install` |
| Run the app | `pnpm dev` |
| Typecheck | `pnpm typecheck` |
| JavaScript tests | `pnpm test` |
| Lint and format | `pnpm fmt` (writes) or `pnpm exec biome check` (reports) |
| Everything CI checks | `pnpm verify` |

From `src-tauri/`:

| Purpose | Command |
|---|---|
| Rust tests | `cargo test --workspace --locked` |
| One crate | `cargo test -p risuko-engine --locked` |
| Lint | `cargo clippy --workspace --locked` |
| Format check | `cargo fmt --all --check` |

Android builds go through `pnpm android:build:debug`; see `README.md`.

## Conventions

- Comments: write almost none. No doc comments on items, no explanatory headers, none in `.vue` or `.css` files, none in workflow YAML. Keep functional ones (`// SAFETY:`, tool directives, clap help text, `#[napi]` docs). A one-line note is fine where removing it would likely let a bug come back. Comments do not end with a full stop.
- Rust: `cargo fmt` formatting. Unit tests sit at the bottom of the file they test in `#[cfg(test)] mod tests`; integration tests are in each crate's `tests/`.
- TypeScript and Vue: Biome formatting (tabs, double quotes). Tests use `node:test`, sit next to the module as `*.test.ts`, and import with a relative path that includes `.ts`.
- User-visible strings come from the locale files; `en-US` is the reference. A new string goes into `en-US`, `zh-CN` and `zh-TW` together, and `pnpm check:locales` verifies it.
- A new setting name goes into `src/shared/configKeys.json` (under `user` or `system`) and, when it has a default, into `src-tauri/risuko-engine/src/config/defaults.rs`; `pnpm check:config-keys` verifies the lists agree.
- Every Tauri plugin is a direct dependency of both the `risuko` package and `risuko-tauri`, with versions in `[workspace.dependencies]`. In `src-tauri/Cargo.toml`, the `tauri` dependency of the `risuko` package lists its features literally; do not change it to `workspace = true`.
- The release profile keeps fat LTO and one codegen unit.
- Operations that would let a JSON-RPC client read or run local files (creating torrents, registering shares, completion-script options) are deliberately not reachable over RPC. Keep it that way when adding RPC methods.
- The one opt-in is "Let RPC clients use any local path" (`rpc-allow-any-dir`, off by default and not settable over RPC): it lets RPC clients save outside the download folders and name local cookie, netrc, and SSH key files. A new RPC option that takes a local path must be refused without it, through the checks in `src-tauri/risuko-engine/src/engine/rpc_paths.rs`.

# Risuko

<p>
  <a href="https://risuko.app">
    <img src="./static/logo.svg" width="256" alt="Risuko App Icon" />
  </a>
</p>

## 一款全能的下载工具

![GitHub_Release](https://img.shields.io/github/v/release/yuemiyuki/risuko?include_prereleases&style=for-the-badge&link=https%3A%2F%2Fgithub.com%2FYueMiyuki%2FRisuko%2Freleases)
![Rust](https://img.shields.io/badge/Rust-black?style=for-the-badge&logo=rust&logoColor=#E57324)
![Vue](https://img.shields.io/badge/Vue%20js-35495E?style=for-the-badge&logo=vuedotjs&logoColor=4FC08D)
![Vite](https://img.shields.io/badge/Vite-B73BFE?style=for-the-badge&logo=vite&logoColor=FFD62E)

[English](./README.md) | 简体中文

Risuko 是一款全能的下载工具，支持下载 HTTP、FTP、BT、磁力链等资源。它的界面简洁易用，希望大家喜欢 👻。

✈️ 去 [官网](https://risuko.app) 逛逛

## 💽 安装

### Github 下载
从 [GitHub Releases](https://github.com/YueMiyuki/Risuko/releases) 下载并安装。Debian、Ubuntu 和 Fedora 请使用下方的 APT / DNF 软件源

### NPM 包
Risuko 提供 NPM 包，包含 UI 和 Risuko 引擎
```
pnpm install -g @risuko/app
risuko-app
```

或只安装CLI
```
pnpm install -g @risuko/cli
risuko --help
```

### Homebrew
Risuko 也可以通过 Homebrew 安装，使用以下命令：
```bash
# CLI
brew install yuemiyuki/risuko/risuko-cli

# 桌面应用 (Cask)
brew install --cask yuemiyuki/risuko/risuko-app
```

### WinGet
```powershell
# 桌面应用
winget install YueMiyuki.Risuko

# CLI
winget install YueMiyuki.Risuko.CLI
```

### APT (Debian / Ubuntu)
```bash
sudo curl -fsSLo /usr/share/keyrings/risuko.gpg https://dl.risuko.app/apt/risuko.gpg
echo "deb [signed-by=/usr/share/keyrings/risuko.gpg] https://dl.risuko.app/apt stable main" | sudo tee /etc/apt/sources.list.d/risuko.list
sudo apt update

# 桌面应用
sudo apt install risuko

# CLI（命令为 risuko-cli）
sudo apt install risuko-cli
```

### DNF (Fedora / RHEL)
```bash
sudo curl -fsSLo /etc/yum.repos.d/risuko.repo https://dl.risuko.app/rpm/risuko.repo

# 桌面应用
sudo dnf install risuko

# CLI（命令为 risuko-cli）
sudo dnf install risuko-cli
```

## 🖥 应用界面

![motrix-screenshot-task-cn.png](./static/readme/UI.png)

## ⌨️ 本地开发

### 克隆代码

```bash
git clone https://github.com/YueMiyuki/Risuko
```

### 安装依赖

需要 Node.js >= 22。

```bash
cd risuko
pnpm install
```

天朝大陆用户建议使用淘宝的 npm 源

```bash
npm config set registry 'https://registry.npmmirror.com'
```

### 开发模式

```bash
pnpm run dev
```

### 编译打包

```bash
pnpm run build
```

### Android

需要 Android SDK 36、build-tools 35.0.0、NDK 27.2.12479018，以及 Rust 的 Android 目标

```bash
# Debug 包，单 ABI（快一些）
pnpm android:build:debug

# Release 拆 ABI 打包，并签名
pnpm android:build:signed
```

签名相关的环境变量在 `scripts/sign-android-apks.mjs`：`ANDROID_SIGNING_KEYSTORE_PATH`（CI 用 `ANDROID_SIGNING_KEYSTORE_BASE64`）、`ANDROID_SIGNING_KEYSTORE_PASSWORD`、`ANDROID_SIGNING_KEY_ALIAS`，可选 `ANDROID_SIGNING_KEY_PASSWORD`

## 🛠 技术栈

- [Tauri v2](https://v2.tauri.app/)
- [Vue 3](https://vuejs.org/) + [Pinia](https://pinia.vuejs.org/) + [shadcn-vue](https://www.shadcn-vue.com/)
- [Vite](https://vite.dev/)
- [TypeScript](https://www.typescriptlang.org/)
- [Tailwind CSS](https://tailwindcss.com/)

## ☑️ TODO

请看Issue的roadmap


## 性能

Risuko 相比原始版本使用内存减少一半，CPU 使用率峰值也显著降低  
在 Next v0.1.0 中，由于 aria2 被替换为原生 Rust 代码，性能得到了优化  
所有数据都是在空闲状态下使用命令 `psrecord <PID> --plot memory.png --include-children --duration 60` 捕获  
应用信息由 Finder 提供  
相比 v0.4.0-alpha，Risuko v0.3.0 的 CPU 和内存使用量显著降低，包体积也更小
相比原始版本，Risuko v0.3.0 具有：
  - ~91% 的包体积减少（219.3 MB -> 19.6 MB）
  - ~70% 的内存使用量减少（四舍五入到最近的十位，~425MB -> ~125MB）
  - ~90% 的峰值 CPU 使用率降低（~145% -> ~15%）

| 原始版本 | Next | Risuko v0.3.0 |
| ------- | ---- | ----------- |
| ![orignal_mem](./static/readme/Original_Memory.png) | ![0.0.4_mem](./static/readme/v0.0.4_Memory.png) | ![0.3.0_mem](./static/readme/v0.3.0_Memory.png) |
| ![original_appinfo](./static/readme/Original_AppInfo.png) | ![0.0.4_appinfo](./static/readme/v0.0.4_AppInfo.png) | ![0.3.0_appinfo](./static/readme/v0.3.0_AppInfo.png) |

本数据通过 [psrecord](https://github.com/astrofrog/psrecord) 生成

## 🤝 参与共建 [![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat)](http://makeapullrequest.com)

如果你有兴趣参与共同开发，欢迎 FORK 和 PR。

## 🌍 国际化

欢迎大家将 Risuko 翻译成更多的语言版本 🧐，开工之前请先阅读一下 [翻译指南](./docs/CONTRIBUTING-CN.md#-翻译指南)。

| Key   | Name                | Status                                                                                                      |
| ----- | :------------------ | :---------------------------------------------------------------------------------------------------------- |
| ar    | Arabic              | ✔️ [@hadialqattan](https://github.com/hadialqattan), [@AhmedElTabarani](https://github.com/AhmedElTabarani) |
| bg    | Българският език    | ✔️ [@null-none](https://github.com/null-none)                                                               |
| ca    | Català              | ✔️ [@marcizhu](https://github.com/marcizhu)                                                                 |
| de    | Deutsch             | ✔️ [@Schloemicher](https://github.com/Schloemicher)                                                         |
| el    | Ελληνικά            | ✔️ [@Likecinema](https://github.com/Likecinema)                                                             |
| en-US | English             | ✔️                                                                                                          |
| es    | Español             | ✔️ [@Chofito](https://github.com/Chofito)                                                                   |
| fa    | فارسی               | ✔️ [@Nima-Ra](https://github.com/Nima-Ra)                                                                   |
| fr    | Français            | ✔️ [@gpatarin](https://github.com/gpatarin)                                                                 |
| hu    | Hungarian           | ✔️ [@zalnaRs](https://github.com/zalnaRs)                                                                   |
| id    | Indonesia           | ✔️ [@aarestu](https://github.com/aarestu)                                                                   |
| it    | Italiano            | ✔️ [@blackcat-917](https://github.com/blackcat-917)                                                         |
| ja    | 日本語              | ✔️ [@hbkrkzk](https://github.com/hbkrkzk)                                                                   |
| ko    | 한국어              | ✔️ [@KOZ39](https://github.com/KOZ39)                                                                       |
| nb    | Norsk Bokmål        | ✔️ [@rubjo](https://github.com/rubjo)                                                                       |
| nl    | Nederlands          | ✔️ [@nickbouwhuis](https://github.com/nickbouwhuis)                                                         |
| pl    | Polski              | ✔️ [@KanarekLife](https://github.com/KanarekLife)                                                           |
| pt-BR | Portuguese (Brazil) | ✔️ [@andrenoberto](https://github.com/andrenoberto)                                                         |
| ro    | Română              | ✔️ [@alyn3d](https://github.com/alyn3d)                                                                     |
| ru    | Русский             | ✔️ [@bladeaweb](https://github.com/bladeaweb)                                                               |
| th    | แบบไทย              | ✔️ [@nxanywhere](https://github.com/nxanywhere)                                                             |
| tr    | Türkçe              | ✔️ [@abdullah](https://github.com/abdullah)                                                                 |
| uk    | Українська          | ✔️ [@bladeaweb](https://github.com/bladeaweb)                                                               |
| vi    | Tiếng Việt          | ✔️ [@duythanhvn](https://github.com/duythanhvn)                                                             |
| zh-CN | 简体中文            | ✔️                                                                                                          |
| zh-TW | 繁體中文            | ✔️ [@Yukaii](https://github.com/Yukaii) [@5idereal](https://github.com/5idereal)                            |

## 📜 开源许可

基于 [MIT license](https://opensource.org/licenses/MIT) 许可进行开源。

原项目来自[agalwood](https://github.com/agalwood/Motrix)  
原作者已经三年没更新了，我本人是 Motrix 重度使用者，十分感谢原作者开源项目  
无论bro现在在哪里、在做什么，我都希望他还好 :D

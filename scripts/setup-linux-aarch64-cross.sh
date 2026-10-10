#!/usr/bin/env bash
set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then
  exec sudo -E "$0" "$@"
fi

install_gtk=0
for arg in "$@"; do
  case "$arg" in
    --gtk) install_gtk=1 ;;
    *) echo "unknown argument: $arg" >&2; exit 2 ;;
  esac
done

export DEBIAN_FRONTEND=noninteractive

if [ -f /etc/apt/sources.list ]; then
  sed -i -E \
    '/\[arch=/! s|^deb http://archive.ubuntu.com/ubuntu/|deb [arch=amd64] http://archive.ubuntu.com/ubuntu/|' \
    /etc/apt/sources.list
  sed -i -E \
    '/\[arch=/! s|^deb http://security.ubuntu.com/ubuntu/|deb [arch=amd64] http://security.ubuntu.com/ubuntu/|' \
    /etc/apt/sources.list
fi

ports=/etc/apt/sources.list.d/ubuntu-ports-arm64.list
if [ ! -f "$ports" ]; then
  cat >"$ports" <<'SRC'
deb [arch=arm64] http://ports.ubuntu.com/ubuntu-ports jammy main restricted universe multiverse
deb [arch=arm64] http://ports.ubuntu.com/ubuntu-ports jammy-updates main restricted universe multiverse
deb [arch=arm64] http://ports.ubuntu.com/ubuntu-ports jammy-security main restricted universe multiverse
deb [arch=arm64] http://ports.ubuntu.com/ubuntu-ports jammy-backports main restricted universe multiverse
SRC
fi

dpkg --add-architecture arm64
apt-get update

packages=(
  gcc-aarch64-linux-gnu
  g++-aarch64-linux-gnu
  libssl-dev:arm64
)
if [ "$install_gtk" -eq 1 ]; then
  packages+=(
    libwebkit2gtk-4.1-dev:arm64
    libgtk-3-dev:arm64
    libayatana-appindicator3-dev:arm64
    librsvg2-dev:arm64
  )
fi

apt-get install -y --no-install-recommends "${packages[@]}"

if [ -n "${GITHUB_ENV:-}" ]; then
  {
    echo "CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc"
    echo "CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc"
    echo "CXX_aarch64_unknown_linux_gnu=aarch64-linux-gnu-g++"
    echo "PKG_CONFIG_ALLOW_CROSS=1"
    echo "PKG_CONFIG_PATH=/usr/lib/aarch64-linux-gnu/pkgconfig"
  } >>"$GITHUB_ENV"
fi

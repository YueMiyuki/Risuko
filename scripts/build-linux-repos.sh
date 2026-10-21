#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
	echo "Usage: build-linux-repos.sh <packages-dir> <repo-dir>" >&2
	echo "Adds packages not yet in <repo-dir> and indexes every package it holds." >&2
	exit 2
fi
: "${GPG_KEY_ID:?GPG_KEY_ID must name the signing key}"
: "${BASE_URL:?BASE_URL must be the public URL the repos are served from}"

packages=$(realpath "$1")
mkdir -p "$2"
out=$(realpath "$2")
apt="$out/apt"
rpm="$out/rpm"

shopt -s nullglob
debs=("$packages"/*.deb)
rpms=("$packages"/*.rpm)
if ((${#debs[@]} == 0 || ${#rpms[@]} == 0)); then
	echo "Expected .deb and .rpm packages in $packages" >&2
	exit 1
fi

sign() {
	gpg --batch --yes --local-user "$GPG_KEY_ID" "$@"
}

for deb in "${debs[@]}"; do
	name=$(dpkg-deb -f "$deb" Package)
	version=$(dpkg-deb -f "$deb" Version)
	arch=$(dpkg-deb -f "$deb" Architecture)
	dest="$apt/pool/main/${name:0:1}/$name/${name}_${version}_${arch}.deb"
	[[ -e "$dest" ]] && continue
	mkdir -p "$(dirname "$dest")"
	cp "$deb" "$dest"
done

cd "$apt"
archs=$(for deb in pool/main/*/*/*.deb; do dpkg-deb -f "$deb" Architecture; done | sort -u | xargs)
for arch in $archs; do
	mkdir -p "dists/stable/main/binary-$arch"
	apt-ftparchive --arch "$arch" packages pool >"dists/stable/main/binary-$arch/Packages"
	gzip -9nkf "dists/stable/main/binary-$arch/Packages"
done
apt-ftparchive \
	-o APT::FTPArchive::Release::Origin=Risuko \
	-o APT::FTPArchive::Release::Label=Risuko \
	-o APT::FTPArchive::Release::Suite=stable \
	-o APT::FTPArchive::Release::Codename=stable \
	-o APT::FTPArchive::Release::Components=main \
	-o "APT::FTPArchive::Release::Architectures=$archs" \
	release dists/stable >"$out/Release"
mv "$out/Release" dists/stable/Release
sign --clearsign -o dists/stable/InRelease dists/stable/Release
sign --armor --detach-sign -o dists/stable/Release.gpg dists/stable/Release
gpg --batch --yes --export "$GPG_KEY_ID" >risuko.gpg

mkdir -p "$rpm/packages"
for file in "${rpms[@]}"; do
	dest="$rpm/packages/$(rpm -qp --qf '%{NAME}-%{VERSION}-%{RELEASE}.%{ARCH}.rpm' "$file")"
	[[ -e "$dest" ]] && continue
	cp "$file" "$dest"
done
for file in "$rpm"/packages/*.rpm; do
	if rpm -qpi "$file" 2>/dev/null | grep -q '^Signature *: (none)'; then
		rpmsign --define "__gpg $(command -v gpg)" --define "_gpg_name $GPG_KEY_ID" --addsign "$file"
	fi
done
createrepo_c --general-compress-type=gz "$rpm"
sign --armor --detach-sign -o "$rpm/repodata/repomd.xml.asc" "$rpm/repodata/repomd.xml"
gpg --batch --yes --armor --export "$GPG_KEY_ID" >"$rpm/risuko.asc"
cat >"$rpm/risuko.repo" <<EOF
[risuko]
name=Risuko
type=rpm-md
baseurl=$BASE_URL/rpm
enabled=1
gpgcheck=1
repo_gpgcheck=1
gpgkey=$BASE_URL/rpm/risuko.asc
EOF

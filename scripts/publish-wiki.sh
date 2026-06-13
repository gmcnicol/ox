#!/usr/bin/env bash
set -euo pipefail

repo="${1:-https://github.com/gmcnicol/ox.wiki.git}"
workdir="${TMPDIR:-/tmp}/ox-wiki-publish"

rm -rf "$workdir"
git clone "$repo" "$workdir"

cp wiki/*.md "$workdir"/

cd "$workdir"
git add .

if git diff --cached --quiet; then
  echo "Wiki is already up to date."
  exit 0
fi

git commit -m "Publish Ox design notes"
git push

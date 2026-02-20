#!/usr/bin/env bash
set -euo pipefail

echo "== CG DB_MQTT: normalize line endings to LF =="

# Prefer dos2unix if installed (handles DOS and old-Mac CR endings)
if command -v dos2unix >/dev/null 2>&1; then
  echo "Using dos2unix…"
  find . -type f \(       -name "*.py" -o -name "*.sh" -o -name "*.yml" -o -name "*.yaml" -o -name "*.sql" -o -name "*.md" -o -name "*.service" -o -name "*.timer" -o -name ".gitignore" -o -name ".gitattributes" -o -name ".editorconfig"     \) -print0 | xargs -0 dos2unix -q
else
  echo "dos2unix not found; using Python converter…"
  python3 - <<'PY'
import pathlib
exts = {".py",".sh",".yml",".yaml",".sql",".md",".service",".timer"}
names = {".gitignore",".gitattributes",".editorconfig"}
for p in pathlib.Path(".").rglob("*"):
    if not p.is_file():
        continue
    if p.name in names or p.suffix in exts:
        b = p.read_bytes()
        # convert CRLF and CR to LF
        b2 = b.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
        if b2 != b:
            p.write_bytes(b2)
print("Done.")
PY
fi

echo ""
echo "Now run (recommended):"
echo "  git status"
echo "  git diff --stat"

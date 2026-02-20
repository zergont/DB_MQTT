#!/usr/bin/env python3
"""Fix line endings (CRLF/CR -> LF) for Linux-friendly repo files.

Run from repo root:
  python scripts/fix_eol.py

It will convert common text files used in deployment and ensure final newline.
"""

from __future__ import annotations

from pathlib import Path

EXTS = {
    ".py", ".sh", ".yml", ".yaml", ".sql", ".md", ".service", ".timer", ".ps1"
}
NAMES = {".gitattributes", ".gitignore", ".editorconfig"}


def should_fix(p: Path) -> bool:
    return p.name in NAMES or p.suffix in EXTS


def main() -> None:
    root = Path(".").resolve()
    changed = 0
    scanned = 0
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        if not should_fix(p):
            continue
        scanned += 1
        b = p.read_bytes()
        if not b:
            continue
        b2 = b.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
        if not b2.endswith(b"\n"):
            b2 += b"\n"
        if b2 != b:
            p.write_bytes(b2)
            changed += 1
            print(f"FIXED: {p.relative_to(root)}")
    print(f"Scanned: {scanned}  Changed: {changed}")


if __name__ == "__main__":
    main()

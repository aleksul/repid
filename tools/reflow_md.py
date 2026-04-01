#!/usr/bin/env python3
"""Simple markdown reflow tool: wrap paragraphs to 100 chars.

Rules:
- Preserve fenced code blocks (```), indented code, and tables (lines with |)
- Preserve lines starting with list markers ('-', '*', '+', or numbered lists)
- Preserve HTML comments and markdownlint directives
"""

from __future__ import annotations

import sys
import textwrap
from pathlib import Path

WIDTH = 100


def should_skip_line(line: str) -> bool:
    s = line.lstrip()
    if not s:
        return True
    # comments, headings, tables, fences
    if s.startswith(("<!--", "#", "|")) or " | " in line:
        return True
    if s.startswith("```"):
        return True
    # blockquote lines should be preserved — return the condition directly
    return s.startswith(">")


def _detect_list_marker(s: str) -> tuple[str | None, str | None]:
    """Return (marker, content) when *s* is a list item, else (None, None).

    Marker includes the trailing space (eg. "- " or "1. ").
    """
    if s.startswith(("- ", "* ", "+ ")):
        return s[:2], s[2:]
    parts = s.split(" ", 1)
    if parts and parts[0].endswith(".") and parts[0][:-1].isdigit():
        marker = parts[0] + " "
        content = parts[1] if len(parts) > 1 else ""
        return marker, content
    return None, None


def _wrap_list_item(line: str, s: str, marker: str, content: str, out_lines: list[str]) -> None:
    """Wrap a single list item and append to out_lines.

    Preserves the original leading whitespace and aligns wrapped lines with
    spaces after the list marker.
    """
    leading = line[: len(line) - len(s)]
    if not content:
        out_lines.append(line)
        return
    wrap_width = WIDTH - (len(leading) + len(marker))
    wrapped = textwrap.fill(content, wrap_width)
    wrapped_lines = wrapped.splitlines()
    out_lines.append(f"{leading}{marker}{wrapped_lines[0]}")
    spacer = " " * len(marker)
    for wl in wrapped_lines[1:]:
        out_lines.append(f"{leading}{spacer}{wl}")


def reflow_file(path: Path) -> bool:
    changed = False
    text = path.read_text(encoding="utf8")
    lines = text.splitlines()

    out_lines: list[str] = []
    in_fence = False
    para: list[str] = []

    def flush_para() -> None:
        nonlocal para, out_lines, changed
        if not para:
            return
        s = " ".join(item.strip() for item in para)
        wrapped = textwrap.fill(s, WIDTH)
        out_lines.extend(wrapped.splitlines())
        para = []

    for line in lines:
        if line.lstrip().startswith("```"):
            if in_fence:
                # closing fence
                out_lines.append(line)
                in_fence = False
                continue
            # opening fence: flush paragraph first
            flush_para()
            out_lines.append(line)
            in_fence = True
            continue

        if in_fence:
            out_lines.append(line)
            continue

        s = line.lstrip()
        # Handle list items (unordered "- ", "* ", "+ ") and numbered lists
        # like "1. ". We wrap the item text while preserving the leading
        # indentation and marker.
        marker, content = _detect_list_marker(s)
        if marker is not None:
            flush_para()
            # Content may be None for safety, but _detect_list_marker returns
            # (None, None) when no marker was found. Here both are str when
            # marker is not None.
            _wrap_list_item(line, s, marker, content or "", out_lines)
            continue

        if should_skip_line(line):
            flush_para()
            out_lines.append(line)
            continue

        # part of a paragraph
        para.append(line)

    flush_para()

    new_text = "\n".join(out_lines) + ("\n" if text.endswith("\n") else "")
    if new_text != text:
        path.write_text(new_text, encoding="utf8")
        changed = True
    return changed


def main(_argv: list[str]) -> int:
    # argv is unused by design; keep signature for future extension
    base = Path.cwd()
    patterns = ["docs/**/*.md", "*.md", "AGENTS.md", ".agents/**/*.md"]
    files = set()
    for p in patterns:
        for f in base.glob(p):
            if f.is_file():
                files.add(f)

    any_changed = False
    for f in sorted(files):
        try:
            changed = reflow_file(f)
            if changed:
                any_changed = True
        except OSError:
            # ignore file system errors per-file; tool should be best-effort
            continue

    return 1 if any_changed else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

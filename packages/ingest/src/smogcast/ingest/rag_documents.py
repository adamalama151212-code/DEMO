"""Official documents for the assistant's knowledge base → landing/rag/_zrodlo/ (originals, unchanged).

Sources are listed in conf/app.yaml (``app.rag.sources``) with the format each must have. Some portals
answer automated clients with an HTML page (a captcha or a redirect loop) under a ``.pdf`` URL — saving
that as the "regulation" would silently poison the knowledge base, so every download is checked
against its expected format. Files already downloaded are kept (cache); delete one to refresh it.
"""

from __future__ import annotations

import logging
from pathlib import Path

import requests

log = logging.getLogger(__name__)

# A browser-like agent: EUR-Lex and gov.pl serve plain clients the same documents, but some
# return an empty body to unknown agents.
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) smogcast/0.1"}


def looks_like(content: bytes, expect: str) -> bool:
    head = content[:512].lstrip().lower()
    if expect == "pdf":
        return content.startswith(b"%PDF-")
    if expect == "html":
        return head.startswith((b"<!doctype html", b"<html")) and len(content) > 2000
    raise ValueError(f"Unknown expected format {expect!r}")


def download_sources(sources: list[dict], out_dir: str | Path, get=requests.get) -> list[Path]:
    """Download missing sources; return the paths of all sources present afterwards."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    present = []
    for src in sources:
        target = out / src["file"]
        if target.exists():
            present.append(target)
            continue
        r = get(src["url"], headers=HEADERS, timeout=120)
        r.raise_for_status()
        if not looks_like(r.content, src["expect"]):
            raise ValueError(f"{src['id']}: expected {src['expect']} from {src['url']}, got something else "
                             f"({len(r.content)} bytes) — the portal may block automated downloads")
        tmp = target.with_suffix(target.suffix + ".part")
        tmp.write_bytes(r.content)
        tmp.replace(target)
        log.info("rag-documents: %s (%d kB)", target.name, len(r.content) // 1024)
        present.append(target)
    return present

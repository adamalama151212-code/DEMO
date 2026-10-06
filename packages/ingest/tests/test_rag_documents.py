"""Knowledge-base downloads: a portal's HTML error page must never be saved as a PDF."""

import pytest

from smogcast.ingest.rag_documents import download_sources, looks_like

SRC = [{"id": "reg", "url": "https://x/reg.pdf", "file": "reg.pdf", "expect": "pdf"}]


class Resp:
    def __init__(self, content):
        self.content = content

    def raise_for_status(self):
        pass


def test_format_guard():
    assert looks_like(b"%PDF-1.7 ...", "pdf")
    assert not looks_like(b"<html><script>captcha</script></html>", "pdf")
    assert looks_like(b"<!DOCTYPE html>" + b"x" * 3000, "html")
    assert not looks_like(b"<!DOCTYPE html>tiny", "html")      # a redirect stub, not the article


def test_download_caches_and_rejects_wrong_format(tmp_path):
    calls = []
    files = download_sources(SRC, tmp_path, get=lambda url, **kw: calls.append(url) or Resp(b"%PDF-1.7 body"))
    assert files == [tmp_path / "reg.pdf"] and len(calls) == 1
    download_sources(SRC, tmp_path, get=lambda url, **kw: calls.append(url) or Resp(b"%PDF-"))
    assert len(calls) == 1                                       # cached: no second request
    with pytest.raises(ValueError, match="expected pdf"):
        download_sources([{**SRC[0], "file": "other.pdf"}], tmp_path, get=lambda url, **kw: Resp(b"<html>"))
    assert not (tmp_path / "other.pdf").exists()

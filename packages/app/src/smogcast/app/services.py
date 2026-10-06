"""Wiring shared by the command line and the web interface: gold reader, language model, RAG index."""

from __future__ import annotations

import logging
from pathlib import Path

from smogcast.app.assistant import Assistant
from smogcast.app.gold import GoldReader
from smogcast.app.llm import LLM
from smogcast.app.rag import Embedder, Index, Retriever, load_chunks

log = logging.getLogger(__name__)


def index_dir(ctx) -> str:
    # Next to the trained models: a derived artefact, rebuilt from landing + docs, never edited by hand.
    return ctx.storage.models("rag_index")


def build_index(ctx) -> Index:
    rag = ctx.cfg["app"]["rag"]
    chunks = load_chunks(rag["project_docs_dir"], ctx.storage.landing("rag", "_zrodlo"), rag["sources"],
                         rag["chunk_chars"], rag["overlap_chars"])
    if not chunks:
        raise FileNotFoundError(f"No documents in {rag['project_docs_dir']} or landing/rag/_zrodlo — "
                                "run `smogcast-ingest --step rag-documents` first")
    index = Index.build(chunks, Embedder(rag["embedding_model"]))
    index.save(index_dir(ctx))
    by_doc: dict[str, int] = {}
    for c in chunks:
        by_doc[c.doc_id] = by_doc.get(c.doc_id, 0) + 1
    log.info("RAG index: %d chunks %s -> %s", len(chunks), by_doc, index_dir(ctx))
    return index


def load_retriever(ctx) -> Retriever | None:
    rag = ctx.cfg["app"]["rag"]
    if not (Path(index_dir(ctx)) / "chunks.json").exists():
        log.warning("RAG index missing — run `smogcast-app build-index`")
        return None
    index = Index.load(index_dir(ctx))
    if index.model_name != rag["embedding_model"]:
        log.warning("RAG index was built with %s, configuration says %s — rebuild it", index.model_name,
                    rag["embedding_model"])
    return Retriever(index, Embedder(rag["embedding_model"]), rag["top_k"])


def build_assistant(ctx) -> Assistant:
    app = ctx.cfg["app"]
    return Assistant(ctx.cfg, GoldReader(ctx, app["sql"]["max_rows"]), LLM(app["llm"]), load_retriever(ctx))

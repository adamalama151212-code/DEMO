"""The assistant: text matching, RAG pieces, guarded gold access, deterministic forecast answers and routing.
No network and no real language model: a scripted fake LLM and a bag-of-words embedder stand in."""

import datetime as dt
import hashlib

import numpy as np
import pytest

from smogcast.app import forecast as fc
from smogcast.app.assistant import NOT_FOUND, Assistant
from smogcast.app.gold import GoldReader
from smogcast.app.guardrails import UnsafeQueryError
from smogcast.app.llm import LLMUnavailable, strip_reasoning
from smogcast.app.rag import BM25, Chunk, Index, Retriever, html_to_text, load_chunks, markdown_sections, split_text
from smogcast.app.text import find_cities, find_pollutants

D = dt.date(2026, 10, 1)


# ----------------------------------------------------------------------------- text
def test_cities_and_pollutants_in_free_text(cfg):
    cities = cfg["cities"]
    assert find_cities("Czy jutro w Krakowie i w Łodzi będzie smog?", cities) == ["krakow", "lodz"]
    assert find_cities("A w Białymstoku?", cities) == ["bialystok"]
    assert find_cities("Chcę poznać normy pyłu", cities) == []          # "poznać" is not Poznań
    assert find_cities("Will there be smog in Warsaw or Cracow tomorrow?", cities) == ["warszawa", "krakow"]
    assert find_pollutants("pm2,5 i PM10") == ["PM10", "PM2.5"]
    assert find_pollutants("pył drobny") == ["PM2.5"] and find_pollutants("smog") == []
    assert find_pollutants("fine dust in PM 2.5") == ["PM2.5"]


def test_reasoning_block_is_removed():
    assert strip_reasoning("<think>the user asks…</think>\nOdpowiedź.") == "Odpowiedź."
    assert strip_reasoning("<think>cut off by max tokens") == ""
    assert strip_reasoning("<thinking>step 1…</thinking>SELECT 1") == "SELECT 1"


# ----------------------------------------------------------------------------- RAG
def test_split_text_respects_size_and_overlap():
    text = "\n\n".join(f"Paragraph {i} " + "word " * 40 for i in range(10))
    pieces = split_text(text, 500, 100)
    assert len(pieces) > 1 and all(len(p) <= 600 for p in pieces)
    assert pieces[1][:50] in pieces[0]                                   # each piece starts with the previous tail
    assert all(p.split()[0] in {"Paragraph", "word"} or p.split()[0].isdigit() for p in pieces)  # never mid-word
    long = split_text("abcdefghij " * 300, 500, 100)                     # one paragraph without breaks
    assert all(p.startswith("abcdefghij") for p in long)


def test_markdown_sections_and_html_text():
    assert [h for h, _ in markdown_sections("# T\nintro\n## A\nx\n## B\ny")] == ["T", "A", "B"]
    html = "<html><nav>menu</nav><script>var x</script><h2>Poziom alarmowy</h2><p>150 µg/m³</p></html>"
    text = html_to_text(html)
    assert "Poziom alarmowy" in text and "150" in text and "menu" not in text and "var x" not in text


class BagOfWords:
    """Deterministic stand-in for a sentence-transformers model: hashed word counts, normalised."""

    model_name = "bag-of-words"

    def encode(self, texts):
        out = np.zeros((len(texts), 256), dtype=np.float32)
        for i, t in enumerate(texts):
            for w in t.lower().split():
                out[i, int(hashlib.md5(w.strip(".,?").encode()).hexdigest(), 16) % 256] += 1
        return out / np.maximum(np.linalg.norm(out, axis=1, keepdims=True), 1e-9)


def test_index_finds_the_right_passage_and_survives_save(tmp_path):
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "kb.md").write_text("# Knowledge\n## Alarm\nThe alarm level for PM10 is 150 per day.\n"
                                "## Model\nGradient boosted trees forecast tomorrow.", encoding="utf-8")
    src = tmp_path / "src"
    src.mkdir()
    (src / "page.html").write_text("<html><p>" + "Health advice: stay indoors when smog is high. " * 3 + "</p></html>",
                                   encoding="utf-8")
    chunks = load_chunks(docs, src, [{"id": "who", "title": "WHO", "publisher": "WHO", "file": "page.html",
                                      "expect": "html"}, {"id": "missing", "file": "nope.pdf", "expect": "pdf"}],
                         size=400, overlap=50)
    assert {c.doc_id for c in chunks} == {"kb", "who"}                      # missing source skipped
    Index.build(chunks, BagOfWords()).save(tmp_path / "idx")
    retriever = Retriever(Index.load(tmp_path / "idx"), BagOfWords(), k=1)
    best, _ = retriever("what is the alarm level for PM10")[0]
    assert best.location == "section: Alarm"


def test_hybrid_search_catches_exact_terms_that_embeddings_blur():
    class Useless:  # every text gets the same vector: only BM25 can rank
        model_name = "constant"

        def encode(self, texts):
            return np.ones((len(texts), 4), dtype=np.float32) / 2

    chunks = [Chunk("d", "Doc", "p", f"page {i}", text) for i, text in enumerate([
        "Poziom alarmowy pyłu PM10 wynosi 150 µg/m³.",
        "Poziom informowania dla pyłu PM10 wynosi 100 µg/m³.",
        "Daily limit value for PM2,5 of 25 µg/m³ shall be attained by 2030.",
    ])]
    bm25 = BM25([c.text for c in chunks])
    assert int(bm25.scores("poziom informowania PM10").argmax()) == 1    # not the alarm level (page 0)
    r = Retriever(Index.build(chunks, Useless()), Useless(), k=1)
    assert r("normy dla PM2.5 od 2030")[0][0].location == "page 2"        # "PM2.5" matches "PM2,5"


# ----------------------------------------------------------------------------- gold tables
FORECAST = ("city_id STRING, city_name STRING, jurisdiction_code STRING, pollutant STRING, issue_day DATE, "
            "target_day DATE, probability DOUBLE, warned BOOLEAN, limit_ug_m3 DOUBLE, status STRING, "
            "unusable_reason STRING, model STRING, model_version STRING, weather_source STRING, "
            "pm_d1_max_ug_m3 DOUBLE, pm_d_morning_max_ug_m3 DOUBLE, t_mean_c DOUBLE, wind_mean_ms DOUBLE, "
            "calm_hours BIGINT, precip_sum_mm DOUBLE")


def forecast_row(city, name, pollutant, p, reason=None):
    return (city, name, "PL-12", pollutant, D, D + dt.timedelta(days=1), p, None if p is None else p >= 0.5,
            50.0 if pollutant == "PM10" else 25.0, "ok" if p is not None else "no_forecast", reason, "gbt", "final",
            "live", 49.1, 43.8, 15.8, 1.6, 9, 0.0)


@pytest.fixture
def gold(ctx):
    s, st = ctx.spark, ctx.storage
    st.overwrite(s.createDataFrame([forecast_row("krakow", "Kraków", "PM10", 0.72),
                                    forecast_row("krakow", "Kraków", "PM2.5", 0.20),
                                    forecast_row("gdansk", "Gdańsk", "PM10", None, "no_valid_pm_yesterday"),
                                    forecast_row("gdansk", "Gdańsk", "PM2.5", 0.05)], FORECAST), "gold", "forecast_tomorrow")
    st.overwrite(s.createDataFrame([("PM10", "gbt", 7, 0.7, 0.8, 40, 0.75, 0.55, "test"),
                                    ("PM10", "gbt", 7, 0.7, 0.8, 10, 0.74, 0.60, "validation")],
                                   "pollutant STRING, model STRING, bin INT, bin_from DOUBLE, bin_to DOUBLE, n_days BIGINT, "
                                   "mean_probability DOUBLE, observed_frequency DOUBLE, split STRING"), "gold", "reliability")
    st.overwrite(s.createDataFrame([("PM10", "gbt", True), ("PM10", "logistic", False)],
                                   "pollutant STRING, model STRING, operational BOOLEAN"), "gold", "model_selection")
    st.overwrite(s.createDataFrame([("PM10", "gbt", "K3", "calibration", 0.21, 0.15, False),
                                    ("PM10", "gbt", "K1", "BSS", 0.24, 0.0, True)],
                                   "pollutant STRING, model STRING, criterion STRING, description STRING, value DOUBLE, "
                                   "threshold DOUBLE, passed BOOLEAN"), "gold", "acceptance")
    return GoldReader(ctx, max_rows=50)


def test_gold_reader_rewrites_names_and_enforces_guardrails(gold):
    sql = gold.physical("SELECT f.city_id FROM gold.forecast_tomorrow f")
    assert "delta.`" in sql and "/gold/forecast_tomorrow`" in sql and " AS f" in sql
    res = gold.query("SELECT city_id, probability FROM gold.forecast_tomorrow WHERE pollutant = 'PM10' ORDER BY city_id")
    assert res.sql.endswith("LIMIT 50") and [r["city_id"] for r in res.rows] == ["gdansk", "krakow"]
    with pytest.raises(UnsafeQueryError):
        gold.query("SELECT * FROM silver.pm_hourly")
    assert "gold.forecast_tomorrow(city_id string" in gold.schema_text()


def test_forecast_text_is_honest(gold, cfg):
    data = fc.load(gold)
    rows = {(r["city_id"], r["pollutant"]): r for r in data["forecast"].rows}
    text = fc.describe(rows[("krakow", "PM10")], data["reliability"].rows, data["acceptance"].rows, 0.5)
    assert "**72%**" in text and "**warning**" in text
    assert "at forecasts of 70%–80% the limit was exceeded on 55% of days (40 days)" in text   # test split only
    assert "does not meet the reliability criteria" in text and "calibration" in text
    missing = fc.describe(rows[("gdansk", "PM10")], [], [], 0.5)
    assert "no forecast" in missing and "yesterday" in missing


def rel_row(pollutant, b, n, mean_p, observed):
    return {"pollutant": pollutant, "bin": b, "bin_from": b / 10, "bin_to": (b + 1) / 10, "n_days": n,
            "mean_probability": mean_p, "observed_frequency": observed}


def test_calibration_text_uses_the_worst_judged_bin():
    rel = [rel_row("PM10", 1, 900, 0.14, 0.12), rel_row("PM10", 7, 40, 0.76, 0.55),
           rel_row("PM10", 9, 5, 0.95, 0.20),                                   # too few days: never the example
           rel_row("PM2.5", 3, 200, 0.35, 0.40)]
    assert fc.worst_bin(rel, "PM10", 30)["bin"] == 7
    text = fc.calibration_text(rel, 30, 0.15)
    assert "the model said 76% on average, the limit was exceeded on 55% of those 40 days: 21 percentage points " \
           "below the diagonal, more than allowed, so K3 is not met" in text
    assert "**PM2.5:** the largest gap is in the 30%–40% group" in text and "5 percentage points above" in text
    assert "fewer than 30 days" in text and "within 15 percentage points" in text
    assert "What it shows" not in fc.calibration_text([], 30, 0.15)


def test_acceptance_rows_read_in_plain_words():
    cells = fc.criterion_cells({"criterion": "K3", "description": "max calibration gap", "value": 0.21,
                                "threshold": 0.15, "passed": False})
    assert cells == ["K3", "calibrated (largest gap)", "21 pp", "≤ 15 pp", "✗ not met"]
    assert fc.criterion_cells({"criterion": "K4a", "description": "POD", "value": 0.47, "threshold": 0.6,
                               "passed": False})[2:4] == ["47%", "≥ 60%"]
    assert fc.criterion_cells({"criterion": "K2", "description": "Brier", "value": 0.047, "threshold": 0.093,
                               "passed": True})[2:] == ["0.047", "< 0.093", "✓ met"]


# ----------------------------------------------------------------------------- assistant
class FakeLLM:
    """Answers by prompt type; ``sql`` is a list of queries returned one per SQL request."""

    def __init__(self, route="DATA", sql=None, up=True):
        self.route, self.sql, self.up, self.calls = route, list(sql or []), up, []

    def available(self):
        return self.up

    def chat(self, system, user, history=None):
        if not self.up:
            raise LLMUnavailable("down")
        self.calls.append(system.split("\n")[0])
        if system.startswith("You route"):
            return self.route
        if system.startswith("You write ONE"):
            return f"```sql\n{self.sql.pop(0)}\n```"
        if system.startswith("You explain"):
            return "Summary of the results."
        return "The alarm level is 150 µg/m³ [1]."


def passages(_q):
    return [(Chunk("pl", "Rozporządzenie", "MKiŚ", "page 9", "pył zawieszony PM10 24 godziny 150"), 0.8)]


def test_city_question_is_answered_without_a_language_model(gold, cfg):
    a = Assistant(cfg, gold, FakeLLM(up=False)).ask("Will PM10 exceed the limit in Kraków tomorrow?")
    assert a.route == "forecast" and "**72%**" in a.text and "gold.forecast_tomorrow" in a.sql
    assert [r["pollutant"] for r in a.rows] == ["PM10"]


def test_data_question_retries_after_guardrails_reject_the_sql(gold, cfg):
    llm = FakeLLM(sql=["SELECT * FROM silver.pm_hourly",
                       "SELECT city_id, probability FROM gold.forecast_tomorrow ORDER BY probability DESC"])
    a = Assistant(cfg, gold, llm).ask("Which city has the highest probability?")
    assert a.route == "data" and a.text == "Summary of the results."
    assert a.rows[0]["city_id"] == "krakow" and "guardrails" in a.notes[0]


def test_knowledge_question_cites_sources_or_shows_passages(cfg):
    a = Assistant(cfg, None, FakeLLM(route="KNOWLEDGE"), passages).ask("What is the PM10 alarm level?")
    assert a.route == "knowledge" and "[1]" in a.text and a.sources
    offline = Assistant(cfg, None, FakeLLM(up=False), passages).ask("Jaki jest poziom alarmowy pyłu?")  # Polish works too
    assert "not available" in offline.text and "150" in offline.text


def test_legal_levels_and_explanations_never_go_to_sql(cfg):
    # Regression: a 7B model routed this to SQL and answered with the daily limit (50) as the "alarm level".
    a = Assistant(cfg, None, FakeLLM(route="DATA"), passages)
    for q in ("What is the PM10 alarm level?", "Why is PM10 harder to forecast than PM2.5?",
              "What are the new EU limits for PM2.5 from 2030?", "What should I do at the PM10 information level?",
              "Jaki jest poziom alarmowy dla PM10?", "Dlaczego PM10 przewiduje się gorzej?"):
        assert a.route(q) == "knowledge", q
    for q in ("Which city had the best PM10 Brier score in 2025?", "How many readings were quarantined?",
              "W którym mieście model PM10 miał najlepszy Brier w 2025?"):
        assert a.route(q) == "data", q


def test_off_topic_question_is_declined_without_touching_data(cfg):
    llm = FakeLLM(route="DATA")                  # even when a small model would call it "DATA"
    a = Assistant(cfg, None, llm, passages).ask("Who won the 2022 football World Cup?")
    assert a.route == "other" and "air quality" in a.text and a.sql is None and llm.calls == []
    # an unclear question with domain words still goes to the model, which may also say OTHER
    assert Assistant(cfg, None, FakeLLM(route="OTHER"), passages).route("What about dust on Mars?") == "other"


def test_not_found_answer_has_no_sources(cfg):
    class Nothing(FakeLLM):
        def chat(self, system, user, history=None):
            return "KNOWLEDGE" if system.startswith("You route") else NOT_FOUND

    a = Assistant(cfg, None, Nothing(), passages).ask("What is the dust concentration on Mars?")
    assert a.text == NOT_FOUND and a.sources == []


def test_ask_rejects_an_unknown_city_without_starting_spark(capsys):
    from smogcast.app.main import main

    assert main(["ask", "--city", "Katowice"]) == 2
    assert "There is no forecast" in capsys.readouterr().out


def test_charts_build(gold, cfg):
    pytest.importorskip("plotly")
    from smogcast.app.ui import charts

    data = fc.load(gold)
    fig = charts.probabilities(data["forecast"].rows, "PM10", 0.5)
    assert list(fig.data[0].y) == ["Gdańsk", "⚠ Kraków"]                 # highest on top, warning marked by text
    assert charts.reliability(data["reliability"].rows, ["PM10"], ("PM10", 0.72)).data


def test_html_table_escapes_and_aligns_numbers():
    from smogcast.app.ui import theme

    html = theme.table(["a", "b"], [["<K1>", "0.24"]], numeric=(1,))
    assert "&lt;K1&gt;" in html and '<td class="num">0.24</td>' in html and "<th>a</th>" in html

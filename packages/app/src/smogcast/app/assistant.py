"""The smogcast assistant: one question in, one answer out — with the SQL or the sources behind it.

Routes (numbers never come from the language model):
- ``forecast`` — a city or "tomorrow" is mentioned: the deterministic answer from gold.forecast_tomorrow
  (works without a language model). If the question also asks what to do / health advice, a
  document-based section is added.
- ``data`` — other questions about numbers (model quality, data quality, rankings): the language model
  writes SQL over the whitelisted gold tables, the guardrails validate it, Spark runs it, and the model
  phrases the returned rows. A rejected or failing query gets one corrected retry.
- ``knowledge`` — norms, health, methodology: retrieval over the documents, the model answers ONLY from
  the retrieved passages and cites them; without a language model the passages themselves are shown.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field

from smogcast.app import forecast as fc
from smogcast.app.gold import GoldReader, QueryResult
from smogcast.app.guardrails import UnsafeQueryError
from smogcast.app.llm import LLM, LLMUnavailable
from smogcast.app.rag import Chunk
from smogcast.app.text import find_cities, find_pollutants, normalize

log = logging.getLogger(__name__)

NOT_FOUND = "I could not find this information in the documents."


@dataclass
class Answer:
    text: str
    route: str                                   # forecast | data | knowledge
    sql: str | None = None
    rows: list[dict] | None = None
    sources: list[tuple[Chunk, float]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


# Routing vocabulary in English AND Polish (after ``normalize``: lower case, no diacritics) — the interface is
# English, but users may still type in Polish.
FORECAST_HINTS = ("tomorrow", "forecast", "exceed", "warning", "smog", "jutr", "prognoz", "przekrocz", "ostrzez")
TOMORROW = ("tomorrow", "jutr")
ADVICE_HINTS = ("advice", "recommend", "what should", "what to do", "health", "children", "walk", "run", "jog",
                "cycling", "exercise", "outdoor", "mask", "window", "asthma", "pregnan", "elderly", "safe",
                "zalec", "co robic", "zdrow", "dzieci", "spacer", "biega", "rower", "aktywn", "maseczk", "okna",
                "astm", "ciaz", "senior", "bezpieczn")
# Rules first, the language model only for what they do not settle. A 7B model once routed "what is the
# PM10 alarm level?" to SQL and answered with the daily limit from a forecast column — legal levels,
# definitions and explanations must always come from the documents.
# "who " alone would also match "who won …": the World Health Organization is named explicitly.
KNOWLEDGE_HINTS = ("alarm level", "information level", "alert threshold", "information threshold", "limit value",
                   "limits", "threshold", " eu ", "standard", "norm", "directive", "regulation", " law", "legal",
                   "world health", "who guideline", "who recommend", "who air", "guideline", "why ",
                   "how does", "how is", "what is a", "what does", "meaning", "defin", "effect", "disease", "advice",
                   "recommend", "health", "methodolog",
                   "poziom alarm", "poziom inform", "dyrektyw", "rozporzadz", "przepis", "prawo", "wytyczn",
                   "dopuszczaln", "dlaczego", "czemu", "jak dziala", "jak liczon", "co to", "co oznacza", "czym jest",
                   "definic", "skutk", "chorob", "zalec", "zdrow", "metodyk")
# Vocabulary of the domain: a question with none of it is off-topic (a small model sometimes calls it "DATA").
DOMAIN_HINTS = ("air", "dust", "pm10", "pm2", "pm 2", "smog", "pollut", "quality", "concentration", "model",
                "forecast", "city", "cities", "station", "measure", "reading", "data", "breath", "protect", "gios",
                "world health", "directive", "brier", "calibrat", "weather", "wind", "limit", "level",
                "powietrz", "pyl", "zanieczyszcz", "jakosc", "stezen", "norm", "prognoz", "miast", "stacj", "pomiar",
                "odczyt", "dane", "danych", "oddych", "chroni", "dyrektyw", "kalibrac", "pogod", "wiatr")
DATA_HINTS = ("how many", "how much", "which city", "which cities", "which model", "highest", "lowest", "best",
              "worst", "average", "mean ", "ranking", "rank ", "compare", "metric", "brier", "auc", "pod ", "far ",
              "bss", "quarantin", "late ", "percent", "share of", "in 2025", "in 2024",
              "ile ", "ilu ", "ktore miast", "ktory model", "ktorym miesc", "najwyz", "najniz", "najlepsz", "srednia",
              "porownaj", "metryk", "kwarantann", "spozni", "procent", "odsetek", "w 2025", "w 2024")

CLASSIFY_SYSTEM = """You route questions for an air-quality forecasting assistant.
Answer with exactly one word:
DATA - the question needs numbers from the platform's tables (forecasts, model quality metrics such as Brier, BSS,
POD, FAR, AUC, calibration, acceptance criteria, data-quality counts, rankings or comparisons of cities).
KNOWLEDGE - the question asks about rules, legal limits, health effects and advice, definitions, or how the
system and the model work.
OTHER - the question is not about air quality, smog, health effects of air pollution or this platform."""

OFF_TOPIC = ("I only answer questions about air quality: tomorrow's smog forecast for the 10 largest Polish cities, "
             "air-quality standards and health advice, and how the smogcast model works and how good it is.")

DATA_MEANING = """Meaning of the data:
- forecast_tomorrow: tomorrow's forecast per city and pollutant ('PM10', 'PM2.5'); probability is 0..1;
  warned = probability >= 0.5; the newest forecast has the greatest target_day. limit_ug_m3 is the DAILY LIMIT
  VALUE that defines an exceedance (PM10 50, PM2.5 25) - it is NOT an information or alarm level.
  pm_d1_max_ug_m3 = yesterday's highest station daily mean; pm_d_morning_max_ug_m3 = this morning's highest mean.
- backtest_metrics: model quality per model, split ('validation' = 2024, 'test' = 2025), pollutant and city;
  city_id = 'ALL' means all cities together; models: the trained ones plus 'persistence' and 'climatology'.
  brier: lower is better; bss: skill vs climatology, higher is better; pod: share of exceedance days warned;
  far: share of warnings that were false; auc: ranking quality.
- reliability: calibration bins (bin 0..9 = probability 0.0-0.1 … 0.9-1.0) per model, split and pollutant.
- acceptance: verdict of criteria K1-K4 for each pollutant's operational model on the test year.
- model_selection: hyper-parameter grid; operational = the model used for forecasts.
- dq_summary: stream data quality in LONG format - one row per (source, metric) with its value and
  pct_of_unique. source: 'live' (GIOŚ API) or 'fault_replay' (demonstration). metric names: records_received,
  unique_readings, resent_duplicates, late_rejected, quarantined, quarantined_<reason> (missing_value, below_min,
  above_max, unknown_station), silver_readings, silver_valid, flag_ok, flag_frozen, flag_spike, flag_drift.
  To get a count, select value WHERE metric = '...' - never COUNT(*) the rows. 'quarantined' is the TOTAL and
  the quarantined_<reason> rows are its breakdown (likewise silver_readings vs flag_*): never add a total to its
  own breakdown."""

SQL_EXAMPLES = """Examples:
Q: How many readings from the API were quarantined?
```sql
SELECT metric, value, pct_of_unique FROM gold.dq_summary WHERE source = 'live' AND metric LIKE 'quarantined%'
```
Q: Which city had the lowest Brier score for PM2.5 in 2025?
```sql
SELECT m.city_id, m.model, m.brier FROM gold.backtest_metrics m
JOIN gold.model_selection s ON m.pollutant = s.pollutant AND m.model = s.model AND s.operational
WHERE m.split = 'test' AND m.pollutant = 'PM2.5' AND m.city_id <> 'ALL' ORDER BY m.brier ASC LIMIT 1
```
Q: What is tomorrow's PM10 probability in all cities?
```sql
SELECT city_name, probability, warned FROM gold.forecast_tomorrow
WHERE pollutant = 'PM10' AND target_day = (SELECT MAX(target_day) FROM gold.forecast_tomorrow) ORDER BY probability DESC
```"""

SQL_SYSTEM = """You write ONE Spark SQL SELECT statement that answers the user's question from these tables only:
{schema}

""" + DATA_MEANING + """
Rules: refer to tables as gold.<table>; only SELECT; at most {max_rows} rows; no comments; use only columns
listed above; model quality questions use backtest_metrics (join model_selection for the operational model).

""" + SQL_EXAMPLES + """
Return only the SQL inside a ```sql code block."""

SUMMARY_SYSTEM = """You explain query results of an air-quality platform to the user.
Answer in English (even if the question is in another language), briefly (at most 5 sentences or a short list).
Use ONLY the numbers present in the result rows - never invent or estimate numbers.
Call every number by what its column means (see below) - never rename it into another concept.
If the rows do not answer the question, say so instead of guessing.
Show probabilities and shares as percentages. If the result is empty, say that there is no data.

""" + DATA_MEANING

KNOWLEDGE_SYSTEM = """You are the knowledge assistant of smogcast, a platform that forecasts PM10 and PM2.5
limit exceedances in Polish cities. Your ONLY source of knowledge is the CONTEXT below.

CONTEXT:
{context}

Rules:
1. Answer in English (even if the question is in another language), clearly and concisely. Cite the passages
   you used as [1], [2], ...
2. If the user asks for a list or a summary and the context has loose facts, build the list from them.
3. If the context contains NO information about the question, answer with exactly this one sentence:
   "I could not find this information in the documents."
4. Do not use general knowledge from outside the context. Do not invent numbers.
5. Copy every number exactly as it appears in the context, together with what it refers to. Different levels
   are different things: the information level, the alarm level and the limit value must never be mixed up."""


def _extract_sql(text: str) -> str:
    m = re.search(r"```(?:sql)?\s*(.*?)```", text, re.DOTALL | re.IGNORECASE)
    return (m.group(1) if m else text).strip().rstrip(";").strip()


def _context(passages: list[tuple[Chunk, float]]) -> str:
    return "\n\n".join(f"[{i}] {c.title} ({c.location})\n{c.text}" for i, (c, _) in enumerate(passages, 1))


class Assistant:
    def __init__(self, cfg: dict, gold: GoldReader | None, llm: LLM | None, retriever=None):
        self.cfg, self.gold, self.llm, self.retriever = cfg, gold, llm, retriever

    # ------------------------------------------------------------------ routing
    def route(self, question: str) -> str:
        q = f" {' '.join(re.findall(r'[a-z0-9]+', normalize(question)))} "  # words only, padded: "who " matches at the end
        if find_cities(question, self.cfg["cities"]) or (any(h in q for h in FORECAST_HINTS) and any(t in q for t in TOMORROW)):
            return "forecast"
        data, knowledge = any(h in q for h in DATA_HINTS), any(h in q for h in KNOWLEDGE_HINTS)
        if not (data or knowledge or any(h in q for h in DOMAIN_HINTS + ADVICE_HINTS + FORECAST_HINTS)):
            return "other"  # nothing about air, dust or this platform — no need to ask the model
        if knowledge and not data:
            return "knowledge"
        if data and not knowledge:
            return "data"
        if self.llm is not None:
            try:
                words = self.llm.chat(CLASSIFY_SYSTEM, question).strip().upper().split()
                for label in ("OTHER", "DATA"):
                    if label in (w.strip(".,:") for w in words[:3]):
                        return label.lower()
                return "knowledge"
            except LLMUnavailable:
                pass
        return "data" if data else "knowledge"

    def ask(self, question: str, history: list[dict] | None = None) -> Answer:
        route = self.route(question)
        if route == "forecast":
            return self.forecast_answer(question)
        if route == "data":
            return self.data_answer(question)
        if route == "other":
            return Answer(OFF_TOPIC, "other")
        return self.knowledge_answer(question, history)

    # ------------------------------------------------------------------ forecast (deterministic)
    def forecast_answer(self, question: str) -> Answer:
        if self.gold is None or not self.gold.exists("forecast_tomorrow"):
            return Answer("There is no forecast table yet — run the live path (`smogcast run-live`).", "forecast")
        data = fc.load(self.gold)
        rows = data["forecast"].rows
        cities = find_cities(question, self.cfg["cities"]) or list(self.cfg["run"]["cities"])
        pollutants = find_pollutants(question) or list(self.cfg["pollutants"])
        chosen = [r for r in rows if r["city_id"] in cities and r["pollutant"] in pollutants]
        if not chosen:
            return Answer("There is no forecast for this city and pollutant.", "forecast", data["forecast"].sql, rows)
        rel = data["reliability"].rows if data["reliability"] else []
        acc = data["acceptance"].rows if data["acceptance"] else []
        warning_p = self.cfg["thresholds"]["warning_probability"]
        if len(cities) > 2:  # an overview: one line per city instead of full paragraphs
            lines = [f"**Tomorrow ({chosen[0]['target_day']}) — probability of exceeding the daily limit:**"]
            for r in sorted(chosen, key=lambda r: -(r["probability"] or -1)):
                flag = " ⚠️ warning" if r["warned"] else ""
                lines.append(f"- {r['city_name']} {fc.POLLUTANT_LABEL[r['pollutant']]}: {fc.pct(r['probability'])}{flag}")
            text = "\n".join(lines)
        else:
            text = "\n\n".join(fc.describe(r, rel, acc, warning_p) for r in chosen)
        answer = Answer(text, "forecast", data["forecast"].sql, chosen)
        if any(h in normalize(question) for h in ADVICE_HINTS):
            advice = self.knowledge_answer(question + " (health advice for high particulate matter pollution)")
            answer.text += "\n\n---\n**Advice (from the documents):** " + advice.text
            answer.sources, answer.notes = advice.sources, advice.notes
        return answer

    # ------------------------------------------------------------------ data (text-to-SQL)
    def data_answer(self, question: str) -> Answer:
        if self.llm is None or not self.llm.available():
            return Answer("Questions about data need the language model, which is not available. Ready-made "
                          "summaries are on the Tomorrow, Model and Data quality pages.", "data")
        system = SQL_SYSTEM.format(schema=self.gold.schema_text(), max_rows=self.gold.max_rows)
        user, notes = question, []
        result: QueryResult | None = None
        for _attempt in range(3):
            sql = _extract_sql(self.llm.chat(system, user))
            try:
                result = self.gold.query(sql)
                break
            except UnsafeQueryError as exc:
                notes.append(f"Query rejected by the guardrails: {exc}")
                user = f"{question}\n\nYour previous SQL was rejected: {exc}. Write a corrected query."
            except Exception as exc:  # Spark analysis error: unknown column, bad syntax …
                notes.append(f"Query failed: {str(exc).splitlines()[0][:200]}")
                user = f"{question}\n\nYour previous SQL failed: {str(exc)[:500]}. Write a corrected query."
        if result is None:
            return Answer("I could not build a safe query for this question. Please try rephrasing it.",
                          "data", sql, None, notes=notes)
        payload = json.dumps(result.rows[:50], default=str, ensure_ascii=False)
        text = self.llm.chat(SUMMARY_SYSTEM, f"Question: {question}\nSQL: {result.sql}\nResult rows (JSON): {payload}")
        return Answer(text, "data", result.sql, result.rows, notes=notes)

    # ------------------------------------------------------------------ knowledge (RAG)
    def knowledge_answer(self, question: str, history: list[dict] | None = None) -> Answer:
        if self.retriever is None:
            return Answer("The knowledge base is not built yet — run `smogcast-app build-index`.", "knowledge")
        passages = self.retriever(question)
        if self.llm is None or not self.llm.available():
            listing = "\n\n".join(f"**[{i}] {c.title}** ({c.location}):\n> {c.text[:500]}…"
                                  for i, (c, _) in enumerate(passages[:3], 1))
            return Answer("The language model is not available — below are the most relevant passages.\n\n" + listing,
                          "knowledge", sources=passages, notes=["answered without the language model"])
        text = self.llm.chat(KNOWLEDGE_SYSTEM.format(context=_context(passages)), question, (history or [])[-4:])
        return Answer(text, "knowledge", sources=[] if text.strip() == NOT_FOUND else passages)

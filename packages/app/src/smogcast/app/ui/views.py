"""Views of the web interface: how it works (info), tomorrow in all cities, one city in detail, model quality,
data quality and the assistant. Every number is read from gold tables through the SQL guardrails; each view shows the SQL it
used. The interface is in English. The page shell (navigation, sidebar) is in streamlit_app.py.
"""

from __future__ import annotations

import sys

import streamlit as st

from smogcast.app import forecast as fc
from smogcast.app.ui import charts
from smogcast.app.ui import theme as t


def setup() -> None:
    st.set_page_config(page_title="smogcast — tomorrow's smog forecast", page_icon="🌫️", layout="wide")
    st.markdown(t.CSS, unsafe_allow_html=True)


# ------------------------------------------------------------------------------------------- resources
@st.cache_resource(show_spinner="Starting the data engine (Spark)…")
def context():
    from smogcast.core.context import build_context

    env = None
    if "--env" in sys.argv:
        env = sys.argv[sys.argv.index("--env") + 1]
    return build_context(env)


@st.cache_resource(show_spinner="Loading the assistant and its knowledge base…")
def assistant():
    from smogcast.app.services import build_assistant

    return build_assistant(context())


@st.cache_data(ttl=300, show_spinner="Reading gold tables…")
def gold_data() -> dict:
    from smogcast.app.gold import GoldReader

    ctx = context()
    return fc.load(GoldReader(ctx, ctx.cfg["app"]["sql"]["max_rows"]))


@st.cache_data(ttl=30, show_spinner=False)
def llm_status() -> tuple[bool, str]:
    llm = assistant().llm
    try:
        return True, llm.model
    except Exception as exc:
        return False, str(exc).split(":")[0]


def cfg() -> dict:
    return context().cfg


def show_sql(sql: str, label: str = "SQL query (through the guardrails)") -> None:
    with st.expander(label):
        st.code(sql, language="sql")


def header(title: str, subtitle: str) -> None:
    st.title(title)
    st.markdown(f'<div class="sc-sub">{t.esc(subtitle)}</div>', unsafe_allow_html=True)


def need(data: dict, key: str, how: str) -> list[dict] | None:
    if data.get(key) is None:
        st.info(f"No data yet ({key}). Run: `{how}`.")
        return None
    return data[key].rows


def count(x: float) -> str:
    return f"{x:,.0f}"


# ------------------------------------------------------------------------------------------- views
def view_info() -> None:
    """How the forecast is made, in plain words. Thresholds and settings come from the configuration and the
    verdict from gold, so the page cannot drift from what the pipeline really does."""
    c = cfg()
    limits, warning_p = c["thresholds"]["daily_limit_ug_m3"], c["thresholds"]["warning_probability"]
    mcfg = c["model"]
    issue_h, morning_end = mcfg["issue_hour_cet"], mcfg["morning_last_hour_cet"] + 1
    split = mcfg["split"]
    train_from, train_to = split["train_years"]
    val_year, test_year = split["validation_year"], split["test_year"]
    header("How smogcast works", "How tomorrow's smog forecast is made, in plain words. The other pages show the "
                                 "numbers; this one explains what they mean.")

    st.subheader("The question")
    st.markdown(
        f"**Will the daily limit be exceeded tomorrow in a city, and how sure is that?** A day counts as an "
        f"exceedance when the daily mean concentration at **at least one station** of the city is above the limit: "
        f"**PM10 > {limits['PM10']} µg/m³** (the Polish and EU limit value) or **PM2.5 > {limits['PM2.5']} µg/m³** "
        f"(the EU daily limit that applies from 2030; Polish law has no daily PM2.5 limit today). A station's daily "
        f"mean counts only if at least {c['thresholds']['daily_min_valid_hours']} of its 24 hourly readings exist. "
        f"The answer is a **probability from 0 to 100%**, separately for PM10 and PM2.5 in each of the "
        f"{len(c['run']['cities'])} largest cities in Poland.")

    st.subheader("How tomorrow's forecast is made")
    steps = [
        ("Today's air", f"Every day at {issue_h}:00 CET the model takes the measurements available at that moment: "
                        f"yesterday's daily means and this morning's readings (00:00–{morning_end:02d}:00)."),
        ("Tomorrow's weather", "The weather forecast for tomorrow from Open-Meteo: temperature, wind, hours of calm "
                               "wind, rain, humidity, pressure, clouds and sunshine."),
        ("The model", "A gradient-boosted trees (GBT) model, trained on years of past days, combines these inputs "
                      "with the month, the weekend and the city."),
        ("Probability and warning", f"The result is the probability of an exceedance tomorrow. At "
                                    f"{fc.pct(warning_p)} or more the city gets a warning."),
    ]
    for col, (i, (title, body)) in zip(st.columns(4), enumerate(steps, 1), strict=True):
        col.markdown(t.step(i, title, body), unsafe_allow_html=True)
    st.write("")

    st.subheader("What the model looks at")
    st.markdown(
        "- **Air pollution so far:** yesterday's daily mean (the worst station and the city average), whether "
        "yesterday was already above the limit, the day before yesterday, and this morning's readings. Smog rarely "
        "appears or disappears overnight, so these are the strongest clues.\n"
        "- **Tomorrow's weather forecast:** cold days mean more home heating; calm wind (below 1.5 m/s) and no rain "
        "let smoke stay over the city; wind and rain clear it away.\n"
        "- **Calendar:** month, weekend, heating season (October–April).\n"
        "- **City:** each city has its own typical level of pollution.\n\n"
        "Nothing from tomorrow itself is used. When the model was tested on past years, it was given the weather "
        "forecasts that really existed one day earlier, not the weather that actually happened — so the test shows "
        "how good the forecast is in real use.")

    st.subheader("How the model turns this into a probability")
    st.markdown(
        "The model is a set of **decision trees** (gradient-boosted trees, GBT). One tree asks a few yes/no "
        "questions about the inputs — for example *“was yesterday above 40 µg/m³?”*, *“will there be more than "
        "8 hours of calm wind?”*, *“is it the heating season?”* — and ends with a score. The trees are built one "
        "after another, and each new tree learns to correct the mistakes the previous ones made on past days. "
        "The scores of all trees are added up and turned into a probability between 0% and 100%.\n\n"
        f"The model learned from history: the GIOŚ archive of hourly measurements for {train_from}–{val_year}, "
        "every day paired with what happened the next day. It is trained once and saved; every day the same saved "
        "model computes the forecast for tomorrow from fresh data.\n\n"
        f"The **probability is always shown**. The warning (≥ {fc.pct(warning_p)}) is only a yes/no summary for "
        "those who need one.")

    st.subheader("How far can it be trusted?")
    data = gold_data()
    acc = data["acceptance"].rows if data.get("acceptance") else []
    st.markdown(
        f"The model was tested on **{test_year}**, a year it had never seen. It must beat two simple rules: "
        "**climatology** (*how often the limit was exceeded in this city in this month*) and **persistence** "
        "(*tomorrow will be like today*). Four criteria were fixed **before training** and never changed after "
        "the results were known:\n"
        "- **K1** — better probabilities than climatology;\n"
        "- **K2** — better than persistence;\n"
        "- **K3** — calibrated: “70%” has to mean about 70% (see the chart on the *Model* page);\n"
        "- **K4** (PM10) — catches most exceedance days and does not raise false alarms half of the time.")
    badges = []
    for pollutant in ("PM10", "PM2.5"):
        passed, failed = fc.verdict(acc, pollutant)
        if passed is None:
            continue
        badges.append(t.badge("good", f"{pollutant}: meets the criteria", "✓") if passed else
                      t.badge("critical", f"{pollutant}: does not meet — "
                              + ", ".join(fc.FAILED.get(f, f) for f in failed), "✗"))
    if badges:
        st.markdown(" ".join(badges), unsafe_allow_html=True)
        st.caption("The numbers behind this verdict are on the Model page and in each city's page.")

    st.subheader("Where the data comes from")
    st.markdown(
        "- **GIOŚ archive** (Chief Inspectorate of Environmental Protection) — hourly PM10 and PM2.5 from all "
        f"stations, {train_from}–{test_year}: the history the model learned from and was tested on.\n"
        "- **GIOŚ live API** — today's measurements, fetched every hour and cleaned (duplicates, missing and "
        "impossible values, frozen sensors, spikes) before they reach the forecast — see *Data quality*.\n"
        "- **Open-Meteo** — weather forecasts: archived forecasts for the history, the current forecast for tomorrow.\n"
        "- **Assistant** — numbers come only from the result tables (the SQL is always shown), knowledge only "
        "from documents (the sources are always shown).")

    with st.expander("Technical details"):
        from smogcast.core.schema import CALENDAR_FEATURES, PM_FEATURES, WEATHER_FEATURES

        grids = "; ".join(f"{m}: {g}" for m, g in mcfg["grids"].items())
        st.markdown(
            f"- **Inputs:** {', '.join(f'`{x}`' for x in PM_FEATURES + WEATHER_FEATURES + CALENDAR_FEATURES)}, "
            "plus the city as a one-hot vector. Concentrations enter as log(1 + x); wind direction and month as "
            "sine/cosine (359° is next to 1°, December next to January).\n"
            f"- **Models compared:** {', '.join(mcfg['models'])}; parameter grids — {grids}.\n"
            f"- **Choice:** trained on {train_from}–{train_to}, the model type and settings with the lowest Brier "
            f"score on {val_year} win (rule fixed before training); then refit on {train_from}–{val_year} and "
            f"evaluated once on {test_year}.\n"
            "- **Brier score** = mean squared error of the probability (0 = perfect). **BSS** = improvement over "
            "climatology. **POD** = share of exceedance days that got a warning. **FAR** = share of warnings that "
            "were false alarms. **AUC** = how well the model ranks days from least to most dangerous.\n"
            "- **Engine:** Spark MLlib; the forecast runs as a step of the live pipeline and writes "
            "`gold.forecast_tomorrow`.")


def view_tomorrow() -> None:
    data = gold_data()
    rows = need(data, "forecast", "smogcast run-live")
    if rows is None:
        return
    warning_p = cfg()["thresholds"]["warning_probability"]
    target, issue = rows[0]["target_day"], rows[0]["issue_day"]
    header("Will there be smog tomorrow?", f"Forecast of daily limit exceedances on {target} for the 10 largest cities "
                                           f"in Poland · issued {issue} · PM10 > 50 µg/m³, PM2.5 > 25 µg/m³")

    pollutant = st.segmented_control("Pollutant", ["PM10", "PM2.5"], default="PM10",
                                     format_func=lambda p: fc.POLLUTANT_LABEL[p], label_visibility="collapsed") or "PM10"
    sel = [r for r in rows if r["pollutant"] == pollutant]
    ok = [r for r in sel if r["probability"] is not None]
    top = max(ok, key=lambda r: r["probability"]) if ok else None
    passed, failed = fc.verdict(data["acceptance"].rows if data["acceptance"] else [], pollutant)

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(t.tile("Forecast for", str(target), f"issued {issue}, 12:00 CET"), unsafe_allow_html=True)
    c2.markdown(t.tile("Cities with a warning", f"{sum(bool(r['warned']) for r in sel)} of {len(sel)}",
                       f"warning at P ≥ {fc.pct(warning_p)}"), unsafe_allow_html=True)
    c3.markdown(t.tile("Highest probability", fc.pct(top["probability"]) if top else "—",
                       top["city_name"] if top else ""), unsafe_allow_html=True)
    verdict_txt = "meets the criteria" if passed else ("fails: " + ", ".join(failed) if passed is False else "—")
    c4.markdown(t.tile("Model reliability (2025 test)", ("✓ " if passed else "✗ ") + verdict_txt,
                       "criteria K1–K4 fixed before training"), unsafe_allow_html=True)

    st.write("")
    left, right = st.columns([3, 2], gap="large")
    with left:
        st.subheader("Probability of an exceedance tomorrow")
        st.plotly_chart(charts.probabilities(rows, pollutant, warning_p), width="stretch",
                        config={"displayModeBar": False})
        st.caption("⚠ and a red bar = warning. Colour is never the only carrier of information.")
    with right:
        st.subheader("Cities")
        grid = st.columns(2)
        for i, r in enumerate(sorted(sel, key=lambda r: -(r["probability"] or -1))):
            color = t.STATUS["critical"] if r["warned"] else t.SERIES[pollutant]
            meta = (f"yesterday max {fc.num(r['pm_d1_max_ug_m3'], 0, 'µg/m³')} · morning {fc.num(r['pm_d_morning_max_ug_m3'], 0)}"
                    if r["probability"] is not None else fc.REASON.get(r["unusable_reason"], "no data"))
            grid[i % 2].markdown(
                f'<div class="sc-card"><div class="city">{t.esc(r["city_name"])}</div>'
                f'<div class="p">{fc.pct(r["probability"])}</div>{t.meter(r["probability"], color)}'
                f'{t.forecast_badge(r)}<div class="meta" style="margin-top:6px">{t.esc(meta)}</div></div>',
                unsafe_allow_html=True)

    with st.expander("Data table"):
        st.dataframe([{k: r[k] for k in ("city_name", "pollutant", "probability", "warned", "status", "pm_d1_max_ug_m3",
                                         "pm_d_morning_max_ug_m3", "t_mean_c", "wind_mean_ms", "calm_hours",
                                         "precip_sum_mm", "model")} for r in sel], width="stretch", hide_index=True)
    show_sql(data["forecast"].sql)


def view_city() -> None:
    data = gold_data()
    rows = need(data, "forecast", "smogcast run-live")
    if rows is None:
        return
    cities = cfg()["cities"]
    city_id = st.selectbox("City", list(cfg()["run"]["cities"]), format_func=lambda c: cities[c]["name"])
    header(cities[city_id]["name"], f"Forecast for {rows[0]['target_day']} and how reliable the model is in this city")
    rel = data["reliability"].rows if data["reliability"] else []
    acc = data["acceptance"].rows if data["acceptance"] else []
    warning_p = cfg()["thresholds"]["warning_probability"]
    for col, pollutant in zip(st.columns(2, gap="large"), ["PM10", "PM2.5"], strict=True):
        r = next((x for x in rows if x["city_id"] == city_id and x["pollutant"] == pollutant), None)
        with col:
            st.subheader(fc.POLLUTANT_LABEL[pollutant])
            if r is None:
                st.info("No forecast row.")
                continue
            color = t.STATUS["critical"] if r["warned"] else t.SERIES[pollutant]
            st.markdown(f'<div class="sc-card"><div class="p" style="font-size:2.6rem">{fc.pct(r["probability"])}</div>'
                        f'{t.meter(r["probability"], color)}{t.forecast_badge(r)}</div>', unsafe_allow_html=True)
            st.markdown(fc.describe(r, rel, acc, warning_p))
            if r["probability"] is not None and rel:
                st.plotly_chart(charts.reliability(rel, [pollutant], (pollutant, r["probability"])),
                                width="stretch", config={"displayModeBar": False})
                st.caption("Model calibration in 2025: a point on the diagonal = “X%” meant X%. Faded points: < 30 days.")
    if data["metrics"]:
        st.subheader("How the model did in this city (2025 test year)")
        m = [x for x in data["metrics"].rows if x["city_id"] == city_id]
        st.dataframe([{"pollutant": x["pollutant"], "model": charts.MODEL_LABEL.get(x["model"], x["model"]),
                       "days": x["n_days"], "exceedance days": x["n_exceed"], "Brier": x["brier"], "BSS": x["bss"],
                       "POD": x["pod"], "FAR": x["far"], "AUC": x["auc"]} for x in m],
                     width="stretch", hide_index=True)
        show_sql(data["metrics"].sql)


def view_model() -> None:
    data = gold_data()
    header("How good is the model?", "The 2025 test year, never seen by the model; weather from real forecasts issued "
                                     "one day earlier. Criteria fixed before training.")
    acc, metrics, rel = (need(data, k, "smogcast run-batch") for k in ("acceptance", "metrics", "reliability"))
    if acc is None or metrics is None:
        return
    for col, pollutant in zip(st.columns(2, gap="large"), ["PM10", "PM2.5"], strict=True):
        with col:
            rows = [r for r in acc if r["pollutant"] == pollutant]
            passed, _ = fc.verdict(acc, pollutant)
            operational = rows[0]["model"] if rows else None
            st.subheader(f"{fc.POLLUTANT_LABEL[pollutant]} · {str(operational).upper()} model")
            st.markdown(t.badge("good", "meets the criteria", "✓") if passed
                        else t.badge("critical", "does not meet the criteria", "✗"), unsafe_allow_html=True)
            # HTML table, not st.dataframe: in a half-width column the dataframe cut off the value columns
            st.markdown(t.table(["", "criterion", "model", "required", "result"],
                                [fc.criterion_cells(r) for r in rows], numeric=(2, 3)), unsafe_allow_html=True)
            st.plotly_chart(charts.brier(metrics, pollutant, operational), width="stretch",
                            config={"displayModeBar": False})
            st.caption("Brier score on the 2025 test year — lower is better. ★ = the model in use.")
    if rel:
        k3 = cfg()["model"]["acceptance"]
        min_days, max_gap = k3["k3_calibration_min_bin_days"], k3["k3_calibration_max_abs_gap"]
        st.subheader("Calibration: does “70%” mean 70%?")
        st.plotly_chart(charts.reliability(rel, ["PM10", "PM2.5"], min_days=min_days), width="stretch",
                        config={"displayModeBar": False})
        st.markdown(fc.calibration_text(rel, min_days, max_gap))
    st.info("PM10 is harder to forecast than PM2.5: PM10 also contains dust from streets, construction sites and soil, "
            "which the weather forecast does not show, and its exceedances are rarer. The PM10 model ranks days well "
            "from least to most dangerous, but misses many exceedance days (POD in the table above), and its "
            "calibration is close to the limit. Ask the assistant for details.")
    show_sql(data["acceptance"].sql)


def view_data_quality() -> None:
    data = gold_data()
    header("Live data quality", "What happened to the stream readings: how many were valid, how many were rejected "
                                "and why")
    rows = need(data, "dq", "smogcast run-live")
    if rows is None:
        return
    labels = {"live": "Live GIOŚ API", "fault_replay": "Fault demo (replay with injected faults)"}
    sources = sorted({r["source"] for r in rows})
    source = st.segmented_control("Source", sources, default=sources[0], format_func=lambda s: labels.get(s, s),
                                  label_visibility="collapsed") or sources[0]
    m = {r["metric"]: r for r in rows if r["source"] == source}

    def v(key: str) -> float:
        return m[key]["value"] if key in m else 0.0

    quarantined = v("quarantined")
    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(t.tile("Records received", count(v("records_received")),
                       f"of which re-sent: {count(v('resent_duplicates'))}"), unsafe_allow_html=True)
    c2.markdown(t.tile("Unique readings", count(v("unique_readings")), "station × hour"), unsafe_allow_html=True)
    c3.markdown(t.tile("Valid", f"{m.get('silver_valid', {}).get('pct_of_unique', 0):.1f}%",
                       f"{count(v('silver_valid'))} readings"), unsafe_allow_html=True)
    c4.markdown(t.tile("Rejected", count(quarantined + v("late_rejected")),
                       f"quarantine {quarantined:.0f} · late {v('late_rejected'):.0f}"), unsafe_allow_html=True)
    st.write("")
    st.plotly_chart(charts.data_quality(rows, source), width="stretch", config={"displayModeBar": False})
    if source == "fault_replay":
        st.caption("Demo: real measurements from Kraków (17–21 Jan 2025) with deliberately injected faults. The real "
                   "smog episode of 20 January passes without flags — a real signal, not a fault.")
    show_sql(data["dq"].sql)


EXAMPLES = [
    "Will there be smog in Kraków tomorrow?",
    "What should I do when the PM10 information level is exceeded?",
    "Which city had the best PM10 Brier score in 2025?",
    "Why is PM10 harder to forecast than PM2.5?",
    "What are the new EU limits for PM2.5 from 2030?",
]


def render_answer(a) -> None:
    st.markdown(a.text)
    if a.sql:
        show_sql(a.sql)
    if a.rows and a.route == "data":
        with st.expander(f"Query result ({len(a.rows)} rows)"):
            st.dataframe(a.rows, width="stretch", hide_index=True)
    if a.sources:
        with st.expander(f"Sources ({len(a.sources)})"):
            for i, (c, score) in enumerate(a.sources, 1):
                st.markdown(f"**[{i}] {t.esc(c.title)}** — {t.esc(c.location)} · relevance {score:.2f}")
                st.markdown(f'<div class="sc-src">{t.esc(c.text[:900])}</div>', unsafe_allow_html=True)
    for note in a.notes:
        st.caption(f"ℹ️ {note}")


def view_chat() -> None:
    header("Assistant", "Ask about the forecast, the quality of the model and the data, or about air-quality standards "
                        "and health advice. Numbers come from tables (always with the SQL shown), knowledge from "
                        "documents (always with the sources).")
    if "messages" not in st.session_state:
        st.session_state.messages = []
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            if msg["role"] == "assistant":
                render_answer(msg["answer"])
            else:
                st.markdown(msg["content"])
    picked = st.pills("Example questions", EXAMPLES, label_visibility="collapsed") if not st.session_state.messages else None
    question = st.chat_input("Ask a question…") or picked
    if not question:
        return
    st.session_state.messages.append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.markdown(question)
    with st.chat_message("assistant"):
        with st.spinner("Looking for the answer…"):
            history = [{"role": m["role"], "content": m["content"] if m["role"] == "user" else m["answer"].text}
                       for m in st.session_state.messages[:-1]]
            answer = assistant().ask(question, history)
        render_answer(answer)
    st.session_state.messages.append({"role": "assistant", "content": answer.text, "answer": answer})

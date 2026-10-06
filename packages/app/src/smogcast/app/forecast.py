"""Tomorrow's forecast in words — deterministic, no language model involved.

The probability, the explanatory values and the track record ("in the test year, at forecasts of
40–50% the limit was exceeded on 38% of days") all come from gold tables. Texts are English, like the
whole user interface.
"""

from __future__ import annotations

from smogcast.app.gold import GoldReader

FORECAST_SQL = (
    "SELECT * FROM gold.forecast_tomorrow "
    "WHERE target_day = (SELECT MAX(target_day) FROM gold.forecast_tomorrow) ORDER BY pollutant, city_id"
)
# Calibration of each pollutant's OPERATIONAL model on the test year: what a given probability meant.
RELIABILITY_SQL = (
    "SELECT r.pollutant, r.model, r.bin, r.bin_from, r.bin_to, r.n_days, r.mean_probability, r.observed_frequency "
    "FROM gold.reliability r JOIN gold.model_selection s ON r.pollutant = s.pollutant AND r.model = s.model "
    "WHERE s.operational AND r.split = 'test' ORDER BY r.pollutant, r.bin"
)
METRICS_SQL = (
    "SELECT m.* FROM gold.backtest_metrics m WHERE m.split = 'test' "
    "ORDER BY m.pollutant, m.city_id, m.model"
)
ACCEPTANCE_SQL = "SELECT * FROM gold.acceptance ORDER BY pollutant, criterion"
DQ_SQL = "SELECT * FROM gold.dq_summary ORDER BY source, metric"

POLLUTANT_LABEL = {"PM10": "PM10", "PM2.5": "PM2.5"}
REASON = {
    "no_valid_pm_yesterday": "no valid daily mean for yesterday (too few measured hours)",
    "no_complete_weather_forecast": "no complete weather forecast for tomorrow",
}
FAILED = {"K1": "better than climatology", "K2": "better than persistence", "K3": "calibration",
          "K4a": "detection of exceedance days", "K4b": "false alarm ratio"}


def pct(p: float | None) -> str:
    return "—" if p is None else f"{round(p * 100):d}%"


def num(x: float | None, digits: int = 0, unit: str = "") -> str:
    if x is None:
        return "—"
    return f"{x:.{digits}f} {unit}".strip()


def bin_for(probability: float, reliability: list[dict], pollutant: str) -> dict | None:
    """The calibration bin of the operational model that contains this probability (10 bins of 0.1)."""
    b = min(int(probability * 10), 9)
    return next((r for r in reliability if r["pollutant"] == pollutant and r["bin"] == b), None)


def worst_bin(reliability: list[dict], pollutant: str, min_days: int) -> dict | None:
    """The judged calibration bin (≥ ``min_days`` days) furthest from the diagonal — the one criterion K3 looks at."""
    bins = [r for r in reliability if r["pollutant"] == pollutant and r["n_days"] >= min_days
            and r["mean_probability"] is not None and r["observed_frequency"] is not None]
    return max(bins, key=lambda r: abs(r["mean_probability"] - r["observed_frequency"]), default=None)


def calibration_text(reliability: list[dict], min_days: int, max_gap: float) -> str:
    """How to read the calibration chart (markdown), with each pollutant's worst judged bin as a worked example."""
    lines = [
        "**How to read this chart.** Every forecast of the 2025 test year falls into one of ten groups by its "
        "probability: 0–10%, 10–20%, … 90–100%. Each point is one such group of days:",
        "- **horizontal axis** — the average probability the model gave on those days;",
        "- **vertical axis** — on what share of those days the daily limit was actually exceeded;",
        "- **grey diagonal** — perfect calibration: of all the days forecast at 70%, the limit is exceeded on 70%. "
        "A point **below** the diagonal means the forecasts were too high (exceedances happened less often than "
        "the model said); **above** it — too low;",
        f"- **faded points** — groups with fewer than {min_days} days: too few to judge, so they are not counted.",
        "",
        f"Criterion **K3** requires every group with at least {min_days} days to lie within "
        f"{max_gap * 100:.0f} percentage points of the diagonal. Calibration is what gives the percentage on the "
        "*Tomorrow* page its meaning: it says how far a “70%” can be trusted.",
    ]
    examples = []
    for p in sorted({r["pollutant"] for r in reliability}):
        b = worst_bin(reliability, p, min_days)
        if b is None:
            continue
        gap = b["mean_probability"] - b["observed_frequency"]
        side = "below" if gap > 0 else "above"
        verdict_txt = "within the limit" if abs(gap) <= max_gap else "more than allowed, so K3 is not met"
        examples.append(
            f"- **{POLLUTANT_LABEL.get(p, p)}:** the largest gap is in the {pct(b['bin_from'])}–{pct(b['bin_to'])} "
            f"group — the model said {pct(b['mean_probability'])} on average, the limit was exceeded on "
            f"{pct(b['observed_frequency'])} of those {b['n_days']} days: {abs(gap) * 100:.0f} percentage points "
            f"{side} the diagonal, {verdict_txt}.")
    if examples:
        lines += ["", "**What it shows for the model in use:**", *examples]
    return "\n".join(lines)


# Plain-language name, comparison and value format of each acceptance criterion (gold.acceptance stores the
# technical description; the interface shows these).
CRITERION = {
    "K1": ("beats climatology (Brier skill score)", ">", "num2"),
    "K2": ("beats persistence (Brier score)", "<", "num3"),
    "K3": ("calibrated (largest gap)", "≤", "pp"),
    "K4a": ("catches exceedance days (POD)", "≥", "pct"),
    "K4b": ("few false alarms (FAR)", "≤", "pct"),
}


def criterion_cells(row: dict) -> list[str]:
    """gold.acceptance row → [criterion, what it checks, model value, required, result] as display text."""
    what, op, kind = CRITERION.get(row["criterion"], (row["description"], "", "num3"))

    def fmt(x: float | None) -> str:
        if x is None:
            return "—"
        return {"num2": f"{x:.2f}", "num3": f"{x:.3f}", "pp": f"{x * 100:.0f} pp", "pct": pct(x)}[kind]

    return [row["criterion"], what, fmt(row["value"]), f"{op} {fmt(row['threshold'])}".strip(),
            "✓ met" if row["passed"] else "✗ not met"]


def verdict(acceptance: list[dict], pollutant: str) -> tuple[bool | None, list[str]]:
    """(all criteria passed?, names of failed criteria) for the pollutant's operational model."""
    rows = [r for r in acceptance if r["pollutant"] == pollutant]
    if not rows:
        return None, []
    failed = [r["criterion"] for r in rows if not r["passed"]]
    return not failed, failed


def describe(row: dict, reliability: list[dict], acceptance: list[dict], warning_p: float) -> str:
    """One forecast row → a short, honest paragraph (markdown)."""
    pol = POLLUTANT_LABEL.get(row["pollutant"], row["pollutant"])
    head = f"**{row['city_name']}, {row['target_day']} — {pol}:** "
    if row["probability"] is None:
        reason = REASON.get(row.get("unusable_reason"), row.get("unusable_reason") or "no data")
        return head + f"no forecast — {reason}."
    p = row["probability"]
    level = "**warning**" if row["warned"] else "no warning"
    parts = [head + f"probability of exceeding the daily limit ({num(row['limit_ug_m3'])} µg/m³) = **{pct(p)}** "
             f"— {level} (warning threshold {pct(warning_p)})."]
    parts.append(
        f"Yesterday's highest station daily mean: {num(row['pm_d1_max_ug_m3'], 0, 'µg/m³')}, "
        f"this morning: {num(row['pm_d_morning_max_ug_m3'], 0, 'µg/m³')}. Weather forecast for tomorrow: "
        f"{num(row['t_mean_c'], 1, '°C')}, wind {num(row['wind_mean_ms'], 1, 'm/s')}, "
        f"{num(row['calm_hours'])} h of calm wind, precipitation {num(row['precip_sum_mm'], 1, 'mm')}."
    )
    b = bin_for(p, reliability, row["pollutant"])
    if b and b["n_days"]:
        parts.append(
            f"How reliable such a “{pct(p)}” is: in the 2025 test year, at forecasts of "
            f"{pct(b['bin_from'])}–{pct(b['bin_to'])} the limit was exceeded on {pct(b['observed_frequency'])} of days "
            f"({b['n_days']} days)."
        )
    ok, failed = verdict(acceptance, row["pollutant"])
    if ok is False:
        parts.append(f"Note: the {pol} model does not meet the reliability criteria fixed before training ("
                     + ", ".join(FAILED.get(f, f) for f in failed) + ").")
    parts.append(f"Model: {str(row['model']).upper()} ({row['model_version']}), issued {row['issue_day']}.")
    return " ".join(parts)


def load(reader: GoldReader) -> dict:
    """Everything the forecast views need, each with the SQL that produced it."""
    out = {}
    for key, sql, table in (("forecast", FORECAST_SQL, "forecast_tomorrow"), ("reliability", RELIABILITY_SQL, "reliability"),
                            ("metrics", METRICS_SQL, "backtest_metrics"), ("acceptance", ACCEPTANCE_SQL, "acceptance"),
                            ("dq", DQ_SQL, "dq_summary")):
        out[key] = reader.query(sql, max_rows=1000) if reader.exists(table) else None
    return out

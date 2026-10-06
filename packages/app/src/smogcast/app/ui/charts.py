"""Plotly charts of the web interface. Each chart has one job; values are also in the table views."""

from __future__ import annotations

import plotly.graph_objects as go

from smogcast.app.forecast import POLLUTANT_LABEL, pct
from smogcast.app.ui import theme as t

ROW_PX = 34  # per bar: keeps bars thin (≈ 18 px) with air between them


def probabilities(rows: list[dict], pollutant: str, warning_p: float) -> go.Figure:
    """Tomorrow's probability per city — magnitude against the warning threshold, highest on top.
    Warned cities carry the status colour AND a ⚠ in their label (never colour alone)."""
    data = sorted((r for r in rows if r["pollutant"] == pollutant), key=lambda r: r["probability"] or -1)
    names = [("⚠ " if r["warned"] else "") + r["city_name"] for r in data]
    values = [r["probability"] or 0 for r in data]
    colors = [t.STATUS["critical"] if r["warned"] else t.SERIES[pollutant] for r in data]
    text = [pct(r["probability"]) if r["probability"] is not None else "no forecast" for r in data]
    hover = [f"<b>{r['city_name']}</b><br>P({POLLUTANT_LABEL[pollutant]} exceedance) = {pct(r['probability'])}"
             f"<br>yesterday max: {r['pm_d1_max_ug_m3'] or 0:.0f} µg/m³ · limit {r['limit_ug_m3']:.0f}"
             for r in data]
    fig = go.Figure(go.Bar(x=values, y=names, orientation="h", marker=dict(color=colors, cornerradius=4),
                           text=text, textposition="outside", textfont=dict(color=t.INK_2),
                           hovertext=hover, hoverinfo="text", cliponaxis=False))
    fig.add_vline(x=warning_p, line_width=1, line_color=t.MUTED)
    fig.add_annotation(x=warning_p, y=1.0, yref="paper", yanchor="bottom", showarrow=False,
                       text=f"warning threshold {pct(warning_p)}", font=dict(color=t.MUTED, size=11))
    fig = t.plot_layout(fig, height=ROW_PX * len(data) + 50, percent_x=True)
    fig.update_xaxes(range=[0, 1.08], dtick=0.25)
    fig.update_yaxes(showgrid=False)
    fig.update_layout(margin=dict(l=8, r=24, t=28, b=8))
    return fig


def reliability(rows: list[dict], pollutants: list[str], highlight: tuple[str, float] | None = None,
                min_days: int = 30) -> go.Figure:
    """Calibration on the test year: forecast probability vs how often the exceedance happened.
    The diagonal = perfect calibration. Bins with fewer than ``min_days`` days are faded (too few to judge)."""
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=[0, 1], y=[0, 1], mode="lines", line=dict(color=t.BASELINE, width=1),
                             hoverinfo="skip", showlegend=False))
    for p in pollutants:
        data = sorted((r for r in rows if r["pollutant"] == p), key=lambda r: r["bin"])
        if not data:
            continue
        fig.add_trace(go.Scatter(
            x=[r["mean_probability"] for r in data], y=[r["observed_frequency"] for r in data],
            mode="lines+markers", name=POLLUTANT_LABEL[p], line=dict(color=t.SERIES[p], width=2),
            marker=dict(size=9, color=t.SERIES[p], line=dict(color=t.SURFACE, width=2),
                        opacity=[1.0 if r["n_days"] >= min_days else 0.35 for r in data]),
            hovertext=[f"<b>{POLLUTANT_LABEL[p]}, forecasts {pct(r['bin_from'])}–{pct(r['bin_to'])}</b><br>"
                       f"mean forecast {pct(r['mean_probability'])}<br>exceeded on {pct(r['observed_frequency'])} of days"
                       f"<br>{r['n_days']} days" for r in data],
            hoverinfo="text"))
    if highlight:
        p, prob = highlight
        fig.add_vline(x=prob, line_width=1, line_color=t.SERIES.get(p, t.ACCENT))
        fig.add_annotation(x=prob, y=1.0, yref="paper", yanchor="bottom", showarrow=False,
                           text=f"tomorrow: {pct(prob)}", font=dict(color=t.INK_2, size=11))
    fig = t.plot_layout(fig, height=360, x_title="forecast probability",
                        y_title="observed exceedance frequency", percent_x=True, percent_y=True,
                        legend=len(pollutants) > 1)
    fig.update_xaxes(range=[0, 1], dtick=0.2)
    fig.update_yaxes(range=[0, 1.02], dtick=0.2)
    fig.update_layout(margin=dict(l=8, r=24, t=30, b=8))
    return fig


MODEL_LABEL = {"logistic": "logistic regression", "gbt": "GBT (gradient-boosted trees)",
               "persistence": "persistence (“tomorrow like today”)", "climatology": "climatology (calendar)"}


def brier(metrics: list[dict], pollutant: str, operational: str, city_id: str = "ALL") -> go.Figure:
    """Emphasis: the operational model in the accent colour, the reference forecasts in grey.
    Lower Brier = better."""
    data = [r for r in metrics if r["pollutant"] == pollutant and r["city_id"] == city_id]
    data.sort(key=lambda r: -r["brier"])
    names = [MODEL_LABEL.get(r["model"], r["model"]) + (" ★" if r["model"] == operational else "") for r in data]
    colors = [t.SERIES[pollutant] if r["model"] == operational else t.DEEMPHASIS for r in data]
    fig = go.Figure(go.Bar(x=[r["brier"] for r in data], y=names, orientation="h",
                           marker=dict(color=colors, cornerradius=4), text=[f"{r['brier']:.3f}" for r in data],
                           textposition="outside", textfont=dict(color=t.INK_2), cliponaxis=False,
                           hovertext=[f"<b>{MODEL_LABEL.get(r['model'], r['model'])}</b><br>Brier {r['brier']:.4f}"
                                      f"<br>BSS {r['bss']:.2f}" if r["bss"] is not None else
                                      f"<b>{r['model']}</b><br>Brier {r['brier']:.4f}" for r in data],
                           hoverinfo="text"))
    # No axis title: in a half-width column a centred title was clipped; the view says "lower = better" in a caption.
    fig = t.plot_layout(fig, height=ROW_PX * len(data) + 30)
    fig.update_yaxes(showgrid=False)
    fig.update_xaxes(range=[0, max((r["brier"] for r in data), default=0.1) * 1.25])
    return fig


DQ_LABEL = {"silver_valid": "valid (used for forecasts)", "flag_frozen": "flag: frozen reading",
            "flag_spike": "flag: spike", "flag_drift": "flag: drift", "late_rejected": "rejected: late",
            "quarantined_missing_value": "quarantine: missing value", "quarantined_below_min": "quarantine: negative value",
            "quarantined_above_max": "quarantine: > 1000 µg/m³", "quarantined_unknown_station": "quarantine: unknown station"}


def data_quality(rows: list[dict], source: str) -> go.Figure:
    """What happened to the unique readings of one source (share of unique readings)."""
    data = [r for r in rows if r["source"] == source and r["metric"] in DQ_LABEL]
    data.sort(key=lambda r: r["pct_of_unique"] or 0)
    fig = go.Figure(go.Bar(
        x=[(r["pct_of_unique"] or 0) / 100 for r in data], y=[DQ_LABEL[r["metric"]] for r in data], orientation="h",
        marker=dict(color=[t.ACCENT if r["metric"] == "silver_valid" else t.DEEMPHASIS for r in data], cornerradius=4),
        text=[f"{r['pct_of_unique']:.2f}% ({r['value']:.0f})" for r in data],
        textposition="outside", textfont=dict(color=t.INK_2), cliponaxis=False, hoverinfo="skip"))
    fig = t.plot_layout(fig, height=ROW_PX * len(data) + 40, x_title="share of unique readings", percent_x=True)
    fig.update_yaxes(showgrid=False)
    fig.update_xaxes(range=[0, 1.18])
    return fig

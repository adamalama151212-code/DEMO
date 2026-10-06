"""Visual language of the web interface: colour tokens, chart layout, stat tiles and badges.

Colours come from a validated data-visualisation palette (checked for colour-vision deficiency and
contrast): blue = magnitude (probability), orange = a second series, status colours only for state and
always paired with an icon and a word. Text never wears a series colour.
"""

from __future__ import annotations

import html

# ---------------------------------------------------------------- tokens
SURFACE = "#fcfcfb"          # chart and card surface
PAGE = "#f9f9f7"             # page plane
INK = "#0b0b0b"              # primary text
INK_2 = "#52514e"            # secondary text
MUTED = "#898781"            # axis labels, captions
GRID = "#e1e0d9"             # hairline gridlines
BASELINE = "#c3c2b7"         # axis / baseline
BORDER = "rgba(11,11,11,0.10)"

SERIES = {"PM10": "#2a78d6", "PM2.5": "#eb6834"}    # categorical slots 1–2 (validated pair)
ACCENT = SERIES["PM10"]
DEEMPHASIS = "#c3c2b7"                              # context bars in emphasis charts

STATUS = {"good": "#0ca30c", "warning": "#fab219", "serious": "#ec835a", "critical": "#d03b3b"}
SUCCESS_TEXT = "#006300"

FONT = 'system-ui, -apple-system, "Segoe UI", Roboto, sans-serif'

# Passed to `streamlit run` (smogcast-app ui), so the theme does not depend on the working directory.
STREAMLIT_THEME = {
    "theme.base": "light", "theme.primaryColor": ACCENT, "theme.backgroundColor": PAGE,
    "theme.secondaryBackgroundColor": "#f0efec", "theme.textColor": INK, "theme.font": "sans serif",
}

CSS = f"""
<style>
  .block-container {{ padding-top: 2.2rem; max-width: 1280px; }}
  h1, h2, h3 {{ letter-spacing: -0.01em; }}
  .sc-sub {{ color: {INK_2}; margin-top: -0.6rem; margin-bottom: 1.2rem; }}
  .sc-tile {{ background: {SURFACE}; border: 1px solid {BORDER}; border-radius: 12px; padding: 14px 16px;
             height: 100%; }}
  .sc-tile .lbl {{ color: {INK_2}; font-size: 0.82rem; }}
  .sc-tile .val {{ color: {INK}; font-size: 1.7rem; font-weight: 600; line-height: 1.25; margin-top: 2px; }}
  .sc-tile .note {{ color: {MUTED}; font-size: 0.78rem; margin-top: 2px; }}
  .sc-card {{ background: {SURFACE}; border: 1px solid {BORDER}; border-radius: 12px; padding: 12px 14px;
             margin-bottom: 12px; }}
  .sc-card .city {{ color: {INK}; font-weight: 600; font-size: 0.95rem; }}
  .sc-card .p {{ color: {INK}; font-size: 2rem; font-weight: 600; line-height: 1.15; margin: 4px 0 2px; }}
  .sc-card .meta {{ color: {MUTED}; font-size: 0.76rem; }}
  .sc-meter {{ height: 6px; border-radius: 3px; margin: 6px 0 8px; overflow: hidden; }}
  .sc-meter > div {{ height: 100%; border-radius: 3px; }}
  .sc-badge {{ display: inline-flex; align-items: center; gap: 6px; font-size: 0.76rem; color: {INK_2};
              border: 1px solid {BORDER}; border-radius: 999px; padding: 1px 9px; background: white; }}
  .sc-badge .dot {{ width: 8px; height: 8px; border-radius: 50%; display: inline-block; }}
  .sc-src {{ color: {INK_2}; font-size: 0.85rem; border-left: 3px solid {GRID}; padding-left: 10px;
            margin: 8px 0; }}
  /* compact table that wraps instead of scrolling sideways in a half-width column */
  .sc-table {{ width: 100%; border-collapse: collapse; font-size: 0.86rem; margin: 10px 0 6px;
              background: {SURFACE}; border: 1px solid {BORDER}; border-radius: 8px; }}
  .sc-table th {{ color: {INK_2}; font-weight: 500; text-align: left; padding: 6px 8px;
                 border-bottom: 1px solid {GRID}; }}
  .sc-table td {{ color: {INK}; padding: 6px 8px; border-bottom: 1px solid {GRID}; vertical-align: top; }}
  .sc-table tr:last-child td {{ border-bottom: none; }}
  .sc-table .num {{ text-align: right; white-space: nowrap; font-variant-numeric: tabular-nums; }}
  .sc-step {{ background: {SURFACE}; border: 1px solid {BORDER}; border-radius: 12px; padding: 12px 14px;
             height: 100%; }}
  .sc-step .n {{ color: {ACCENT}; font-weight: 600; font-size: 0.8rem; }}
  .sc-step .h {{ color: {INK}; font-weight: 600; margin: 2px 0 4px; }}
  .sc-step .b {{ color: {INK_2}; font-size: 0.85rem; line-height: 1.4; }}
</style>
"""


def esc(x) -> str:
    return html.escape(str(x))


def tile(label: str, value: str, note: str = "") -> str:
    """Stat tile: label · value · optional note (rendered with st.markdown(unsafe_allow_html=True))."""
    note_html = f'<div class="note">{esc(note)}</div>' if note else ""
    return f'<div class="sc-tile"><div class="lbl">{esc(label)}</div><div class="val">{esc(value)}</div>{note_html}</div>'


def table(header: list[str], rows: list[list[str]], numeric: tuple[int, ...] = ()) -> str:
    """Small HTML table (escaped); columns listed in ``numeric`` are right-aligned and never wrap."""
    def cells(tag: str, values: list[str]) -> str:
        num = ' class="num"'
        return "".join(f"<{tag}{num if i in numeric else ''}>{esc(v)}</{tag}>" for i, v in enumerate(values))

    body = "".join(f"<tr>{cells('td', r)}</tr>" for r in rows)
    return f'<table class="sc-table"><thead><tr>{cells("th", header)}</tr></thead><tbody>{body}</tbody></table>'


def step(number: int, title: str, body: str) -> str:
    """Numbered step card for the how-it-works page."""
    return (f'<div class="sc-step"><div class="n">STEP {number}</div><div class="h">{esc(title)}</div>'
            f'<div class="b">{esc(body)}</div></div>')


def badge(status: str, text: str, icon: str = "") -> str:
    """Status never by colour alone: a coloured dot + an icon + a word."""
    return (f'<span class="sc-badge"><span class="dot" style="background:{STATUS[status]}"></span>'
            f'{esc(icon + " " if icon else "")}{esc(text)}</span>')


def forecast_badge(row: dict) -> str:
    if row.get("probability") is None:
        return badge("warning", "no forecast", "–")
    if row.get("warned"):
        return badge("critical", "warning", "⚠")
    return badge("good", "no warning", "✓")


def meter(probability: float | None, color: str) -> str:
    """Probability as a filled track (same-hue lighter track, so the state reads across the whole bar)."""
    width = 0 if probability is None else max(min(probability, 1.0), 0.0) * 100
    # track = the fill colour at ~18% opacity: a lighter step of the same hue, whatever the state colour
    return (f'<div class="sc-meter" style="background:{color}2e">'
            f'<div style="width:{width:.0f}%;background:{color}"></div></div>')


def plot_layout(fig, height: int, x_title: str = "", y_title: str = "", percent_x: bool = False,
                percent_y: bool = False, legend: bool = False):
    """Recessive chrome: hairline solid grid, muted axis text, surface background, system sans."""
    fig.update_layout(
        height=height, margin=dict(l=8, r=24, t=8, b=8), paper_bgcolor=SURFACE, plot_bgcolor=SURFACE,
        font=dict(family=FONT, color=INK_2, size=13), showlegend=legend, bargap=0.45,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0, font=dict(color=INK_2)),
        hoverlabel=dict(bgcolor="white", bordercolor=GRID, font=dict(family=FONT, color=INK, size=13)),
    )
    axis = dict(gridcolor=GRID, gridwidth=1, zeroline=False, linecolor=BASELINE, tickfont=dict(color=MUTED),
                title_font=dict(color=MUTED, size=12))
    fig.update_xaxes(**axis, title_text=x_title, **({"tickformat": ".0%"} if percent_x else {}))
    fig.update_yaxes(**axis, title_text=y_title, **({"tickformat": ".0%"} if percent_y else {}))
    return fig

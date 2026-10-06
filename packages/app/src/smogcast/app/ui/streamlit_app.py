"""smogcast web interface — run with ``smogcast-app ui`` (or ``streamlit run`` on this file).

The shell only: page setup, navigation and sidebar. The views are in views.py.
"""

from __future__ import annotations

import streamlit as st

from smogcast.app.ui import theme as t
from smogcast.app.ui import views as v

v.setup()

with st.sidebar:
    st.markdown("### 🌫️ smogcast")
    st.caption("Tomorrow's PM10 and PM2.5 limit exceedance forecast · data: GIOŚ and Open-Meteo")
page = st.navigation([
    # first in the menu by the author's choice; the start page stays "Tomorrow"
    st.Page(v.view_info, title="Info", icon="ℹ️", url_path="info"),
    st.Page(v.view_tomorrow, title="Tomorrow", icon="📍", url_path="tomorrow", default=True),
    st.Page(v.view_city, title="City", icon="🏙️", url_path="city"),
    st.Page(v.view_model, title="Model", icon="📈", url_path="model"),
    st.Page(v.view_data_quality, title="Data quality", icon="🧹", url_path="data-quality"),
    st.Page(v.view_chat, title="Assistant", icon="💬", url_path="assistant"),
])
with st.sidebar:
    st.divider()
    ok, model = v.llm_status()
    st.markdown(t.badge("good", f"model: {model}", "●") if ok else t.badge("warning", "language model unavailable", "○"),
                unsafe_allow_html=True)
    st.caption(f"environment: {v.cfg()['env']}")
    if st.button("Refresh data", width="stretch"):
        v.gold_data.clear()
        v.llm_status.clear()
        st.rerun()
page.run()

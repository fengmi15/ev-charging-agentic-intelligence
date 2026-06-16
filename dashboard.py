"""
EV Charging Network — Live Operations Dashboard

Consumes ev.alerts and ev.reports Kafka topics in real-time and renders:
1. Live folium map — 110 stations, color-coded by status/alert severity
2. Alert feed — real-time severity-colored incident stream
3. City grid load chart — per-city power utilization
4. SoC distribution — active charging session health
5. AI incident reports — latest Claude-generated ops reports
6. KPI summary row — fleet-wide health metrics
"""
import json
import time
import os
from pathlib import Path
from collections import defaultdict

import pandas as pd
import numpy as np
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import folium
from streamlit_folium import st_folium
from confluent_kafka import Consumer, KafkaError

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="EV Fleet Operations — Germany",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Styling ────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .stApp { background-color: #ffffff !important; color: #1e293b !important; }
    .main { background-color: #ffffff !important; }
    .block-container { padding-top: 1rem; padding-bottom: 1rem; }
    p, h1, h2, h3, h4, label, span, div { color: #1e293b; }
    .stMetric label { color: #64748b !important; font-size: 13px !important; }
    .stMetric [data-testid="stMetricValue"] { color: #1e293b !important; font-weight: 700; }
    .alert-critical {
        background: #fef2f2;
        border-left: 4px solid #ef4444;
        padding: 8px 12px;
        border-radius: 4px;
        margin: 4px 0;
        color: #1e293b;
    }
    .alert-high {
        background: #fffbeb;
        border-left: 4px solid #f59e0b;
        padding: 8px 12px;
        border-radius: 4px;
        margin: 4px 0;
        color: #1e293b;
    }
    .alert-medium {
        background: #eff6ff;
        border-left: 4px solid #3b82f6;
        padding: 8px 12px;
        border-radius: 4px;
        margin: 4px 0;
        color: #1e293b;
    }
    .alert-low {
        background: #f0fdf4;
        border-left: 4px solid #22c55e;
        padding: 8px 12px;
        border-radius: 4px;
        margin: 4px 0;
        color: #1e293b;
    }
    .report-box {
        background: #f8fafc;
        border: 1px solid #e2e8f0;
        border-radius: 8px;
        padding: 16px;
        margin: 8px 0;
        color: #1e293b;
    }
    .stExpander { border: 1px solid #e2e8f0 !important; border-radius: 8px !important; }
    .streamlit-expanderHeader { background-color: #f8fafc !important; color: #1e293b !important; }
    [data-testid="stExpander"] summary { background-color: #f8fafc !important; color: #1e293b !important; }
    [data-testid="stSidebar"] { background-color: #f8fafc !important; }
    .stInfo { background-color: #eff6ff !important; color: #1e293b !important; }
</style>
""", unsafe_allow_html=True)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

# ── Data loading ───────────────────────────────────────────────────────────────
@st.cache_data(ttl=0)
def load_stations():
    return pd.read_csv("data/stations.csv")

@st.cache_data(ttl=0)
def load_telemetry():
    return pd.read_csv("data/telemetry_faulted.csv", low_memory=False)

def load_reports():
    reports = []
    report_dir = Path("data/reports")
    for f in sorted(report_dir.glob("*.json"), reverse=True)[:5]:
        with open(f) as fh:
            reports.append(json.load(fh))
    return reports


def consume_recent_alerts(max_alerts=200):
    """Pull recent alerts from ev.alerts topic."""
    alerts = []
    try:
        c = Consumer({
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": f"dashboard-{int(time.time() * 1000)}",  # unique group = always read from start
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        c.subscribe(["ev.alerts"])
        deadline = time.time() + 5.0  # give more time to drain topic
        while time.time() < deadline and len(alerts) < max_alerts:
            msg = c.poll(timeout=0.5)
            if msg and not msg.error():
                try:
                    alerts.append(json.loads(msg.value().decode("utf-8")))
                except Exception:
                    pass
        c.close()
    except Exception:
        pass
    return alerts


# ── Color maps ─────────────────────────────────────────────────────────────────
SEVERITY_COLOR = {
    "CRITICAL": "#ef4444",
    "HIGH":     "#f59e0b",
    "MEDIUM":   "#3b82f6",
    "LOW":      "#22c55e",
    None:       "#94a3b8",
}

FAULT_ICON = {
    "THERMAL_RUNAWAY":  "🔥",
    "GRID_CONGESTION":  "⚡",
    "PHANTOM_SESSION":  "👻",
    "SOC_PLATEAU":      "🔋",
    "FIRMWARE_FAULT":   "⚠️",
    "GHOST_STATION":    "📡",
}

CITY_NAMES = {
    "BER": "Berlin", "MUC": "Munich", "HAM": "Hamburg",
    "FRA": "Frankfurt", "CGN": "Cologne", "STU": "Stuttgart",
    "DUS": "Düsseldorf", "LEJ": "Leipzig",
}


# ── Map builder ────────────────────────────────────────────────────────────────
def build_map(stations_df, alerts):
    station_severity = {}
    for a in alerts:
        sid = a.get("station_id", "")
        sev = a.get("severity", "LOW")
        existing = station_severity.get(sid)
        priority = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
        if existing is None or priority[sev] < priority.get(existing, 99):
            station_severity[sid] = sev

    m = folium.Map(
        location=[51.1657, 10.4515],
        zoom_start=6,
        tiles="CartoDB positron",
    )

    for _, row in stations_df.iterrows():
        sid = row["station_id"]
        sev = station_severity.get(sid)
        color = SEVERITY_COLOR.get(sev, "#22c55e")
        radius = {"CRITICAL": 10, "HIGH": 8, "MEDIUM": 6}.get(sev, 4)

        popup_html = f"""
        <b>{sid}</b><br>
        City: {row['city_name']}<br>
        Connector: {row['connector_type']}<br>
        Max Power: {row['max_power_kw']} kW<br>
        Firmware: {row['firmware_version']}<br>
        Status: <b style='color:{color}'>{sev or 'NORMAL'}</b>
        """
        folium.CircleMarker(
            location=[row["lat"], row["lon"]],
            radius=radius,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.8,
            popup=folium.Popup(popup_html, max_width=200),
            tooltip=f"{sid} — {sev or 'NORMAL'}",
        ).add_to(m)

    legend_html = """
    <div style="position:fixed; bottom:30px; left:30px; z-index:1000;
                background:white; padding:12px; border-radius:8px;
                border:1px solid #e2e8f0; font-size:12px; color:#1e293b;">
        <b>Station Status</b><br>
        <span style='color:#ef4444'>●</span> CRITICAL &nbsp;
        <span style='color:#f59e0b'>●</span> HIGH<br>
        <span style='color:#3b82f6'>●</span> MEDIUM &nbsp;
        <span style='color:#22c55e'>●</span> NORMAL
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))
    return m


# ── Chart builders ─────────────────────────────────────────────────────────────
def build_grid_load_chart(telemetry_df):
    latest = (
        telemetry_df.groupby("city_code")["grid_load_pct"]
        .max()
        .reset_index()
        .sort_values("grid_load_pct", ascending=True)
    )
    latest["city_name"] = latest["city_code"].map(CITY_NAMES)
    latest["color"] = latest["grid_load_pct"].apply(
        lambda x: "#ef4444" if x > 120 else "#f59e0b" if x > 90 else "#22c55e"
    )

    fig = go.Figure(go.Bar(
        x=latest["grid_load_pct"],
        y=latest["city_name"],
        orientation="h",
        marker_color=latest["color"],
        text=latest["grid_load_pct"].round(1).astype(str) + "%",
        textposition="outside",
        textfont=dict(color="#1e293b", size=12),
    ))
    fig.add_vline(x=100, line_dash="dash", line_color="#ef4444",
                  annotation_text="Capacity", annotation_position="top right",
                  annotation_font_color="#1e293b")
    fig.update_layout(
        title=dict(text="Peak Grid Load by City", font=dict(color="#1e293b", size=14)),
        xaxis_title="Grid Load (%)",
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(color="#1e293b"),
        xaxis=dict(gridcolor="#e2e8f0", range=[0, max(latest["grid_load_pct"]) * 1.15],
                   tickfont=dict(color="#1e293b")),
        yaxis=dict(gridcolor="#e2e8f0", tickfont=dict(color="#1e293b")),
        height=320,
        margin=dict(l=10, r=60, t=40, b=10),
    )
    return fig


def build_fault_timeline(alerts):
    if not alerts:
        return None

    df = pd.DataFrame(alerts)
    if "fault_type" not in df.columns:
        return None

    counts = df.groupby(["fault_type", "severity"]).size().reset_index(name="count")

    fig = go.Figure()
    for _, row in counts.iterrows():
        fig.add_trace(go.Bar(
            name=row["fault_type"],
            x=[row["fault_type"]],
            y=[row["count"]],
            marker_color=SEVERITY_COLOR.get(row["severity"], "#94a3b8"),
            text=str(row["count"]),
            textposition="outside",
            textfont=dict(color="#1e293b"),
        ))

    fig.update_layout(
        title=dict(text="Active Alert Distribution", font=dict(color="#1e293b", size=14)),
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(color="#1e293b"),
        xaxis=dict(gridcolor="#e2e8f0", tickfont=dict(color="#1e293b")),
        yaxis=dict(gridcolor="#e2e8f0", title="Alert Count",
                   tickfont=dict(color="#1e293b")),
        showlegend=False,
        height=320,
        margin=dict(l=10, r=10, t=40, b=10),
        barmode="group",
    )
    return fig


def build_connector_power_chart(telemetry_df):
    charging = telemetry_df[telemetry_df["status"] == "CHARGING"]
    avg_power = (
        charging.groupby("connector_type")["power_kw"]
        .agg(["mean", "max", "count"])
        .reset_index()
    )
    colors = {"CCS2": "#3b82f6", "CHAdeMO": "#f59e0b", "Type2": "#22c55e"}

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=avg_power["connector_type"],
        y=avg_power["mean"].round(2),
        name="Avg Power (kW)",
        marker_color=[colors.get(c, "#94a3b8") for c in avg_power["connector_type"]],
        text=avg_power["mean"].round(1),
        textposition="outside",
        textfont=dict(color="#1e293b"),
    ))
    fig.update_layout(
        title=dict(text="Average Charging Power by Connector Type",
                   font=dict(color="#1e293b", size=14)),
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(color="#1e293b"),
        xaxis=dict(gridcolor="#e2e8f0", tickfont=dict(color="#1e293b")),
        yaxis=dict(gridcolor="#e2e8f0", title="Power (kW)",
                   tickfont=dict(color="#1e293b")),
        height=320,
        margin=dict(l=10, r=10, t=40, b=10),
        showlegend=False,
    )
    return fig


def build_sessions_by_city(telemetry_df):
    sessions = (
        telemetry_df[telemetry_df["session_id"].notna()]
        .groupby("city_code")["session_id"]
        .nunique()
        .reset_index()
        .rename(columns={"session_id": "sessions"})
        .sort_values("sessions", ascending=False)
    )
    sessions["city_name"] = sessions["city_code"].map(CITY_NAMES)

    fig = px.bar(
        sessions, x="city_name", y="sessions",
        color="sessions",
        color_continuous_scale=["#dbeafe", "#3b82f6", "#1d4ed8"],
        text="sessions",
    )
    fig.update_traces(textposition="outside", textfont=dict(color="#1e293b"))
    fig.update_layout(
        title=dict(text="Total Charging Sessions by City (24h)",
                   font=dict(color="#1e293b", size=14)),
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(color="#1e293b"),
        xaxis=dict(gridcolor="#e2e8f0", title="", tickfont=dict(color="#1e293b")),
        yaxis=dict(gridcolor="#e2e8f0", title="Sessions", tickfont=dict(color="#1e293b")),
        coloraxis_showscale=False,
        height=320,
        margin=dict(l=10, r=10, t=40, b=10),
    )
    return fig


# ── Main dashboard ─────────────────────────────────────────────────────────────
def main():
    st.markdown(
        "<h2 style='color:#1e293b'>⚡ EV Charging Network — Live Operations Center</h2>",
        unsafe_allow_html=True,
    )
    st.markdown(
        "<p style='color:#64748b; margin-top:-12px'>110 stations · 8 German cities · "
        "Real-time anomaly detection · AI incident reports</p>",
        unsafe_allow_html=True,
    )

    stations_df  = load_stations()
    telemetry_df = load_telemetry()
    alerts       = consume_recent_alerts(max_alerts=300)
    reports      = load_reports()

    # ── KPI Row ──────────────────────────────────────────────────────────────
    total_stations  = len(stations_df)
    active_sessions = telemetry_df[telemetry_df["status"] == "CHARGING"]["session_id"].nunique()
    total_energy    = round(telemetry_df["energy_delivered_kwh"].sum() / 1000, 1)
    critical_count  = sum(1 for a in alerts if a.get("severity") == "CRITICAL")
    fault_stations  = len({a["station_id"] for a in alerts})

    k1, k2, k3, k4, k5 = st.columns(5)
    with k1:
        st.metric("Total Stations", total_stations)
    with k2:
        st.metric("Active Sessions (24h)", f"{active_sessions:,}")
    with k3:
        st.metric("Energy Delivered", f"{total_energy} MWh")
    with k4:
        st.metric("🔴 Critical Alerts", critical_count,
                  delta="Action required" if critical_count > 0 else None,
                  delta_color="inverse")
    with k5:
        st.metric("Stations with Faults", fault_stations)

    st.divider()

    # ── Map + Alert Feed ──────────────────────────────────────────────────────
    col_map, col_alerts = st.columns([3, 2])

    with col_map:
        st.markdown("<h3 style='color:#1e293b'>🗺️ Station Network — Germany</h3>",
                    unsafe_allow_html=True)
        m = build_map(stations_df, alerts)
        st_folium(m, use_container_width=True, height=480, returned_objects=[])

    with col_alerts:
        st.markdown("<h3 style='color:#1e293b'>🚨 Live Alert Feed</h3>",
                    unsafe_allow_html=True)
        if not alerts:
            st.info("No active alerts — all stations nominal")
        else:
            priority = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
            sorted_alerts = sorted(
                alerts, key=lambda a: priority.get(a.get("severity", "LOW"), 3)
            )[:25]

            for a in sorted_alerts:
                sev  = a.get("severity", "LOW")
                ft   = a.get("fault_type", "")
                sid  = a.get("station_id", "")
                msg  = a.get("evidence", {}).get("message", "")
                icon = FAULT_ICON.get(ft, "⚠️")
                css  = f"alert-{sev.lower()}"
                st.markdown(
                    f'<div class="{css}">'
                    f'<b style="color:#1e293b">{icon} {sev}</b>'
                    f' — <span style="color:#1e293b">{ft}</span><br>'
                    f'<small style="color:#475569">{sid} · {msg}</small>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

    st.divider()

    # ── Charts Row ────────────────────────────────────────────────────────────
    c1, c2 = st.columns(2)
    with c1:
        st.plotly_chart(build_grid_load_chart(telemetry_df), use_container_width=True)
    with c2:
        fault_chart = build_fault_timeline(alerts)
        if fault_chart:
            st.plotly_chart(fault_chart, use_container_width=True)
        else:
            st.info("No alerts to chart yet — anomaly agent generating...")

    c3, c4 = st.columns(2)
    with c3:
        st.plotly_chart(build_connector_power_chart(telemetry_df), use_container_width=True)
    with c4:
        st.plotly_chart(build_sessions_by_city(telemetry_df), use_container_width=True)

    st.divider()

    # ── AI Incident Reports ───────────────────────────────────────────────────
    st.markdown("<h3 style='color:#1e293b'>🤖 AI-Generated Incident Reports</h3>",
                unsafe_allow_html=True)

    if not reports:
        st.info("No reports generated yet — report agent generates every 60s")
    else:
        for report in reports:
            sev = report.get("severity_level", "LOW")
            with st.expander(
                f"📋 {report.get('report_id','?')} — "
                f"**{sev}** — {report.get('generated_at','')} "
                f"({report.get('alert_count',0)} alerts)",
                expanded=(sev == "CRITICAL"),
            ):
                st.markdown(
                    f"<p style='color:#1e293b'><b>Summary:</b> "
                    f"{report.get('summary','')}</p>",
                    unsafe_allow_html=True,
                )

                patterns = report.get("cross_station_patterns")
                if patterns and patterns not in (None, "null", "None"):
                    st.markdown(
                        f"<p style='color:#1e293b'><b>Cross-station patterns:</b> "
                        f"{patterns}</p>",
                        unsafe_allow_html=True,
                    )

                st.markdown("<b style='color:#1e293b'>Incidents:</b>",
                            unsafe_allow_html=True)

                for inc in report.get("incidents", []):
                    urgency = inc.get("urgency", "Monitor")
                    urgency_color = {
                        "Immediate": "#ef4444",
                        "Within 1h": "#f59e0b",
                        "Within 4h": "#3b82f6",
                        "Monitor":   "#22c55e",
                    }.get(urgency, "#94a3b8")

                    # Flexible key lookup — Claude sometimes varies key names
                    ft          = inc.get("fault_type") or inc.get("type", "UNKNOWN")
                    city        = inc.get("city") or inc.get("city_code", "?")
                    description = inc.get("description") or inc.get("details", "")
                    action      = inc.get("recommended_action") or inc.get("action", "")
                    icon        = FAULT_ICON.get(ft, "⚠️")

                    st.markdown(
                        f'<div class="report-box">'
                        f'<b style="color:{urgency_color}">[{urgency}]</b> '
                        f'{icon} <b style="color:#1e293b">{ft}</b> — '
                        f'<span style="color:#475569">{city}</span><br>'
                        f'<p style="color:#1e293b; margin:6px 0">{description}</p>'
                        f'<b style="color:#1e293b">→ Action:</b> '
                        f'<span style="color:#334155">{action}</span>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )

    # ── Auto-refresh ──────────────────────────────────────────────────────────
    st.divider()
    st.markdown("<p style='color:#94a3b8; font-size:12px'>"
                "Dashboard auto-refreshes every 30 seconds</p>",
                unsafe_allow_html=True)
    time.sleep(30)
    st.rerun()


if __name__ == "__main__":
    main()
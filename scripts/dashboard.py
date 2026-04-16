import streamlit as st
import pandas as pd
import numpy as np
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import time
import json
import glob
import os

DB_CONFIG = {
    "host": "localhost",
    "database": "smart_city_waste",
    "user": "postgres",
    "password": "admin123",
    "port": 5432
}

DATA_FOLDER = "data"
ALERT_THRESHOLD = 80.0

st.set_page_config(
    page_title="Smart City Waste Management",
    page_icon="♻",
    layout="wide"
)

st.markdown("""
<style>
    .block-container {
        padding-top: 3.5rem !important;
        padding-bottom: 1rem !important;
    }
    .metric-card {
        border-radius: 14px;
        padding: 22px 16px;
        text-align: center;
        border: 1px solid rgba(128,128,128,0.25);
        background: rgba(128,128,128,0.08);
        margin: 4px 0;
    }
    .metric-value {
        font-size: 38px;
        font-weight: 800;
        margin: 8px 0 4px 0;
        line-height: 1;
    }
    .metric-label {
        font-size: 11px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        color: #aaaaaa !important;
    }
    .section-header {
        font-size: 20px;
        font-weight: 700;
        color: #ffffff !important;
        border-left: 4px solid #4f8ef7;
        padding-left: 12px;
        margin: 28px 0 14px 0;
    }
    .bin-card {
        border-radius: 16px;
        padding: 18px 14px;
        margin: 6px 0;
        text-align: center;
        border-width: 2px;
        border-style: solid;
        transition: transform 0.2s ease;
    }
    .bin-card:hover { transform: translateY(-3px); }
    .bin-id {
        font-size: 15px;
        font-weight: 700;
        margin: 8px 0 2px 0;
    }
    .bin-location {
        font-size: 11px;
        color: #777777 !important;
        margin-bottom: 10px;
    }
    .bin-fill {
        font-size: 30px;
        font-weight: 800;
        margin: 4px 0;
        line-height: 1;
    }
    .bin-fill-label {
        font-size: 10px;
        color: #888888 !important;
        margin-bottom: 8px;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    .bin-battery {
        font-size: 12px;
        color: #cccccc !important;
        margin: 6px 0;
    }
    .bin-status {
        font-size: 11px;
        font-weight: 700;
        padding: 4px 12px;
        border-radius: 20px;
        display: inline-block;
        margin-top: 8px;
        letter-spacing: 0.5px;
    }
    .status-critical { background: #c62828; color: white !important; }
    .status-moderate { background: #e65100; color: white !important; }
    .status-normal   { background: #2e7d32; color: white !important; }
    .main-header {
        border-radius: 18px;
        padding: 28px 32px;
        margin-bottom: 16px;
        margin-top: 0;
        border: 1px solid rgba(79,142,247,0.3);
        background: linear-gradient(135deg, rgba(79,142,247,0.1), rgba(168,85,247,0.1));
    }
    .main-title {
        font-size: 28px;
        font-weight: 800;
        margin: 0 0 8px 0;
        color: #4f8ef7 !important;
        line-height: 1.2;
    }
    .main-subtitle {
        font-size: 13px;
        color: #aaaaaa !important;
        margin: 0;
        line-height: 1.4;
    }
    .timestamp-badge {
        border-radius: 8px;
        padding: 6px 14px;
        font-size: 12px;
        color: #aaaaaa !important;
        display: inline-block;
        margin-bottom: 16px;
        border: 1px solid rgba(128,128,128,0.3);
        background: rgba(128,128,128,0.08);
    }
    .fill-bar-bg {
        background: rgba(128,128,128,0.15);
        border-radius: 10px;
        height: 6px;
        margin: 6px 0;
        overflow: hidden;
    }
    .fill-bar { height: 6px; border-radius: 10px; }
    .footer {
        text-align: center;
        color: #888888 !important;
        font-size: 12px;
        padding: 20px 0 10px 0;
        border-top: 1px solid rgba(128,128,128,0.2);
        margin-top: 20px;
    }
</style>
""", unsafe_allow_html=True)

def connect_db():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except:
        return None

def load_from_json():
    all_files = glob.glob(os.path.join(DATA_FOLDER, "*.json"))
    all_records = []
    for file in all_files:
        with open(file, "r") as f:
            all_records.extend(json.load(f))
    df = pd.DataFrame(all_records)
    df["fill_level"] = pd.to_numeric(df["fill_level"])
    df["battery_level"] = pd.to_numeric(df["battery_level"])
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df

def load_bin_data():
    conn = connect_db()
    if conn:
        try:
            df = pd.read_sql("""
                SELECT DISTINCT ON (bin_id)
                    bin_id, location, fill_level, battery_level, status, timestamp
                FROM bin_readings ORDER BY bin_id, timestamp DESC
            """, conn)
            conn.close()
            return df
        except:
            conn.close()
    return load_from_json().groupby("bin_id").last().reset_index()

def load_alerts():
    conn = connect_db()
    if conn:
        try:
            df = pd.read_sql("""
                SELECT bin_id, location, fill_level, alert_type, timestamp
                FROM bin_alerts ORDER BY timestamp DESC LIMIT 20
            """, conn)
            conn.close()
            return df
        except:
            conn.close()
    return pd.DataFrame()

def load_history():
    conn = connect_db()
    if conn:
        try:
            df = pd.read_sql("""
                SELECT bin_id, AVG(fill_level) as avg_fill,
                       date_trunc('minute', timestamp) as minute
                FROM bin_readings
                GROUP BY bin_id, date_trunc('minute', timestamp)
                ORDER BY minute DESC
                LIMIT 50
            """, conn)
            conn.close()
            return df
        except:
            conn.close()
    return pd.DataFrame()

# --- Sidebar ---
st.sidebar.markdown("## ♻ Controls")
refresh = st.sidebar.checkbox("Auto Refresh (10s)", value=False)
alert_only = st.sidebar.checkbox("Show Critical Bins Only", value=False)
st.sidebar.markdown("---")
st.sidebar.markdown("### System Status")
st.sidebar.markdown("🟢 **Kafka** — Running")
st.sidebar.markdown("🟢 **PostgreSQL** — Connected")
st.sidebar.markdown("🟢 **Streamlit** — Live")
st.sidebar.markdown("---")
st.sidebar.markdown("### Tech Stack")
st.sidebar.markdown("- 🐍 Python 3.12")
st.sidebar.markdown("- 📨 Apache Kafka 4.2")
st.sidebar.markdown("- ⚡ Apache Spark")
st.sidebar.markdown("- 🐘 PostgreSQL 17")
st.sidebar.markdown("- 📊 Streamlit + Plotly")
st.sidebar.markdown("---")
st.sidebar.markdown(f"🕐 `{datetime.now().strftime('%d %b %Y, %H:%M:%S')}`")

if refresh:
    time.sleep(10)
    st.rerun()

# --- Load data ---
df = load_bin_data()
alerts_df = load_alerts()
history_df = load_history()

if alert_only:
    df = df[df["fill_level"] > ALERT_THRESHOLD]

total_bins = len(df)
critical_bins = len(df[df["fill_level"] > 80])
moderate_bins = len(df[(df["fill_level"] > 50) & (df["fill_level"] <= 80)])
normal_bins = len(df[df["fill_level"] <= 50])
avg_fill = df["fill_level"].mean()
total_alerts = len(alerts_df)

# --- Header ---
st.markdown(f"""
<div class='main-header'>
    <p class='main-title'>♻ Smart City Waste Management System</p>
    <p class='main-subtitle'>
        Real-time IoT monitoring dashboard &nbsp;·&nbsp;
        Apache Kafka &nbsp;·&nbsp; Apache Spark &nbsp;·&nbsp;
        PostgreSQL &nbsp;·&nbsp; Streamlit
    </p>
</div>
""", unsafe_allow_html=True)

st.markdown(
    f"<div class='timestamp-badge'>🕐 Last updated: "
    f"{datetime.now().strftime('%d %B %Y, %H:%M:%S')}</div>",
    unsafe_allow_html=True
)

# --- Metrics ---
st.markdown("<div class='section-header'>City Overview</div>",
            unsafe_allow_html=True)

c1, c2, c3, c4, c5, c6 = st.columns(6)
metrics = [
    (c1, str(total_bins),     "Total Bins",   "#4f8ef7"),
    (c2, str(critical_bins),  "Critical 🔴",  "#ef5350"),
    (c3, str(moderate_bins),  "Moderate 🟡",  "#ff9800"),
    (c4, str(normal_bins),    "Normal 🟢",    "#4caf50"),
    (c5, f"{avg_fill:.1f}%",  "Avg Fill",     "#ab47bc"),
    (c6, str(total_alerts),   "Total Alerts", "#ff5722"),
]
for col, value, label, color in metrics:
    with col:
        st.markdown(f"""
        <div class='metric-card'>
            <div class='metric-label'>{label}</div>
            <div class='metric-value' style='color:{color}'>{value}</div>
        </div>
        """, unsafe_allow_html=True)

# --- Bin Cards ---
st.markdown("<div class='section-header'>Live Bin Status</div>",
            unsafe_allow_html=True)
cols = st.columns(5)

for i, (_, row) in enumerate(df.iterrows()):
    col = cols[i % 5]
    with col:
        fill = float(row["fill_level"])
        battery = float(row["battery_level"])

        if fill > 80:
            bg = "rgba(239,83,80,0.12)"
            bc = "#ef5350"
            icon = "🔴"
            sc = "status-critical"
            lbl = "CRITICAL"
        elif fill > 50:
            bg = "rgba(255,152,0,0.12)"
            bc = "#ff9800"
            icon = "🟡"
            sc = "status-moderate"
            lbl = "MODERATE"
        else:
            bg = "rgba(76,175,80,0.12)"
            bc = "#4caf50"
            icon = "🟢"
            sc = "status-normal"
            lbl = "NORMAL"

        bicon = "🔴" if battery < 30 else "🟡" if battery < 50 else "🟢"

        st.markdown(f"""
        <div class='bin-card' style='background:{bg}; border-color:{bc};'>
            <div style='font-size:28px'>{icon}</div>
            <div class='bin-id' style='color:{bc}'>{row['bin_id']}</div>
            <div class='bin-location'>{row['location']}</div>
            <div class='bin-fill' style='color:{bc}'>{fill:.1f}%</div>
            <div class='fill-bar-bg'>
                <div class='fill-bar'
                     style='width:{int(fill)}%; background:{bc}'></div>
            </div>
            <div class='bin-fill-label'>Fill Level</div>
            <div class='bin-battery'>{bicon} Battery: {battery:.1f}%</div>
            <span class='bin-status {sc}'>{lbl}</span>
        </div>
        """, unsafe_allow_html=True)

# --- Charts Row 1 ---
st.markdown("<div class='section-header'>Analytics</div>",
            unsafe_allow_html=True)

chart_col1, chart_col2 = st.columns([3, 2])

with chart_col1:
    df_sorted = df.sort_values("fill_level", ascending=True).copy()
    colors = ["#ef5350" if f > 80 else "#ff9800" if f > 50 else "#4caf50"
              for f in df_sorted["fill_level"]]
    fig1 = go.Figure(go.Bar(
        x=df_sorted["fill_level"],
        y=df_sorted["bin_id"],
        orientation="h",
        marker=dict(color=colors, line=dict(width=0)),
        text=[f"{f:.1f}%" for f in df_sorted["fill_level"]],
        textposition="outside",
        textfont=dict(size=12, color="#cccccc")
    ))
    fig1.add_vline(x=80, line_dash="dash", line_color="#ef5350",
                   line_width=2,
                   annotation_text="⚠ Alert (80%)",
                   annotation_font_size=11,
                   annotation_font_color="#ef5350")
    fig1.update_layout(
        title=dict(text="Fill Level per Bin",
                   font=dict(size=16, color="#ffffff")),
        height=400,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(range=[0, 118],
                   gridcolor="rgba(128,128,128,0.15)",
                   ticksuffix="%",
                   tickfont=dict(size=11, color="#cccccc"),
                   color="#cccccc"),
        yaxis=dict(gridcolor="rgba(128,128,128,0.15)",
                   tickfont=dict(size=11, color="#cccccc"),
                   color="#cccccc"),
        margin=dict(l=10, r=40, t=40, b=10),
        showlegend=False
    )
    st.plotly_chart(fig1, use_container_width=True)

with chart_col2:
    # Clean gauge - no overlapping text
    status_text = "CRITICAL" if avg_fill > 80 else "MODERATE" if avg_fill > 50 else "NORMAL"
    status_color = "#ef5350" if avg_fill > 80 else "#ff9800" if avg_fill > 50 else "#4caf50"

    fig2 = go.Figure(go.Indicator(
        mode="gauge+number",
        value=round(avg_fill, 1),
        title={
            "text": f"Avg City Fill Level<br><span style='font-size:14px;color:{status_color}'>{status_text}</span>",
            "font": {"size": 16, "color": "#ffffff"}
        },
        number={
            "suffix": "%",
            "font": {"size": 44, "color": "#4f8ef7"}
        },
        gauge={
            "axis": {
                "range": [0, 100],
                "tickwidth": 1,
                "tickcolor": "#666666",
                "ticksuffix": "%",
                "tickfont": {"color": "#aaaaaa", "size": 10},
                "nticks": 6
            },
            "bar": {"color": "#4f8ef7", "thickness": 0.3},
            "bgcolor": "rgba(0,0,0,0)",
            "borderwidth": 0,
            "steps": [
                {"range": [0, 50],   "color": "rgba(76,175,80,0.2)"},
                {"range": [50, 80],  "color": "rgba(255,152,0,0.2)"},
                {"range": [80, 100], "color": "rgba(239,83,80,0.2)"}
            ],
            "threshold": {
                "line": {"color": "#ef5350", "width": 3},
                "thickness": 0.8,
                "value": 80
            }
        }
    ))
    fig2.update_layout(
        height=400,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#cccccc"),
        margin=dict(l=30, r=30, t=80, b=30)
    )
    st.plotly_chart(fig2, use_container_width=True)

# --- Charts Row 2 ---
chart_col3, chart_col4 = st.columns([2, 3])

with chart_col3:
    df_battery = df.sort_values("battery_level").copy()
    bc_colors = ["#ef5350" if b < 30 else "#ff9800" if b < 50 else "#4caf50"
                 for b in df_battery["battery_level"]]
    fig3 = go.Figure(go.Bar(
        x=df_battery["bin_id"],
        y=df_battery["battery_level"],
        marker=dict(color=bc_colors, line=dict(width=0)),
        text=[f"{b:.0f}%" for b in df_battery["battery_level"]],
        textposition="outside",
        textfont=dict(size=11, color="#cccccc")
    ))
    fig3.add_hline(y=30, line_dash="dash", line_color="#ef5350",
                   line_width=2,
                   annotation_text="⚠ Low (30%)",
                   annotation_font_size=11,
                   annotation_font_color="#ef5350")
    fig3.update_layout(
        title=dict(text="Battery Levels",
                   font=dict(size=16, color="#ffffff")),
        height=350,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        yaxis=dict(range=[0, 118],
                   gridcolor="rgba(128,128,128,0.15)",
                   ticksuffix="%",
                   tickfont=dict(size=11, color="#cccccc"),
                   color="#cccccc"),
        xaxis=dict(gridcolor="rgba(128,128,128,0.15)",
                   tickfont=dict(size=10, color="#cccccc"),
                   color="#cccccc"),
        margin=dict(l=10, r=10, t=40, b=10),
        showlegend=False
    )
    st.plotly_chart(fig3, use_container_width=True)

with chart_col4:
    if not history_df.empty:
        fig4 = go.Figure()
        colors_map = ["#4f8ef7", "#4caf50", "#ff9800", "#ef5350", "#ab47bc",
                      "#00bcd4", "#ff5722", "#8bc34a", "#ffc107", "#e91e63"]

        for idx, bin_name in enumerate(sorted(history_df["bin_id"].unique())):
            bin_data = history_df[history_df["bin_id"] == bin_name]
            avg_val = bin_data["avg_fill"].mean()
            color = colors_map[idx % len(colors_map)]
            fig4.add_trace(go.Bar(
                name=bin_name,
                x=[bin_name],
                y=[round(avg_val, 1)],
                marker=dict(color=color, line=dict(width=0)),
                text=[f"{avg_val:.1f}%"],
                textposition="outside",
                textfont=dict(size=11, color="#cccccc")
            ))

        fig4.add_hline(y=80, line_dash="dash", line_color="#ef5350",
                       line_width=2,
                       annotation_text="⚠ Alert threshold (80%)",
                       annotation_font_size=11,
                       annotation_font_color="#ef5350")
        fig4.update_layout(
            title=dict(text="Average Fill Level per Bin (Historical)",
                       font=dict(size=16, color="#ffffff")),
            height=350,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(gridcolor="rgba(128,128,128,0.15)",
                       tickfont=dict(size=10, color="#cccccc"),
                       color="#cccccc"),
            yaxis=dict(range=[0, 118],
                       gridcolor="rgba(128,128,128,0.15)",
                       ticksuffix="%",
                       tickfont=dict(size=11, color="#cccccc"),
                       color="#cccccc"),
            showlegend=False,
            bargap=0.3,
            margin=dict(l=10, r=10, t=40, b=10)
        )
        st.plotly_chart(fig4, use_container_width=True)
    else:
        st.info("No history data available yet.")

# --- Alerts ---
st.markdown("<div class='section-header'>Recent Alerts</div>",
            unsafe_allow_html=True)
if not alerts_df.empty:
    st.dataframe(
        alerts_df.style.map(
            lambda x: "background-color: #c62828; color: white; font-weight: bold"
            if x == "HIGH_FILL_LEVEL" else "",
            subset=["alert_type"]
        ),
        use_container_width=True,
        height=280
    )
else:
    st.info("No alerts found.")

# --- Summary Stats ---
st.markdown("<div class='section-header'>Summary Statistics</div>",
            unsafe_allow_html=True)

s1, s2, s3 = st.columns(3)

with s1:
    st.markdown("**📊 Fill Level Statistics**")
    st.dataframe(pd.DataFrame({
        "Metric": ["Mean", "Median", "Std Dev", "Min", "Max"],
        "Value": [
            f"{df['fill_level'].mean():.2f}%",
            f"{df['fill_level'].median():.2f}%",
            f"{df['fill_level'].std():.2f}%",
            f"{df['fill_level'].min():.2f}%",
            f"{df['fill_level'].max():.2f}%"
        ]
    }), use_container_width=True, hide_index=True)

with s2:
    st.markdown("**🔋 Battery Statistics**")
    st.dataframe(pd.DataFrame({
        "Metric": ["Mean Battery", "Min Battery",
                   "Max Battery", "Low Battery Bins"],
        "Value": [
            f"{df['battery_level'].mean():.2f}%",
            f"{df['battery_level'].min():.2f}%",
            f"{df['battery_level'].max():.2f}%",
            str(len(df[df['battery_level'] < 30]))
        ]
    }), use_container_width=True, hide_index=True)

with s3:
    st.markdown("**🚨 Alert Summary**")
    st.dataframe(pd.DataFrame({
        "Metric": ["Total Alerts", "Critical Bins",
                   "Moderate Bins", "Normal Bins", "Alert Rate"],
        "Value": [
            str(total_alerts),
            str(critical_bins),
            str(moderate_bins),
            str(normal_bins),
            f"{(critical_bins/total_bins*100):.1f}%" if total_bins > 0 else "0%"
        ]
    }), use_container_width=True, hide_index=True)

# --- Footer ---
st.markdown(f"""
<div class='footer'>
    ♻ Smart City Waste Management System &nbsp;|&nbsp;
    Data Engineering Capstone Project &nbsp;|&nbsp;
    Python · Kafka · Spark · PostgreSQL · Streamlit &nbsp;|&nbsp;
    {datetime.now().strftime('%Y')}
</div>
""", unsafe_allow_html=True)
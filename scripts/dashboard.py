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

# --- Professional CSS ---
st.markdown("""
<style>
    /* Main background */
    .stApp { background-color: #0f1117; }
    
    /* Header */
    .main-header {
        background: linear-gradient(135deg, #1a1f2e, #16213e);
        border: 1px solid #2d3561;
        border-radius: 16px;
        padding: 24px 32px;
        margin-bottom: 24px;
        display: flex;
        align-items: center;
        gap: 16px;
    }
    .main-title {
        font-size: 28px;
        font-weight: 700;
        color: #ffffff;
        margin: 0;
    }
    .main-subtitle {
        font-size: 14px;
        color: #8892b0;
        margin: 4px 0 0 0;
    }
    
    /* Metric cards */
    .metric-card {
        background: #1a1f2e;
        border: 1px solid #2d3561;
        border-radius: 12px;
        padding: 20px;
        text-align: center;
    }
    .metric-value {
        font-size: 36px;
        font-weight: 700;
        margin: 8px 0 4px 0;
    }
    .metric-label {
        font-size: 13px;
        color: #8892b0;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 1px;
    }

    /* Bin cards */
    .bin-card {
        border-radius: 14px;
        padding: 16px 12px;
        margin: 6px 0;
        text-align: center;
        transition: transform 0.2s;
    }
    .bin-card:hover { transform: translateY(-2px); }
    .bin-id {
        font-size: 15px;
        font-weight: 700;
        color: #1a1a1a;
        margin: 8px 0 2px 0;
    }
    .bin-location {
        font-size: 11px;
        color: #444;
        margin-bottom: 10px;
    }
    .bin-fill {
        font-size: 28px;
        font-weight: 800;
        color: #1a1a1a;
        margin: 4px 0;
    }
    .bin-fill-label {
        font-size: 11px;
        color: #555;
        margin-bottom: 6px;
    }
    .bin-battery {
        font-size: 12px;
        color: #333;
        margin: 4px 0;
    }
    .bin-status {
        font-size: 11px;
        font-weight: 700;
        padding: 3px 10px;
        border-radius: 20px;
        display: inline-block;
        margin-top: 6px;
    }
    .status-critical { background: #c62828; color: white; }
    .status-moderate { background: #f57f17; color: white; }
    .status-normal   { background: #2e7d32; color: white; }

    /* Section headers */
    .section-header {
        font-size: 18px;
        font-weight: 700;
        color: #ffffff;
        border-left: 4px solid #4f8ef7;
        padding-left: 12px;
        margin: 28px 0 16px 0;
    }

    /* Sidebar */
    .css-1d391kg { background-color: #1a1f2e; }

    /* Timestamp badge */
    .timestamp-badge {
        background: #1a1f2e;
        border: 1px solid #2d3561;
        border-radius: 8px;
        padding: 6px 14px;
        font-size: 12px;
        color: #8892b0;
        display: inline-block;
        margin-bottom: 20px;
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

# --- Sidebar ---
st.sidebar.markdown("## ♻ Controls")
refresh = st.sidebar.checkbox("Auto Refresh (10s)", value=False)
alert_only = st.sidebar.checkbox("Show Critical Bins Only", value=False)
st.sidebar.markdown("---")
st.sidebar.markdown("### System Info")
st.sidebar.markdown("- **Ingestion:** Apache Kafka")
st.sidebar.markdown("- **Processing:** Spark / Pandas")
st.sidebar.markdown("- **Storage:** PostgreSQL")
st.sidebar.markdown("- **Dashboard:** Streamlit")
st.sidebar.markdown("---")
st.sidebar.markdown(f"🕐 Last refresh: `{datetime.now().strftime('%H:%M:%S')}`")

if refresh:
    time.sleep(10)
    st.rerun()

# --- Header ---
st.markdown("""
<div class='main-header'>
    <div>
        <p class='main-title'>♻ Smart City Waste Management System</p>
        <p class='main-subtitle'>Real-time IoT monitoring · Kafka · Spark · PostgreSQL · Streamlit</p>
    </div>
</div>
""", unsafe_allow_html=True)

st.markdown(f"<div class='timestamp-badge'>🕐 Dashboard updated: {datetime.now().strftime('%d %B %Y, %H:%M:%S')}</div>",
            unsafe_allow_html=True)

# --- Load data ---
df = load_bin_data()
alerts_df = load_alerts()

if alert_only:
    df = df[df["fill_level"] > ALERT_THRESHOLD]

# --- Metrics ---
st.markdown("<div class='section-header'>City Overview</div>", unsafe_allow_html=True)

total_bins = len(df)
critical_bins = len(df[df["fill_level"] > 80])
moderate_bins = len(df[(df["fill_level"] > 50) & (df["fill_level"] <= 80)])
normal_bins = len(df[df["fill_level"] <= 50])
avg_fill = df["fill_level"].mean()

col1, col2, col3, col4, col5 = st.columns(5)

metrics = [
    (col1, str(total_bins), "Total Bins", "#4f8ef7"),
    (col2, str(critical_bins), "Critical 🔴", "#ef5350"),
    (col3, str(moderate_bins), "Moderate 🟡", "#ffca28"),
    (col4, str(normal_bins), "Normal 🟢", "#66bb6a"),
    (col5, f"{avg_fill:.1f}%", "Avg Fill Level", "#ab47bc"),
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
st.markdown("<div class='section-header'>Live Bin Status</div>", unsafe_allow_html=True)
cols = st.columns(5)

for i, (_, row) in enumerate(df.iterrows()):
    col = cols[i % 5]
    with col:
        fill = float(row["fill_level"])
        battery = float(row["battery_level"])

        if fill > 80:
            bg = "#ffebee"
            border = "2px solid #ef5350"
            icon = "🔴"
            status_class = "status-critical"
            label = "CRITICAL"
        elif fill > 50:
            bg = "#fff8e1"
            border = "2px solid #ffca28"
            icon = "🟡"
            status_class = "status-moderate"
            label = "MODERATE"
        else:
            bg = "#e8f5e9"
            border = "2px solid #66bb6a"
            icon = "🟢"
            status_class = "status-normal"
            label = "NORMAL"

        battery_icon = "🔴" if battery < 30 else "🟡" if battery < 50 else "🟢"

        st.markdown(f"""
        <div class='bin-card' style='background:{bg}; border:{border};'>
            <div style='font-size:28px'>{icon}</div>
            <div class='bin-id'>{row['bin_id']}</div>
            <div class='bin-location'>{row['location']}</div>
            <div class='bin-fill'>{fill:.1f}%</div>
            <div class='bin-fill-label'>Fill Level</div>
            <div class='bin-battery'>{battery_icon} Battery: {battery:.1f}%</div>
            <span class='bin-status {status_class}'>{label}</span>
        </div>
        """, unsafe_allow_html=True)

# --- Charts ---
st.markdown("<div class='section-header'>Analytics</div>", unsafe_allow_html=True)
chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    df_sorted = df.sort_values("fill_level", ascending=True).copy()
    colors = ["#ef5350" if f > 80 else "#ffca28" if f > 50 else "#66bb6a"
              for f in df_sorted["fill_level"]]
    fig1 = go.Figure(go.Bar(
        x=df_sorted["fill_level"],
        y=df_sorted["bin_id"],
        orientation="h",
        marker_color=colors,
        text=[f"{f:.1f}%" for f in df_sorted["fill_level"]],
        textposition="outside",
        textfont=dict(color="white", size=12)
    ))
    fig1.add_vline(x=80, line_dash="dash", line_color="#ef5350",
                   annotation_text="Alert (80%)",
                   annotation_font_color="white")
    fig1.update_layout(
        title=dict(text="Fill Level by Bin", font=dict(color="white", size=16)),
        height=380,
        paper_bgcolor="#1a1f2e",
        plot_bgcolor="#1a1f2e",
        font=dict(color="white"),
        xaxis=dict(range=[0, 115], gridcolor="#2d3561", color="white"),
        yaxis=dict(gridcolor="#2d3561", color="white"),
        margin=dict(l=10, r=30, t=40, b=10)
    )
    st.plotly_chart(fig1, use_container_width=True)

with chart_col2:
    status_data = {
        "Status": ["Critical", "Moderate", "Normal"],
        "Count": [critical_bins, moderate_bins, normal_bins]
    }
    fig2 = px.pie(
        status_data, values="Count", names="Status",
        color="Status",
        color_discrete_map={
            "Critical": "#ef5350",
            "Moderate": "#ffca28",
            "Normal": "#66bb6a"
        },
        hole=0.5
    )
    fig2.update_layout(
        title=dict(text="Bin Status Distribution", font=dict(color="white", size=16)),
        height=380,
        paper_bgcolor="#1a1f2e",
        plot_bgcolor="#1a1f2e",
        font=dict(color="white"),
        legend=dict(font=dict(color="white")),
        margin=dict(l=10, r=10, t=40, b=10)
    )
    fig2.update_traces(textfont_color="white")
    st.plotly_chart(fig2, use_container_width=True)

# --- Alerts ---
st.markdown("<div class='section-header'>Recent Alerts</div>", unsafe_allow_html=True)
if not alerts_df.empty:
    st.dataframe(
        alerts_df.style.map(
            lambda x: "background-color: #c62828; color: white; font-weight: bold"
            if x == "HIGH_FILL_LEVEL" else "",
            subset=["alert_type"]
        ),
        use_container_width=True,
        height=250
    )
else:
    st.info("No alerts found.")

# --- Battery chart ---
st.markdown("<div class='section-header'>Battery Levels</div>", unsafe_allow_html=True)
df_battery = df.sort_values("battery_level").copy()
battery_colors = ["#ef5350" if b < 30 else "#ffca28" if b < 50 else "#66bb6a"
                  for b in df_battery["battery_level"]]
fig3 = go.Figure(go.Bar(
    x=df_battery["bin_id"],
    y=df_battery["battery_level"],
    marker_color=battery_colors,
    text=[f"{b:.1f}%" for b in df_battery["battery_level"]],
    textposition="outside",
    textfont=dict(color="white", size=12)
))
fig3.add_hline(y=30, line_dash="dash", line_color="#ef5350",
               annotation_text="Low Battery (30%)",
               annotation_font_color="white")
fig3.update_layout(
    title=dict(text="Battery Level per Bin", font=dict(color="white", size=16)),
    height=320,
    paper_bgcolor="#1a1f2e",
    plot_bgcolor="#1a1f2e",
    font=dict(color="white"),
    yaxis=dict(range=[0, 115], gridcolor="#2d3561", color="white"),
    xaxis=dict(gridcolor="#2d3561", color="white"),
    margin=dict(l=10, r=10, t=40, b=10)
)
st.plotly_chart(fig3, use_container_width=True)

# --- Footer ---
st.markdown("---")
st.markdown("""
<div style='text-align:center; color:#8892b0; font-size:13px; padding:10px'>
    ♻ Smart City Waste Management System &nbsp;|&nbsp; 
    Data Engineering Capstone Project &nbsp;|&nbsp;
    Built with Python · Kafka · Spark · PostgreSQL · Streamlit
</div>
""", unsafe_allow_html=True)
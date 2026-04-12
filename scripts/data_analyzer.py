import json
import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime

# --- Configuration ---
DATA_FOLDER = "data"
LOG_FOLDER = "logs"
ALERT_THRESHOLD = 80.0   # bins above this % are flagged urgent

def load_all_bin_data():
    """Reads all JSON files from the data folder and combines them."""
    all_files = glob.glob(os.path.join(DATA_FOLDER, "*.json"))

    if not all_files:
        print("No data files found! Run sensor_simulator.py first.")
        return None

    all_records = []

    for file in all_files:
        with open(file, "r") as f:
            records = json.load(f)
            all_records.extend(records)

    print(f"Loaded {len(all_records)} records from {len(all_files)} files.")
    return all_records

def create_dataframe(records):
    """Converts raw list of records into a Pandas DataFrame."""
    df = pd.DataFrame(records)

    # Convert timestamp column to proper datetime type
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Convert fill_level and battery_level to numeric (just to be safe)
    df["fill_level"] = pd.to_numeric(df["fill_level"])
    df["battery_level"] = pd.to_numeric(df["battery_level"])

    return df

def analyze_data(df):
    """Performs analysis on the bin data using Pandas and NumPy."""

    print("\n" + "=" * 55)
    print("  DATA ANALYSIS REPORT")
    print("=" * 55)

    # --- Basic stats ---
    print(f"\n  Total records     : {len(df)}")
    print(f"  Unique bins       : {df['bin_id'].nunique()}")
    print(f"  Time range        : {df['timestamp'].min()} → {df['timestamp'].max()}")

    # --- Average fill level per bin (using Pandas groupby) ---
    print("\n  --- Average Fill Level Per Bin ---")
    avg_fill = df.groupby("bin_id")["fill_level"].mean().round(2)
    for bin_id, avg in avg_fill.items():
        bar = "█" * int(avg / 10)   # simple text bar chart
        print(f"  {bin_id}  |  {bar:<10}  {avg}%")

    # --- NumPy stats on fill levels ---
    fill_array = np.array(df["fill_level"])
    print(f"\n  --- Overall Fill Level Stats ---")
    print(f"  Mean   : {np.mean(fill_array):.2f}%")
    print(f"  Median : {np.median(fill_array):.2f}%")
    print(f"  Std Dev: {np.std(fill_array):.2f}%")
    print(f"  Min    : {np.min(fill_array):.2f}%")
    print(f"  Max    : {np.max(fill_array):.2f}%")

    # --- Alert Detection ---
    print(f"\n  --- URGENT ALERTS (Fill Level > {ALERT_THRESHOLD}%) ---")
    alerts = df[df["fill_level"] > ALERT_THRESHOLD].copy()

    if alerts.empty:
        print("  No urgent bins right now. All bins are fine.")
    else:
        alerts = alerts.sort_values("fill_level", ascending=False)
        for _, row in alerts.iterrows():
            print(f"  *** ALERT *** {row['bin_id']} at {row['location']} — {row['fill_level']}% full | {row['timestamp']}")

    print(f"\n  Total alerts: {len(alerts)} out of {len(df)} records")

    # --- Low battery warning ---
    print(f"\n  --- LOW BATTERY WARNINGS (Battery < 30%) ---")
    low_battery = df[df["battery_level"] < 30]
    if low_battery.empty:
        print("  All batteries are fine.")
    else:
        for _, row in low_battery.iterrows():
            print(f"  !! LOW BATTERY !! {row['bin_id']} at {row['location']} — {row['battery_level']}%")

    print("\n" + "=" * 55)
    return alerts

def save_alerts(alerts):
    """Saves alert records to a separate CSV file for later use."""
    if alerts.empty:
        print("  No alerts to save.")
        return

    alert_file = os.path.join(DATA_FOLDER, "alerts.csv")
    alerts.to_csv(alert_file, index=False)
    print(f"  Alerts saved to → {alert_file}")

def log_operation(message):
    """Writes a log entry."""
    log_file = os.path.join(LOG_FOLDER, "analyzer.log")
    with open(log_file, "a") as f:
        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}\n")

def run_analyzer():
    """Main function."""
    log_operation("Analyzer started")

    # Step 1 — Load data
    records = load_all_bin_data()
    if records is None:
        return

    # Step 2 — Create DataFrame
    df = create_dataframe(records)

    # Step 3 — Analyze
    alerts = analyze_data(df)

    # Step 4 — Save alerts
    save_alerts(alerts)

    log_operation(f"Analyzer finished — {len(alerts)} alerts found")
    print("\n  Done! Check data/alerts.csv for the alert records.")

# --- Run it ---
if __name__ == "__main__":
    run_analyzer()
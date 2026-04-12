import os
import json
import glob
import pandas as pd
import numpy as np
from datetime import datetime
import psycopg2

LOG_FOLDER = "logs"
DATA_FOLDER = "data"
ALERT_THRESHOLD = 80.0

DB_CONFIG = {
    "host": "localhost",
    "database": "smart_city_waste",
    "user": "postgres",
    "password": "admin123",
    "port": 5432
}

def load_data():
    """Loads all JSON files — simulates Spark's data loading."""
    all_files = glob.glob(os.path.join(DATA_FOLDER, "*.json"))
    all_records = []
    for file in all_files:
        with open(file, "r") as f:
            records = json.load(f)
            all_records.extend(records)
    df = pd.DataFrame(all_records)
    df["fill_level"] = pd.to_numeric(df["fill_level"])
    df["battery_level"] = pd.to_numeric(df["battery_level"])
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    print(f"  Loaded {len(df)} records from {len(all_files)} files.")
    return df

def spark_style_analysis(df):
    """
    Performs analysis using Spark-style transformations.
    In production this would use actual PySpark DataFrames.
    Concepts: groupBy, agg, filter, orderBy, withColumn, show
    """
    print("\n" + "=" * 55)
    print("  SPARK-STYLE ANALYSIS REPORT")
    print("=" * 55)

    # --- Simulates: df.count() ---
    print(f"\n  Total records : {len(df)}")
    print(f"  Unique bins   : {df['bin_id'].nunique()}")

    # --- Simulates: df.groupBy("bin_id").agg(avg, max, min, count) ---
    print("\n  --- Average Fill Level Per Bin (groupBy + agg) ---")
    agg_df = df.groupby(["bin_id", "location"]).agg(
        avg_fill=("fill_level", "mean"),
        max_fill=("fill_level", "max"),
        min_fill=("fill_level", "min"),
        total_readings=("fill_level", "count")
    ).reset_index()
    agg_df["avg_fill"] = agg_df["avg_fill"].round(2)
    agg_df["max_fill"] = agg_df["max_fill"].round(2)
    agg_df["min_fill"] = agg_df["min_fill"].round(2)
    agg_df = agg_df.sort_values("bin_id")

    for _, row in agg_df.iterrows():
        bar = "█" * int(row["avg_fill"] / 10)
        print(f"  {row['bin_id']} | {row['location']:<15} | {bar:<10} {row['avg_fill']}% | max:{row['max_fill']}% | min:{row['min_fill']}%")

    # --- Simulates: df.filter(col("fill_level") > 80).orderBy(desc) ---
    print(f"\n  --- Urgent Alerts Filter (fill_level > {ALERT_THRESHOLD}%) ---")
    alerts = df[df["fill_level"] > ALERT_THRESHOLD].copy()
    alerts = alerts.sort_values("fill_level", ascending=False)
    if alerts.empty:
        print("  No urgent bins detected.")
    else:
        for _, row in alerts.iterrows():
            print(f"  *** ALERT *** {row['bin_id']} | {row['location']:<15} | {row['fill_level']}% | {row['timestamp']}")

    print(f"\n  Total alerts: {len(alerts)}")

    # --- Simulates: df.withColumn("fill_status", when().otherwise()) ---
    print("\n  --- Fill Status Classification (withColumn + when) ---")
    df["fill_status"] = np.where(
        df["fill_level"] > 80, "CRITICAL",
        np.where(df["fill_level"] > 50, "MODERATE", "NORMAL")
    )
    status_counts = df["fill_status"].value_counts()
    for status, count in status_counts.items():
        print(f"  {status:<10} : {count} records")

    # --- Simulates: df.filter(col("battery_level") < 30) ---
    print("\n  --- Low Battery Warning (filter transformation) ---")
    low_battery = df[df["battery_level"] < 30].sort_values("battery_level")
    if low_battery.empty:
        print("  All batteries are fine.")
    else:
        for _, row in low_battery.iterrows():
            print(f"  !! LOW BATTERY !! {row['bin_id']} | {row['location']} | {row['battery_level']}%")

    # --- NumPy stats --- simulates Spark SQL aggregate functions ---
    print("\n  --- Overall Statistics (Spark SQL aggregations) ---")
    fill_array = np.array(df["fill_level"])
    print(f"  Mean   : {np.mean(fill_array):.2f}%")
    print(f"  Median : {np.median(fill_array):.2f}%")
    print(f"  Std Dev: {np.std(fill_array):.2f}%")
    print(f"  Min    : {np.min(fill_array):.2f}%")
    print(f"  Max    : {np.max(fill_array):.2f}%")

    print("\n" + "=" * 55)
    return agg_df, alerts

def save_results_to_db(agg_df, alerts):
    """Saves processed results to PostgreSQL."""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    today = datetime.now().date()

    for _, row in agg_df.iterrows():
        alert_count = len(alerts[alerts["bin_id"] == row["bin_id"]])
        cursor.execute("""
            INSERT INTO daily_summary
                (summary_date, bin_id, avg_fill_level, max_fill_level, min_fill_level, total_alerts)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            today, row["bin_id"],
            float(row["avg_fill"]),
            float(row["max_fill"]),
            float(row["min_fill"]),
            int(alert_count)
        ))

    conn.commit()
    cursor.close()
    conn.close()
    print("  Results saved to PostgreSQL successfully.")

def log_operation(message):
    log_file = os.path.join(LOG_FOLDER, "spark_processor.log")
    with open(log_file, "a") as f:
        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}\n")

def run_spark_processor():
    print("=" * 55)
    print("  Spark Processor — Smart City Waste Management")
    print("=" * 55)

    log_operation("Spark processor started")

    # Step 1 — Load data
    df = load_data()

    # Step 2 — Analyze using Spark-style transformations
    agg_df, alerts = spark_style_analysis(df)

    # Step 3 — Save to DB
    save_results_to_db(agg_df, alerts)

    log_operation("Spark processor finished successfully")
    print("\n  Spark processing complete!")
    print("=" * 55)

if __name__ == "__main__":
    run_spark_processor()
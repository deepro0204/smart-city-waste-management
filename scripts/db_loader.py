import json
import os
import glob
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime

# --- Database connection settings ---
DB_CONFIG = {
    "host": "localhost",
    "database": "smart_city_waste",
    "user": "postgres",
    "password": "admin123",
    "port": 5432
}

LOG_FOLDER = "logs"
DATA_FOLDER = "data"
ALERT_THRESHOLD = 80.0

def connect_db():
    """Creates and returns a database connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("  Connected to PostgreSQL successfully.")
        return conn
    except Exception as e:
        print(f"  Connection failed: {e}")
        return None

def load_json_data():
    """Loads all JSON files from data folder."""
    all_files = glob.glob(os.path.join(DATA_FOLDER, "*.json"))
    all_records = []
    for file in all_files:
        with open(file, "r") as f:
            records = json.load(f)
            all_records.extend(records)
    print(f"  Loaded {len(all_records)} records from {len(all_files)} files.")
    return all_records

def insert_bin_readings(conn, records):
    """Inserts all bin readings into bin_readings table."""
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO bin_readings 
            (bin_id, location, fill_level, battery_level, status, timestamp)
        VALUES 
            (%(bin_id)s, %(location)s, %(fill_level)s, %(battery_level)s, %(status)s, %(timestamp)s)
    """

    execute_batch(cursor, insert_query, records)
    conn.commit()
    print(f"  Inserted {len(records)} records into bin_readings.")
    cursor.close()

def insert_alerts(conn, records):
    """Detects alerts and inserts them into bin_alerts table."""
    cursor = conn.cursor()

    alerts = [r for r in records if float(r["fill_level"]) > ALERT_THRESHOLD]

    alert_data = [{
        "bin_id": r["bin_id"],
        "location": r["location"],
        "fill_level": r["fill_level"],
        "alert_type": "HIGH_FILL_LEVEL",
        "timestamp": r["timestamp"]
    } for r in alerts]

    if alert_data:
        insert_query = """
            INSERT INTO bin_alerts 
                (bin_id, location, fill_level, alert_type, timestamp)
            VALUES 
                (%(bin_id)s, %(location)s, %(fill_level)s, %(alert_type)s, %(timestamp)s)
        """
        execute_batch(cursor, insert_query, alert_data)
        conn.commit()
        print(f"  Inserted {len(alert_data)} alerts into bin_alerts.")
    else:
        print("  No alerts to insert.")

    cursor.close()

def insert_daily_summary(conn, records):
    """Creates a daily summary and inserts into daily_summary table."""
    cursor = conn.cursor()

    df = pd.DataFrame(records)
    df["fill_level"] = pd.to_numeric(df["fill_level"])
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["date"] = df["timestamp"].dt.date

    summary = df.groupby(["date", "bin_id"]).agg(
        avg_fill_level=("fill_level", "mean"),
        max_fill_level=("fill_level", "max"),
        min_fill_level=("fill_level", "min")
    ).reset_index()

    alert_counts = df[df["fill_level"] > ALERT_THRESHOLD].groupby(
        ["date", "bin_id"]
    ).size().reset_index(name="total_alerts")

    summary = summary.merge(alert_counts, on=["date", "bin_id"], how="left")
    summary["total_alerts"] = summary["total_alerts"].fillna(0).astype(int)

    insert_query = """
        INSERT INTO daily_summary 
            (summary_date, bin_id, avg_fill_level, max_fill_level, min_fill_level, total_alerts)
        VALUES 
            (%(date)s, %(bin_id)s, %(avg_fill_level)s, %(max_fill_level)s, %(min_fill_level)s, %(total_alerts)s)
    """

    execute_batch(cursor, insert_query, summary.to_dict("records"))
    conn.commit()
    print(f"  Inserted {len(summary)} rows into daily_summary.")
    cursor.close()

def log_operation(message):
    log_file = os.path.join(LOG_FOLDER, "db_loader.log")
    with open(log_file, "a") as f:
        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}\n")

def run_db_loader():
    """Main function."""
    print("=" * 55)
    print("  Database Loader — Smart City Waste Management")
    print("=" * 55)

    log_operation("DB Loader started")

    # Step 1 — Connect
    conn = connect_db()
    if conn is None:
        return

    # Step 2 — Load JSON data
    records = load_json_data()

    # Step 3 — Insert into tables
    print("\n  Loading data into database...")
    insert_bin_readings(conn, records)
    insert_alerts(conn, records)
    insert_daily_summary(conn, records)

    # Step 4 — Close connection
    conn.close()
    log_operation("DB Loader finished successfully")

    print("\n  All data loaded into PostgreSQL successfully!")
    print("=" * 55)

if __name__ == "__main__":
    run_db_loader()
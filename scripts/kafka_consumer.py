import json
from datetime import datetime
from kafka import KafkaConsumer
import psycopg2

KAFKA_TOPIC = "waste-bin-data"
KAFKA_SERVER = "localhost:9092"
ALERT_THRESHOLD = 80.0

DB_CONFIG = {
    "host": "localhost",
    "database": "smart_city_waste",
    "user": "postgres",
    "password": "admin123",
    "port": 5432
}

def connect_db():
    conn = psycopg2.connect(**DB_CONFIG)
    print("  Connected to PostgreSQL.")
    return conn

def create_consumer():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="waste-management-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )
    print("  Kafka Consumer connected successfully.")
    return consumer

def process_message(msg, conn):
    data = msg.value
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO bin_readings 
            (bin_id, location, fill_level, battery_level, status, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        data["bin_id"], data["location"], data["fill_level"],
        data["battery_level"], data["status"], data["timestamp"]
    ))

    if float(data["fill_level"]) > ALERT_THRESHOLD:
        cursor.execute("""
            INSERT INTO bin_alerts 
                (bin_id, location, fill_level, alert_type, timestamp)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            data["bin_id"], data["location"], data["fill_level"],
            "HIGH_FILL_LEVEL", data["timestamp"]
        ))
        print(f"  *** ALERT *** {data['bin_id']} at {data['location']} — {data['fill_level']}% full!")

    conn.commit()
    cursor.close()

def run_consumer():
    print("=" * 55)
    print("  Kafka Consumer — Smart City Waste Management")
    print("=" * 55)
    print("  Listening for messages... (Press Ctrl+C to stop)\n")

    conn = connect_db()
    consumer = create_consumer()
    message_count = 0

    try:
        for message in consumer:
            data = message.value
            message_count += 1
            print(f"  [{message_count}] Received → {data['bin_id']} | {data['location']:<15} | Fill: {data['fill_level']}%")
            process_message(message, conn)

    except KeyboardInterrupt:
        print(f"\n  Consumer stopped. Total messages processed: {message_count}")

    finally:
        consumer.close()
        conn.close()
        print("  Connections closed cleanly.")

if __name__ == "__main__":
    run_consumer()
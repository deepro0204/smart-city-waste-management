import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

KAFKA_TOPIC = "waste-bin-data"
KAFKA_SERVER = "localhost:9092"
NUM_BINS = 10

BIN_LOCATIONS = [
    "MG Road", "Station Square", "City Mall", "Bus Stand",
    "Park Street", "Hospital Road", "Market Area",
    "School Zone", "Airport Gate", "Tech Park"
]

def create_producer():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    print("  Kafka Producer connected successfully.")
    return producer

def generate_bin_data(bin_id):
    return {
        "bin_id": f"BIN_{bin_id:02d}",
        "location": BIN_LOCATIONS[bin_id - 1],
        "fill_level": round(random.uniform(0, 100), 2),
        "battery_level": round(random.uniform(20, 100), 2),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "active"
    }

def run_producer(rounds=10):
    print("=" * 55)
    print("  Kafka Producer — Smart City Waste Management")
    print("=" * 55)

    producer = create_producer()

    for round_num in range(1, rounds + 1):
        print(f"\n--- Round {round_num} of {rounds} ---")

        for bin_id in range(1, NUM_BINS + 1):
            data = generate_bin_data(bin_id)
            producer.send(KAFKA_TOPIC, value=data)
            print(f"  Sent → {data['bin_id']} | {data['location']:<15} | Fill: {data['fill_level']}%")

        producer.flush()
        print(f"  Round {round_num} sent to Kafka topic '{KAFKA_TOPIC}'")

        if round_num < rounds:
            time.sleep(2)

    producer.close()
    print("\n  All data sent to Kafka successfully!")
    print("=" * 55)

if __name__ == "__main__":
    run_producer(rounds=10)
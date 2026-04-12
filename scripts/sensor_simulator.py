import json
import random
import time
import os
from datetime import datetime

# --- Configuration ---
NUM_BINS = 10
DATA_FOLDER = "data"
LOG_FOLDER = "logs"

# --- List of bin locations in the city ---
BIN_LOCATIONS = [
    "MG Road",
    "Station Square",
    "City Mall",
    "Bus Stand",
    "Park Street",
    "Hospital Road",
    "Market Area",
    "School Zone",
    "Airport Gate",
    "Tech Park"
]

def generate_bin_data(bin_id):
    """Simulates one bin sending its current fill level."""
    return {
        "bin_id": f"BIN_{bin_id:02d}",
        "location": BIN_LOCATIONS[bin_id - 1],
        "fill_level": round(random.uniform(0, 100), 2),
        "battery_level": round(random.uniform(20, 100), 2),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "active"
    }

def save_data(data):
    """Saves bin data to a JSON file."""
    filename = os.path.join(DATA_FOLDER, f"bin_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Saved {len(data)} bin records → {filename}")

def log_operation(message):
    """Writes a log entry to the logs folder."""
    log_file = os.path.join(LOG_FOLDER, "simulator.log")
    with open(log_file, "a") as f:
        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}\n")

def run_simulator(rounds=5):
    """Main function — runs the simulator for a given number of rounds."""
    print("=" * 50)
    print("  Smart City Waste Management — Sensor Simulator")
    print("=" * 50)
    log_operation("Simulator started")

    for round_num in range(1, rounds + 1):
        print(f"\n--- Round {round_num} of {rounds} ---")
        all_bin_data = []

        for bin_id in range(1, NUM_BINS + 1):
            bin_data = generate_bin_data(bin_id)
            all_bin_data.append(bin_data)
            print(f"  {bin_data['bin_id']} | {bin_data['location']:<15} | Fill: {bin_data['fill_level']}% | Battery: {bin_data['battery_level']}%")

        save_data(all_bin_data)
        log_operation(f"Round {round_num} completed — {NUM_BINS} bins recorded")

        if round_num < rounds:
            print("\n  Waiting 2 seconds for next reading...")
            time.sleep(2)

    print("\n" + "=" * 50)
    print("  Simulation complete!")
    print(f"  Check your 'data' folder — {rounds} JSON files created.")
    print(f"  Check your 'logs' folder — simulator.log created.")
    print("=" * 50)
    log_operation("Simulator finished successfully")

# --- Run it ---
if __name__ == "__main__":
    run_simulator(rounds=5)
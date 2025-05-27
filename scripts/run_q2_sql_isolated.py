import subprocess
import time
import statistics
import csv
import os

NUM_RUNS = 1
SCRIPT_PATH = "/opt/spark/work-dir/sql/q2-sql.py"
CONTAINER_NAME = "spark-master"
OUTPUT_CSV = "./analyses/performance_q2_sql_stats.csv"

def stop_spark_containers():
    subprocess.run(["docker", "stop", "spark-worker-1", "spark-worker-2", "spark-master"], check=True)
    subprocess.run(["docker", "rm", "spark-worker-1", "spark-worker-2", "spark-master"], check=True)

def start_spark_containers():
    subprocess.run(["docker-compose", "up", "-d", "--no-deps", "--build", "spark-master", "spark-worker-1", "spark-worker-2"], check=True)

def run_spark_job():
    result = subprocess.run(
        ["docker", "exec", CONTAINER_NAME, "spark-submit", SCRIPT_PATH],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    return result

def main():
    durations = []

    for i in range(NUM_RUNS):
        print(f"\n=== RUN {i+1} ===")

        print("🔻 Stopping Spark containers...")
        stop_spark_containers()
        time.sleep(5)

        print("🔼 Starting Spark containers...")
        start_spark_containers()

        print("⏳ Waiting for containers to fully initialize...")
        time.sleep(20)

        print("🚀 Launching Spark SQL job...")
        start_time = time.time()
        result = run_spark_job()
        end_time = time.time()

        duration = round(end_time - start_time, 2)
        durations.append(duration)

        print(f"✅ Run {i+1} completed in {duration} seconds")

        if result.returncode != 0:
            print("❌ Job failed with error:")
            print(result.stderr.decode())
            break

    # Calcola statistiche
    mean_duration = round(statistics.mean(durations), 2)
    stdev_duration = round(statistics.stdev(durations), 2) if len(durations) > 1 else 0.0

    print("\n📊 RISULTATI FINALI")
    print(f"Durate (s): {durations}")
    print(f"Media: {mean_duration} s")
    print(f"Deviazione standard: {stdev_duration} s")

    # Scrivi su CSV
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Run", "Duration (s)"])
        for idx, d in enumerate(durations, 1):
            writer.writerow([idx, d])
        writer.writerow([])
        writer.writerow(["Mean", mean_duration])
        writer.writerow(["Stdev", stdev_duration])

    print(f"\n📁 Dati salvati in: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()


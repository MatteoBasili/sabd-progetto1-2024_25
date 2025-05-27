import subprocess
import time
import statistics

NUM_RUNS = 10
DOCKER_COMPOSE_PATH = "."  # Modifica se il docker-compose.yml non è in cwd
OUTPUT_FILE = "./analyses/performance_q2_rdd_stats.txt"

# Comando per lanciare il job Spark all'interno del container spark-master
SPARK_SUBMIT_COMMAND = [
    "docker", "exec", "spark-master",
    "spark-submit",
    "/opt/spark/work-dir/q2-rdd.py"
]

def stop_and_remove_spark_containers():
    print("Stopping Spark containers...")
    subprocess.run(["docker-compose", "stop", "spark-worker-1", "spark-worker-2", "spark-master"],
                   cwd=DOCKER_COMPOSE_PATH, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print("Removing Spark containers...")
    subprocess.run(["docker-compose", "rm", "-f", "spark-worker-1", "spark-worker-2", "spark-master"],
                   cwd=DOCKER_COMPOSE_PATH, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def start_spark_containers():
    print("Starting Spark containers...")
    subprocess.run(["docker-compose", "up", "-d", "spark-master", "spark-worker-1", "spark-worker-2"],
                   cwd=DOCKER_COMPOSE_PATH, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print("Waiting 20 seconds for Spark to initialize...")
    time.sleep(20)  # Attendere che Spark sia pronto

def run_spark_job():
    print("Running Spark job...")
    start = time.time()
    result = subprocess.run(SPARK_SUBMIT_COMMAND, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    end = time.time()
    duration = end - start

    if result.returncode != 0:
        print("Spark job failed:")
        print(result.stderr)
    else:
        print("Spark job succeeded.")

    return duration

def main():
    durations = []
    for i in range(1, NUM_RUNS + 1):
        print(f"\n=== RUN {i} ===")
        stop_and_remove_spark_containers()
        start_spark_containers()
        duration = run_spark_job()
        durations.append(duration)
        print(f"Run {i} completed in {duration:.2f} seconds")

    avg_time = statistics.mean(durations)
    std_dev = statistics.stdev(durations) if len(durations) > 1 else 0.0

    with open(OUTPUT_FILE, "w") as f:
        f.write("=== PERFORMANCE STATISTICS ===\n")
        f.write(f"Number of runs: {NUM_RUNS}\n")
        f.write(f"Average execution time: {avg_time:.2f} seconds\n")
        f.write(f"Standard deviation: {std_dev:.2f} seconds\n")
        f.write(f"All run times: {[round(d, 2) for d in durations]}\n")

    print(f"\nPerformance statistics written to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()


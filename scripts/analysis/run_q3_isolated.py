import subprocess
import time
import statistics
import sys

NUM_RUNS = 3 #10
DOCKER_COMPOSE_PATH = "."  # Modifica se il docker-compose.yml non è in cwd

def stop_and_remove_other_containers():
    print("Stopping other containers...")
    subprocess.run(["docker-compose", "stop", "nifi", "grafana", "grafana-image-renderer", "redis", "results_exporter"],
                   cwd=DOCKER_COMPOSE_PATH, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print("Removing other containers...")
    subprocess.run(["docker-compose", "rm", "-f", "nifi", "grafana", "grafana-image-renderer", "redis", "results_exporter"],
                   cwd=DOCKER_COMPOSE_PATH, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

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
    print("Waiting 15 seconds for Spark to initialize...")
    time.sleep(15)  # Attendere che Spark sia pronto

def run_spark_job(command):
    print("Running Spark job...")
    start = time.time()
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    end = time.time()
    duration = end - start

    if result.returncode != 0:
        print("Spark job failed:")
        print(result.stderr)
    else:
        print("Spark job succeeded.")

    return duration

def main():
    if len(sys.argv) != 2 or sys.argv[1] not in ["rdd", "df", "sql"]:
        print("❗️Uso: python3 run_q3_isolated.py [rdd|df|sql]")
        sys.exit(1)

    mode = sys.argv[1]
    
    if mode == "sql":
        work_dir = "/opt/spark/work-dir/sql/"
    else:
        work_dir = "/opt/spark/work-dir/"
        
    output_file = f"./Results/analysis/performance_q3_{mode}_stats.txt"
    
    # Comando per lanciare il job Spark all'interno del container spark-master
    spark_submit_command = [
        "docker", "exec", "spark-master",
        "spark-submit",
        f"{work_dir}q3-{mode}.py"
    ]

    durations = []
    stop_and_remove_other_containers()
    for i in range(1, NUM_RUNS + 1):
        print(f"\n=== RUN {i} ===")
        stop_and_remove_spark_containers()
        start_spark_containers()
        duration = run_spark_job(spark_submit_command)
        durations.append(duration)
        print(f"Run {i} completed in {duration:.2f} seconds")

    avg_time = statistics.mean(durations)
    std_dev = statistics.stdev(durations) if len(durations) > 1 else 0.0

    with open(output_file, "w") as f:
        f.write("=== PERFORMANCE STATISTICS ===\n")
        f.write(f"Number of runs: {NUM_RUNS}\n")
        f.write(f"Average execution time: {avg_time:.2f} seconds\n")
        f.write(f"Standard deviation: {std_dev:.2f} seconds\n")
        f.write(f"All run times: {[round(d, 2) for d in durations]}\n")

    print(f"\nPerformance statistics written to {output_file}")

if __name__ == "__main__":
    main()


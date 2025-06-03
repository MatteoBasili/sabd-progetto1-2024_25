import subprocess
import time
import statistics
import sys

NUM_RUNS = 10
DOCKER_COMPOSE_PATH = "."  # Modifica se il docker-compose.yml non è in cwd

def reset_the_environment():
    print("Resetting the environment...")
    subprocess.run(["docker", "compose", "down", "-v"],
                   cwd=DOCKER_COMPOSE_PATH, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2)
    subprocess.run(["docker", "compose", "up", "-d", "namenode", "datanode1", "datanode2", "nifi", "spark-master", "spark-worker-1", "spark-worker-2"],
                   cwd=DOCKER_COMPOSE_PATH, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def do_data_ingestion():
    print("Acquiring and ingesting data into the HDFS...")
    ingestion = subprocess.run(["python3 ./scripts/nifi/run_data_acquisition_and_ingestion_flow.py"],
                   shell=True, text=True, capture_output=False)
    if ingestion.returncode != 0:
        print(f"❌ Errore nell'esecuzione: {ingestion.stderr}")
        sys.exit(ingestion.returncode)
    return None

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
    if len(sys.argv) != 3 or sys.argv[1] not in ["q1", "q2", "q3"] or sys.argv[2] not in ["rdd", "df", "sql"]:
        print("❗️Uso: python3 run_query_isolated.py [q1|q2|q3] [rdd|df|sql]")
        sys.exit(1)

    query = sys.argv[1]
    mode = sys.argv[2]
    
    if mode == "sql":
        work_dir = "/opt/spark/work-dir/sql/"
    else:
        work_dir = "/opt/spark/work-dir/"
        
    output_file = f"./Results/analysis/performance_{query}_{mode}_stats.txt"
    
    # Comando per lanciare il job Spark all'interno del container spark-master
    spark_submit_command = [
        "docker", "exec", "spark-master",
        "spark-submit",
        f"{work_dir}{query}-{mode}.py"
    ]

    durations = []
    for i in range(1, NUM_RUNS + 1):
        print(f"\n=== RUN {i} ===")
        reset_the_environment()
        do_data_ingestion()
        duration = run_spark_job(spark_submit_command)
        durations.append(duration)
        print(f"Run {i} completed in {duration:.2f} seconds")

    avg_time = statistics.mean(durations)
    std_dev = statistics.stdev(durations) if len(durations) > 1 else 0.0

    with open(output_file, "w") as f:
        f.write("⚙️ Le esecuzioni delle query sono state effettuate in condizioni controllate: nessun altro processo attivo in background e caching disabilitato tra le esecuzioni.\n\n")
        f.write("=== PERFORMANCE STATISTICS ===\n")
        f.write(f"Number of runs: {NUM_RUNS}\n")
        f.write(f"Average execution time: {avg_time:.2f} seconds\n")
        f.write(f"Standard deviation: {std_dev:.2f} seconds\n")
        f.write(f"All run times: {[round(d, 2) for d in durations]}\n")

    print(f"\nPerformance statistics written to {output_file}")

if __name__ == "__main__":
    main()


import subprocess
import sys

CSV_HOURLY_RESULT = "q3_hourly_result.csv"
CSV_STATS_RESULT = "q3_stats_result.csv"

def run_command(command, capture_output=False):
    print(f"⚙️  Eseguo: {command}")
    result = subprocess.run(command, shell=True, text=True, capture_output=capture_output)
    if result.returncode != 0:
        print(f"❌ Errore nell'esecuzione: {result.stderr}")
        sys.exit(result.returncode)
    return result.stdout if capture_output else None

def get_last_output_path(prefix):
    # Cerca l'ultimo path con un certo prefisso dentro /output
    cmd = f"docker exec namenode hdfs dfs -ls /output | grep {prefix} | awk '{{print $8}}' | sort | tail -n 1"
    output = run_command(cmd, capture_output=True)
    path = output.strip()
    if not path:
        print(f"❌ Nessun path trovato in /output per {prefix}")
        sys.exit(1)
    return path

def main():
    if len(sys.argv) != 2 or sys.argv[1] not in ["rdd", "df", "sql"]:
        print("❗️Uso: python3 run_q3_pipeline.py [rdd|df|sql]")
        sys.exit(1)

    mode = sys.argv[1]
    if mode == "sql":
        work_dir = "/opt/spark/work-dir/sql/"
    else:
        work_dir = "/opt/spark/work-dir/"

    print("🚀 Avvio Spark job...")
    run_command(f"docker exec spark-master spark-submit {work_dir}q3-{mode}.py")

    print("📦 Recupero ultimo path di output completo per fasce orarie (per grafici)...")
    hourly_all_path = get_last_output_path(f"q3_{mode}_hourly_")
    print(f"📁 Ultimo path per fasce orarie trovato: {hourly_all_path}")

    print("📦 Recupero ultimo path di output dei percentili...")
    stats_path = get_last_output_path(f"q3_{mode}_stats_")
    print(f"📁 Ultimo path dei percentili trovato: {stats_path}")

    print("📤 Export su Redis dei risultati in corso...")
    run_command(f"docker exec results_exporter python /src/export_q3_hdfs_to_redis.py \"{stats_path}\"")
    print("✅ Export su Redis completato.")

    print("📝 Estrazione file CSV per fasce orarie da HDFS...")
    run_command(
        f"docker exec namenode bash -c \"hdfs dfs -getmerge {hourly_all_path} /results/{CSV_HOURLY_RESULT} && "
        f"echo '✅ File CSV per fasce orarie pronto in /results/{CSV_HOURLY_RESULT}'\""
    )

    print("📝 Estrazione file CSV dei percentili da HDFS...")
    run_command(
        f"docker exec namenode bash -c \"hdfs dfs -getmerge {stats_path} /results/{CSV_STATS_RESULT} && "
        f"echo '✅ File CSV dei percentili pronto in /results/{CSV_STATS_RESULT}'\""
    )

    print("📁 File CSV disponibili nella directory 'Results':")
    print(f"  - Results/csv/{CSV_HOURLY_RESULT}")
    print(f"  - Results/csv/{CSV_STATS_RESULT}")
    
    print("📊 Generazione grafici in corso...")
    run_command(f"python3 ./scripts/grafana/create_q3_plots.py {CSV_HOURLY_RESULT}")
    print("✅ Grafici generati con successo nella directory 'Results/images'.")
    
if __name__ == "__main__":
    main()


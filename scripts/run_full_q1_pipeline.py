import subprocess
import sys
import time

CSV_RESULT = "q1_result.csv"

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
        print("❗️Uso: python3 run_full_q1_pipeline.py [rdd|df|sql]")
        sys.exit(1)

    mode = sys.argv[1]
    if mode == "sql":
        work_dir = "/opt/spark/work-dir/sql/"
    else:
        work_dir = "/opt/spark/work-dir/"

    print("🚀 Avvio Spark job...")
    run_command(f"docker exec spark-master spark-submit {work_dir}q1-{mode}.py")

    print("📦 Recupero ultimo path di output HDFS...")
    last_path = get_last_output_path(f"q1_{mode}_")
    print(f"📁 Ultimo path di output trovato: {last_path}")

    print("📤 Export su Redis dei risultati in corso...")
    run_command(f"docker exec spark-master python /opt/spark/export/export_q1_hdfs_to_redis.py \"{last_path}\"")
    print("✅ Export su Redis completato.")

    print("📝 Estrazione file CSV da HDFS...")
    run_command(
        f"docker exec namenode bash -c \"hdfs dfs -getmerge {last_path} /results/{CSV_RESULT} && "
        f"echo '✅ File CSV pronto in /results/{CSV_RESULT}'\""
    )

    print("📁 File CSV disponibile nella directory 'Results':")
    print(f"  - Results/csv/{CSV_RESULT}")
    
    print("📊 Generazione grafici in corso...")
    run_command(f"python3 ./scripts/grafana/create_q1_plots.py {CSV_RESULT}")
    print("✅ Grafici generati con successo nella directory 'Results/images'.")
    
if __name__ == "__main__":
    main()


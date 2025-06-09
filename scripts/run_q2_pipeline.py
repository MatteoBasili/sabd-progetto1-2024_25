import subprocess
import sys

CSV_ALL_RESULT = "q2_all_result.csv"
CSV_TOP5_RESULT = "q2_top_result.csv"

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
        print("❗️Uso: python3 run_q2_pipeline.py [rdd|df|sql]")
        sys.exit(1)

    mode = sys.argv[1]
    if mode == "sql":
        work_dir = "/opt/spark/work-dir/sql/"
    else:
        work_dir = "/opt/spark/work-dir/"

    print("🚀 Avvio Spark job...")
    run_command(f"docker exec spark-master spark-submit --master spark://spark-master:7077 {work_dir}q2-{mode}.py")

    print("📦 Recupero ultimo path di output mensile completo (per grafici)...")
    monthly_all_path = get_last_output_path(f"q2_{mode}_all_")
    print(f"📁 Ultimo path mensile trovato: {monthly_all_path}")

    print("📦 Recupero ultimo path di output top 5...")
    top5_path = get_last_output_path(f"q2_{mode}_top_")
    print(f"📁 Ultimo path top 5 trovato: {top5_path}")

    print("📤 Export su Redis dei risultati in corso...")
    run_command(f"docker exec results_exporter python /src/export_q2_hdfs_to_redis.py \"{top5_path}\"")
    print("✅ Export su Redis completato.")

    print("📝 Estrazione file CSV mensile completo da HDFS...")
    run_command(
        f"docker exec namenode bash -c \"hdfs dfs -getmerge {monthly_all_path} /results/{CSV_ALL_RESULT} && "
        f"echo '✅ File CSV mensile pronto in /results/{CSV_ALL_RESULT}'\""
    )

    print("📝 Estrazione file CSV top 5 da HDFS...")
    run_command(
        f"docker exec namenode bash -c \"hdfs dfs -getmerge {top5_path} /results/{CSV_TOP5_RESULT} && "
        f"echo '✅ File CSV top 5 pronto in /results/{CSV_TOP5_RESULT}'\""
    )

    print("📁 File CSV disponibili nella directory 'Results':")
    print(f"  - Results/csv/{CSV_ALL_RESULT}")
    print(f"  - Results/csv/{CSV_TOP5_RESULT}")
    
    print("📊 Generazione grafici in corso...")
    run_command(f"python3 ./scripts/grafana/create_q2_plots.py {CSV_ALL_RESULT}")
    print("✅ Grafici generati con successo nella directory 'Results/images'.")
    
if __name__ == "__main__":
    main()


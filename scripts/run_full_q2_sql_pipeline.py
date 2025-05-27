import subprocess
import sys
import time

def run_command(command, capture_output=False):
    print(f"⚙️  Eseguo: {command}")
    result = subprocess.run(command, shell=True, text=True, capture_output=capture_output)
    if result.returncode != 0:
        print(f"❌ Errore nell'esecuzione: {result.stderr}")
        sys.exit(result.returncode)
    return result.stdout if capture_output else None

def main():
    print("🚀 Avvio Spark job...")
    run_command("docker exec spark-master spark-submit /opt/spark/work-dir/sql/q2-sql.py")

    print("📦 Recupero ultimo path di output HDFS...")
    hdfs_list_output = run_command(
        "docker exec namenode hdfs dfs -ls /output | grep q2_sql_ | awk '{print $8}' | sort | tail -n 1",
        capture_output=True
    )

    last_output_path = hdfs_list_output.strip()
    if not last_output_path:
        print("❌ Nessun path trovato in /output per q2_sql_")
        sys.exit(1)

    print(f"📁 Ultimo path HDFS trovato: {last_output_path}")

    print("📤 Esportazione su Redis in corso...")
    run_command(f"docker exec spark-master python /opt/spark/export/export_q2_hdfs_to_redis.py \"{last_output_path}\"")
    print("✅ Export su Redis completato.")

    print("📝 Estrazione dei risultati da HDFS...")
    run_command(
        f"docker exec namenode bash -c \"hdfs dfs -getmerge {last_output_path} /shared_data/q2_sql_result.csv && "
        f"echo '✅ File CSV pronto in /shared_data/q2_sql_result.csv'\""
    )

    print("📁 File CSV disponibile nella directory montata: ./hdfs/shared_data/q2_sql_result.csv")

if __name__ == "__main__":
    main()


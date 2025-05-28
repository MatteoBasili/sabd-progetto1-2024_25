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
    print("🚀 Avvio Spark job...")
    run_command("docker exec spark-master spark-submit /opt/spark/work-dir/q1-rdd.py")

    print("📦 Recupero ultimo path di output HDFS...")
    last_path = get_last_output_path("q1_rdd_")
    print(f"📁 Ultimo path di output trovato: {last_path}")

    print("📤 Export su Redis dei risultati in corso...")
    run_command(f"docker exec spark-master python /opt/spark/export/export_q1_hdfs_to_redis.py \"{last_path}\"")
    print("✅ Export su Redis completato.")

    print("📝 Estrazione file CSV da HDFS...")
    run_command(
        f"docker exec namenode bash -c \"hdfs dfs -getmerge {last_path} /results/q1_rdd_result.csv && "
        f"echo '✅ File CSV pronto in /results/q1_rdd_result.csv'\""
    )

    print("📁 File CSV disponibile nella directory 'Results':")
    print("  - Results/csv/q1_rdd_result.csv")
    
if __name__ == "__main__":
    main()


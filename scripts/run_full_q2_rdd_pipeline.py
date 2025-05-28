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
    run_command("docker exec spark-master spark-submit /opt/spark/work-dir/q2-rdd.py")

    print("📦 Recupero ultimo path di output mensile completo (per grafici)...")
    monthly_all_path = get_last_output_path("q2_rdd_all_")
    print(f"📁 Ultimo path mensile trovato: {monthly_all_path}")

    print("📦 Recupero ultimo path di output top 5...")
    top5_path = get_last_output_path("q2_rdd_top_")
    print(f"📁 Ultimo path top 5 trovato: {top5_path}")

    print("📤 Export su Redis del file top 5 in corso...")
    run_command(f"docker exec spark-master python /opt/spark/export/export_q2_hdfs_to_redis.py \"{top5_path}\"")
    print("✅ Export su Redis completato.")

    print("📝 Estrazione file CSV mensile completo da HDFS...")
    run_command(
        f"docker exec namenode bash -c \"hdfs dfs -getmerge {monthly_all_path} /shared_data/q2_rdd_all.csv && "
        f"echo '✅ File CSV mensile pronto in /shared_data/q2_rdd_all.csv'\""
    )

    print("📝 Estrazione file CSV top 5 da HDFS...")
    run_command(
        f"docker exec namenode bash -c \"hdfs dfs -getmerge {top5_path} /shared_data/q2_rdd_top.csv && "
        f"echo '✅ File CSV top 5 pronto in /shared_data/q2_rdd_top.csv'\""
    )

    print("📁 File CSV disponibili nella directory montata:")
    print("  - ./hdfs/shared_data/q2_rdd_all.csv")
    print("  - ./hdfs/shared_data/q2_rdd_top.csv")
    
if __name__ == "__main__":
    main()


import sys
from hdfs import InsecureClient
import redis
import glob

if len(sys.argv) != 2:
    print("Usage: python export_hdfs_to_redis.py <hdfs_output_path>")
    sys.exit(1)

# Connessione a WebHDFS (porta 9870)
hdfs_client = InsecureClient('http://namenode:9870', user='root')

# Connetti a Redis (il nome del container Redis nella compose)
redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)

# Percorso nella HDFS del job Spark
hdfs_output_dir = sys.argv[1]

# Leggi tutti i file part-* (esclude _SUCCESS)
status = hdfs_client.list(hdfs_output_dir)
part_files = [f for f in status if f.startswith('part-')]

for part in part_files:
    with hdfs_client.read(f"{hdfs_output_dir}/{part}", encoding='utf-8') as reader:
        for line in reader:
            if line.startswith("date") or not line.strip():
                continue  # Salta intestazioni o righe vuote
            parts = line.strip().split(",")
            year, country = parts[0], parts[1]
            key = f"{country}:{year}"
            value = {
                "carbon_mean": parts[2],
                "carbon_min": parts[3],
                "carbon_max": parts[4],
                "cfe_mean": parts[5],
                "cfe_min": parts[6],
                "cfe_max": parts[7],
            }
            redis_client.hset(key, mapping=value)
            print(f"✅ Inserito in Redis: {key} → {value}")


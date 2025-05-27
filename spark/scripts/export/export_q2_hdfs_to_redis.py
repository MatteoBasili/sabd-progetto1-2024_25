import sys
from hdfs import InsecureClient
import redis

if len(sys.argv) != 2:
    print("Usage: python export_q2_hdfs_to_redis.py <hdfs_output_path>")
    sys.exit(1)

# Connessione a WebHDFS
hdfs_client = InsecureClient('http://namenode:9870', user='root')

# Connessione a Redis
redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)

# Directory output HDFS della query Q2
hdfs_output_dir = sys.argv[1]

# Filtra i file part-*
status = hdfs_client.list(hdfs_output_dir)
part_files = [f for f in status if f.startswith('part-')]

for part in part_files:
    with hdfs_client.read(f"{hdfs_output_dir}/{part}", encoding='utf-8') as reader:
        for line in reader:
            if line.startswith("date") or not line.strip():
                continue  # Salta intestazioni o righe vuote

            parts = line.strip().split(",")
            if len(parts) < 4:
                continue  # Salta righe incomplete

            date = parts[0]        # es: 2022_3
            carbon = parts[1]      # media carbon intensity
            cfe = parts[2]         # media CFE
            order_type = parts[3]  # es: carbon_desc

            redis_key = f"q2:{order_type}:{date}"
            redis_value = {
                "carbon": carbon,
                "cfe": cfe
            }

            redis_client.hset(redis_key, mapping=redis_value)
            print(f"✅ Inserito in Redis: {redis_key} → {redis_value}")


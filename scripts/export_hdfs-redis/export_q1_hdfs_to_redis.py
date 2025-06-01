import sys
import os
from hdfs import InsecureClient
import redis

def main(hdfs_output_path, hdfs_host, redis_host, redis_port):
    # Connetti a WebHDFS
    hdfs_client = InsecureClient(f'http://{hdfs_host}:9870', user='root')

    # Connetti a Redis
    redis_client = redis.Redis(host=redis_host, port=int(redis_port), decode_responses=True)
    pipe = redis_client.pipeline(transaction=False)

    # Lista i file part-*
    status = hdfs_client.list(hdfs_output_path)
    part_files = [f for f in status if f.startswith('part-')]

    for part_file in part_files:
        with hdfs_client.read(f"{hdfs_output_path}/{part_file}", encoding='utf-8') as reader:
            for line in reader:
                line = line.strip()
                if not line or line.startswith("year"):
                    continue
                parts = line.split(",")
                if len(parts) < 8:
                    continue  # ignora righe incomplete

                year, country = parts[0], parts[1]
                key = f"Q1:{country}:{year}"
                value = {
                    "carbon_mean": parts[2],
                    "carbon_min": parts[3],
                    "carbon_max": parts[4],
                    "cfe_mean": parts[5],
                    "cfe_min": parts[6],
                    "cfe_max": parts[7],
                }

                pipe.hset(key, mapping=value)
                print(f"✅ Inserito in Redis: {key} → {value}")

    # Esegui tutte le scritture in batch
    pipe.execute()
    print("✅ Esportazione completata con successo.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python export_q1_hdfs_to_redis.py <hdfs_output_path>")
        sys.exit(1)
        
    # Parametri da env o default
    HDFS_HOST = os.getenv('HDFS_HOST', 'namenode')
    REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
    REDIS_PORT = os.getenv('REDIS_PORT', '6379')

    hdfs_output_dir = sys.argv[1]
    main(hdfs_output_dir, HDFS_HOST, REDIS_HOST, REDIS_PORT)


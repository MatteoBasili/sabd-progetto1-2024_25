from pyspark.sql import SparkSession
from datetime import datetime
from collections import defaultdict
import csv
import io

def parse_line(line):
    # Parsing CSV string safely (inclusi eventuali valori con virgole)
    values = next(csv.reader([line]))
    try:
        dt = values[0]
        year = int(dt[:4])
        country = values[1]
        carbon = float(values[2])
        cfe = float(values[3])
        return ((year, country), (carbon, cfe, 1, carbon, carbon, cfe, cfe))
    except:
        return None  # skip malformed lines

def combine(a, b):
    return (
        a[0] + b[0],  # sum carbon
        a[1] + b[1],  # sum cfe
        a[2] + b[2],  # count
        min(a[3], b[3]),  # min carbon
        max(a[4], b[4]),  # max carbon
        min(a[5], b[5]),  # min cfe
        max(a[6], b[6])   # max cfe
    )

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ElectricityStatsQ1RDD_CSV") \
        .getOrCreate()

    sc = spark.sparkContext

    # Percorsi CSV
    path_it = "hdfs://namenode:9000/data/electricity/italy_2021_2024_clean.csv"
    path_se = "hdfs://namenode:9000/data/electricity/sweden_2021_2024_clean.csv"

    # Carica e unisci RDD
    rdd = sc.textFile(path_it).union(sc.textFile(path_se))

    # Rimuovi header
    header = rdd.first()
    rdd_data = rdd.filter(lambda row: row != header)

    # Parsing e trasformazioni
    parsed = rdd_data.map(parse_line).filter(lambda x: x is not None)

    # Aggregazione per (year, country)
    aggregated = parsed.reduceByKey(combine)

    # Calcolo statistiche
    final_result = aggregated.map(lambda kv: (
        kv[0][0],  # year
        kv[0][1],  # country
        round(kv[1][0] / kv[1][2], 6),  # avg carbon
        round(kv[1][3], 2),             # min carbon
        round(kv[1][4], 2),             # max carbon
        round(kv[1][1] / kv[1][2], 6),  # avg cfe
        round(kv[1][5], 2),             # min cfe
        round(kv[1][6], 2)              # max cfe
    ))

    # Ordina per country e anno
    sorted_result = final_result.sortBy(lambda x: (x[1], x[0]))

    # Aggiungi header + salva
    output_lines = sorted_result.map(lambda x: f"{x[0]},{x[1]},{x[2]},{x[3]},{x[4]},{x[5]},{x[6]},{x[7]}")
    header_line = sc.parallelize(["year,country,carbon-mean,carbon-min,carbon-max,cfe-mean,cfe-min,cfe-max"])
    full_output = header_line.union(output_lines)

    # Scrivi in CSV su HDFS
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    output_path = f"hdfs://namenode:9000/output/q1_rdd_{timestamp}"
    full_output.saveAsTextFile(output_path)

    spark.stop()


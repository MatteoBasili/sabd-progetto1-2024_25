from pyspark import SparkConf, SparkContext
from datetime import datetime
import subprocess

def parse_line(line):
    try:
        parts = line.split(",")
        if parts[0] == "datetime":
            return None  # Skip header
        date = datetime.strptime(parts[0], "%Y-%m-%d %H:%M:%S")
        year = date.year
        country = parts[1]
        carbon = float(parts[2])
        cfe = float(parts[3])
        return ((year, country), (carbon, cfe, carbon, carbon, cfe, cfe, 1))
    except:
        return None

def reduce_stats(a, b):
    return (
        a[0] + b[0],  # sum of carbon
        a[1] + b[1],  # sum of cfe
        min(a[2], b[2]),  # min carbon
        max(a[3], b[3]),  # max carbon
        min(a[4], b[4]),  # min cfe
        max(a[5], b[5]),  # max cfe
        a[6] + b[6]  # count
    )

if __name__ == "__main__":
    conf = SparkConf().setAppName("ElectricityStatsQ1RDD")
    sc = SparkContext(conf=conf)

    hdfs_path_it = "hdfs://namenode:9000/data/electricity/italy_2021_2024_clean.csv"
    hdfs_path_se = "hdfs://namenode:9000/data/electricity/sweden_2021_2024_clean.csv"

    data_it = sc.textFile(hdfs_path_it)
    data_se = sc.textFile(hdfs_path_se)

    data = data_it.union(data_se)
    parsed = data.map(parse_line).filter(lambda x: x is not None)

    reduced = parsed.reduceByKey(reduce_stats)

    results = reduced.map(lambda x: (
        x[0][0],  # year
        x[0][1],  # country
        round(x[1][0] / x[1][6], 6),  # mean carbon
        round(x[1][2], 2),            # min carbon
        round(x[1][3], 2),            # max carbon
        round(x[1][1] / x[1][6], 6),  # mean cfe
        round(x[1][4], 2),            # min cfe
        round(x[1][5], 2),            # max cfe
    )).sortBy(lambda x: (x[1], x[0]))  # sort by country, then year

    # Save results as CSV
    output = results.map(lambda x: f"{x[0]},{x[1]},{x[2]},{x[3]},{x[4]},{x[5]},{x[6]},{x[7]}")
    header = sc.parallelize(["date,country,carbon-mean,carbon-min,carbon-max,cfe-mean,cfe-min,cfe-max"])
    full_output = header.union(output)
    output_path = f"/output/q1_rdd_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    full_output.saveAsTextFile(f"hdfs://namenode:9000{output_path}")

    sc.stop()
    
    # Lancia lo script exporter e passagli il path
    print("✅ Job Spark completato. Avvio export su Redis...")

    subprocess.run(["python", "/opt/spark/export/export_hdfs_to_redis.py", output_path], check=True)

    print("✅ Export completato.")


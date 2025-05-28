from pyspark import SparkConf, SparkContext
from datetime import datetime

def parse_line(line):
    try:
        parts = line.split(",")
        if parts[0] == "datetime":
            return None  # Skip header
        dt = datetime.strptime(parts[0], "%Y-%m-%d %H:%M:%S")
        date = dt.strftime("%Y-%m")
        carbon = float(parts[2])
        cfe = float(parts[3])
        return (date, (carbon, cfe, 1))
    except:
        return None

def reduce_values(a, b):
    return (
        a[0] + b[0],  # sum carbon
        a[1] + b[1],  # sum cfe
        a[2] + b[2]   # count
    )

def format_monthly_result(record):
    date, (carbon_sum, cfe_sum, count) = record
    carbon_avg = round(carbon_sum / count, 6)
    cfe_avg = round(cfe_sum / count, 6)
    return (date, carbon_avg, cfe_avg)

if __name__ == "__main__":
    conf = SparkConf().setAppName("ElectricityStatsQ2RDD_Updated")
    sc = SparkContext(conf=conf)

    hdfs_path = "hdfs://namenode:9000/data/electricity/italy_2021_2024_clean.csv"
    raw_data = sc.textFile(hdfs_path)

    parsed = raw_data.map(parse_line).filter(lambda x: x is not None)
    reduced = parsed.reduceByKey(reduce_values)
    monthly = reduced.map(format_monthly_result)

    # ---- Parte 1: Salva tutti i dati mensili ----
    all_monthly_sorted = monthly.sortBy(lambda x: x[0]) \
        .map(lambda x: f"{x[0]},{x[1]},{x[2]}")
    
    header_all = sc.parallelize(["date,carbon-intensity,cfe"])
    final_all = header_all.union(all_monthly_sorted)

    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    output_path_all = f"/output/q2_rdd_all_{timestamp}"
    final_all.coalesce(1).saveAsTextFile(f"hdfs://namenode:9000{output_path_all}")

    # ---- Parte 2: Top 5 classifiche ----
    results_list = monthly.collect()
    top_carbon_desc = sorted(results_list, key=lambda x: -x[1])[:5]
    top_carbon_asc = sorted(results_list, key=lambda x: x[1])[:5]
    top_cfe_desc = sorted(results_list, key=lambda x: -x[2])[:5]
    top_cfe_asc = sorted(results_list, key=lambda x: x[2])[:5]

    labeled = []
    labeled += [(d, c, f, "carbon_desc") for (d, c, f) in top_carbon_desc]
    labeled += [(d, c, f, "carbon_asc") for (d, c, f) in top_carbon_asc]
    labeled += [(d, c, f, "cfe_desc") for (d, c, f) in top_cfe_desc]
    labeled += [(d, c, f, "cfe_asc") for (d, c, f) in top_cfe_asc]

    rdd_top = sc.parallelize(labeled)
    header_top = sc.parallelize(["date,carbon-intensity,cfe,order_type"])
    final_top = header_top.union(rdd_top.map(lambda x: f"{x[0]},{x[1]},{x[2]},{x[3]}"))

    output_path_top = f"/output/q2_rdd_top_{timestamp}"
    final_top.coalesce(1).saveAsTextFile(f"hdfs://namenode:9000{output_path_top}")

    sc.stop()


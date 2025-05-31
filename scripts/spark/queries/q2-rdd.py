from pyspark import SparkConf, SparkContext
from datetime import datetime

def parse_line(line):
    try:
        parts = line.split(",")
        if parts[0] == "datetime":
            return None
        dt = datetime.strptime(parts[0], "%Y-%m-%d %H:%M:%S")
        date = dt.strftime("%Y-%m")
        carbon = float(parts[2])
        cfe = float(parts[3])
        return (date, (carbon, cfe, 1))
    except:
        return None

def reduce_values(a, b):
    return (
        a[0] + b[0],  # carbon
        a[1] + b[1],  # cfe
        a[2] + b[2]   # count
    )

def compute_avg(record):
    date, (carbon_sum, cfe_sum, count) = record
    carbon_avg = round(carbon_sum / count, 6)
    cfe_avg = round(cfe_sum / count, 6)
    return (date, carbon_avg, cfe_avg)

def rank_list(data, key_fn, label, ascending=True):
    sorted_data = sorted(data, key=key_fn, reverse=not ascending)[:5]
    return [(i+1, d[0], d[1], d[2], label) for i, d in enumerate(sorted_data)]

if __name__ == "__main__":
    conf = SparkConf().setAppName("ElectricityStatsQ2RDD_CSV")
    sc = SparkContext(conf=conf)

    hdfs_path = "hdfs://namenode:9000/data/electricity/italy_2021_2024_clean.csv"
    rdd = sc.textFile(hdfs_path)
    
    # Parsing + Aggregazione mensile
    monthly = (
        rdd.map(parse_line)
           .filter(lambda x: x is not None)
           .reduceByKey(reduce_values)
           .map(compute_avg)
           .cache()
    )

    # Salvataggio CSV ordinato per data (grafici)
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    output_path_all = f"/output/q2_rdd_all_{timestamp}"
    header_all = sc.parallelize(["date,carbon-intensity,cfe"])
    formatted = monthly.sortBy(lambda x: x[0]) \
                       .map(lambda x: f"{x[0]},{x[1]},{x[2]}")
    final_all = header_all.union(formatted)
    final_all.saveAsTextFile(f"hdfs://namenode:9000{output_path_all}")

    # Raccolta per classifiche
    all_data = monthly.collect()

    # Calcolo classifiche ordinate per categoria
    top_carbon_desc = rank_list(all_data, key_fn=lambda x: x[1], label="carbon_desc", ascending=False)
    top_carbon_asc  = rank_list(all_data, key_fn=lambda x: x[1], label="carbon_asc",  ascending=True)
    top_cfe_desc    = rank_list(all_data, key_fn=lambda x: x[2], label="cfe_desc",    ascending=False)
    top_cfe_asc     = rank_list(all_data, key_fn=lambda x: x[2], label="cfe_asc",     ascending=True)

    # Concatenazione in ordine richiesto
    top5 = top_carbon_desc + top_carbon_asc + top_cfe_desc + top_cfe_asc

    # Conversione in RDD e salvataggio classifiche
    rdd_top = sc.parallelize(top5)
    header_top = sc.parallelize(["date,carbon-intensity,cfe,order_type,rank"])
    lines_top = rdd_top.map(lambda x: f"{x[1]},{x[2]},{x[3]},{x[4]},{x[0]}")
    final_top = header_top.union(lines_top)

    output_path_top = f"/output/q2_rdd_top_{timestamp}"
    final_top.saveAsTextFile(f"hdfs://namenode:9000{output_path_top}")

    sc.stop()


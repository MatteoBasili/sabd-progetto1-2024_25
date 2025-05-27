from pyspark import SparkConf, SparkContext
from datetime import datetime

def parse_line(line):
    try:
        parts = line.split(",")
        if parts[0] == "datetime":
            return None  # salta header
        dt = datetime.strptime(parts[0], "%Y-%m-%d %H:%M:%S")
        year = dt.year
        month = dt.month
        carbon = float(parts[2])
        cfe = float(parts[3])
        key = (year, month)
        return (key, (carbon, cfe, 1))
    except:
        return None

def reduce_values(a, b):
    return (
        a[0] + b[0],  # somma carbon
        a[1] + b[1],  # somma cfe
        a[2] + b[2]   # conteggio
    )

def format_result(record):
    (year, month), (carbon_sum, cfe_sum, count) = record
    carbon_avg = round(carbon_sum / count, 6)
    cfe_avg = round(cfe_sum / count, 6)
    date = f"{year}_{month}"
    return (date, carbon_avg, cfe_avg)

if __name__ == "__main__":
    conf = SparkConf().setAppName("ElectricityStatsQ2RDD")
    sc = SparkContext(conf=conf)

    hdfs_path = "hdfs://namenode:9000/data/electricity/italy_2021_2024_clean.csv"
    raw_data = sc.textFile(hdfs_path)

    parsed = raw_data.map(parse_line).filter(lambda x: x is not None)
    reduced = parsed.reduceByKey(reduce_values)
    results = reduced.map(format_result)

    # Recupera le classifiche richieste
    top_carbon_desc = results.takeOrdered(5, key=lambda x: -x[1])
    top_carbon_asc = results.takeOrdered(5, key=lambda x: x[1])
    top_cfe_desc = results.takeOrdered(5, key=lambda x: -x[2])
    top_cfe_asc = results.takeOrdered(5, key=lambda x: x[2])

    # Aggiungi il tipo di ordinamento
    labeled = []
    labeled += [(d, c, f, "carbon_desc") for (d, c, f) in top_carbon_desc]
    labeled += [(d, c, f, "carbon_asc") for (d, c, f) in top_carbon_asc]
    labeled += [(d, c, f, "cfe_desc") for (d, c, f) in top_cfe_desc]
    labeled += [(d, c, f, "cfe_asc") for (d, c, f) in top_cfe_asc]

    # Salvataggio in HDFS come file CSV con un solo header
    rdd_output = sc.parallelize(labeled)
    header = sc.parallelize(["date,carbon-intensity,cfe,order_type"])
    final_output = header.union(rdd_output.map(lambda x: f"{x[0]},{x[1]},{x[2]},{x[3]}"))

    output_path = f"/output/q2_rdd_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    final_output.coalesce(1).saveAsTextFile(f"hdfs://namenode:9000{output_path}")

    sc.stop()


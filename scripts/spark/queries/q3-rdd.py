from pyspark import SparkConf, SparkContext
from datetime import datetime

def parse_line(line):
    try:
        parts = line.split(",")
        dt = datetime.strptime(parts[0], '%Y-%m-%d %H:%M:%S')
        hour = int(dt.strftime("%H"))
        country = parts[1]
        carbon = float(parts[2])
        cfe = float(parts[3])
        return (hour, country, carbon, cfe)
    except:
        return None
        
# Calcolo medie orarie
def map_key_for_avg(row):
    hour, country, carbon, cfe = row
    return ((hour, country), (carbon, cfe, 1))  # (key), (sum_c, sum_cfe, count)

def reduce_avg(a, b):
    return (a[0]+b[0], a[1]+b[1], a[2]+b[2])

# Percentili
def get_percentiles(values):
    """Calcola [min, 25%, 50%, 75%, max] con interpolazione lineare"""
    sorted_vals = sorted(values)
    n = len(sorted_vals)

    def interpolate(p):
        pos = p * (n - 1)
        lower = int(pos)
        upper = min(lower + 1, n - 1)
        weight = pos - lower
        return round(sorted_vals[lower] * (1 - weight) + sorted_vals[upper] * weight, 6)

    return [
        round(sorted_vals[0], 6),           # min
        interpolate(0.25),                  # 25th
        interpolate(0.5),                   # 50th
        interpolate(0.75),                  # 75th
        round(sorted_vals[-1], 6)           # max
    ]

# Raggruppamento per (country, metrica)
def metric_pair(row):
    hour, country, carbon, cfe = row
    return [((country, "carbon-intensity"), carbon), ((country, "cfe"), cfe)]

if __name__ == "__main__":
    conf = SparkConf().setAppName("ElectricityStatsQ3RDD_CSV")
    sc = SparkContext(conf=conf)

    # Percorsi CSV
    path_it = "hdfs://namenode:9000/data/electricity/italy_2021_2024_clean.csv"
    path_se = "hdfs://namenode:9000/data/electricity/sweden_2021_2024_clean.csv"

    # Lettura file e rimozione header
    raw_it = sc.textFile(path_it)
    header_it = raw_it.first()
    rdd_it = raw_it.filter(lambda line: line != header_it) \
                   .map(parse_line) \
                   .filter(lambda x: x is not None)

    raw_se = sc.textFile(path_se)
    header_se = raw_se.first()
    rdd_se = raw_se.filter(lambda line: line != header_se) \
                   .map(parse_line) \
                   .filter(lambda x: x is not None)

    # Unione
    rdd_all = rdd_it.union(rdd_se)  # (hour, country, carbon, cfe)

    hourly_avg = rdd_all.map(map_key_for_avg) \
                        .reduceByKey(reduce_avg) \
                        .mapValues(lambda x: (
                            round(x[0]/x[2], 6),
                            round(x[1]/x[2], 6)
                        )) \
                        .map(lambda x: (x[0][0], x[0][1], x[1][0], x[1][1])) \
                        .cache()
                        
    hourly_avg_sorted = hourly_avg.sortBy(lambda x: (x[1], x[0]))  # sort by country, then hour


    # Salvataggio CSV: valori medi orari
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    output_hourly_path = f"hdfs://namenode:9000/output/q3_rdd_hourly_{timestamp}"
    header_hourly = "hour,country,carbon-intensity,cfe"

    sc.parallelize([header_hourly]) \
      .union(hourly_avg_sorted.map(lambda row: f"{row[0]},{row[1]},{row[2]},{row[3]}")) \
      .saveAsTextFile(output_hourly_path)

    # Calcola percentili sui valori medi orari
    carbon_it = hourly_avg.filter(lambda x: x[1] == "IT").map(lambda x: x[2]).collect()
    cfe_it = hourly_avg.filter(lambda x: x[1] == "IT").map(lambda x: x[3]).collect()

    carbon_se = hourly_avg.filter(lambda x: x[1] == "SE").map(lambda x: x[2]).collect()
    cfe_se = hourly_avg.filter(lambda x: x[1] == "SE").map(lambda x: x[3]).collect()

    # Calcolo dei percentili
    stats_final = sc.parallelize([
        ("IT", "carbon-intensity", *get_percentiles(carbon_it)),
        ("IT", "cfe", *get_percentiles(cfe_it)),
        ("SE", "carbon-intensity", *get_percentiles(carbon_se)),
        ("SE", "cfe", *get_percentiles(cfe_se)),
    ])

    # Salvataggio
    output_stats_path = f"hdfs://namenode:9000/output/q3_rdd_stats_{timestamp}"
    header_stats = "country,data,min,25-perc,50-perc,75-perc,max"
    sc.parallelize([header_stats]) \
      .union(stats_final.map(lambda r: f"{r[0]},{r[1]},{r[2]},{r[3]},{r[4]},{r[5]},{r[6]}")) \
      .saveAsTextFile(output_stats_path)

    sc.stop()


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, avg, round, lit, expr
from datetime import datetime

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ElectricityStatsQ3DF_Parquet") \
        .getOrCreate()

    # Percorsi HDFS
    hdfs_path_it = "hdfs://namenode:9000/data/electricity/italy_2021_2024_clean.parquet"
    hdfs_path_se = "hdfs://namenode:9000/data/electricity/sweden_2021_2024_clean.parquet"

    # Caricamento dati
    df_it = spark.read.parquet(hdfs_path_it)
    df_se = spark.read.parquet(hdfs_path_se)

    # Preprocessing
    def preprocess(df, country_code):
        return df.withColumn("datetime", col("datetime").cast("timestamp")) \
                 .withColumn("hour", hour("datetime")) \
                 .withColumn("carbon", col("carbon_intensity_direct").cast("double")) \
                 .withColumn("cfe", col("cfe_percent").cast("double")) \
                 .withColumn("country", lit(country_code)) \
                 .select("hour", "country", "carbon", "cfe")

    df_it = preprocess(df_it, "IT")
    df_se = preprocess(df_se, "SE")

    # Calcola media per ora per ciascun paese (24 valori ciascuno)
    hourly_avg_it = df_it.groupBy("hour").agg(
        round(avg("carbon"), 6).alias("carbon-intensity"),
        round(avg("cfe"), 6).alias("cfe")
    ).withColumn("country", lit("IT"))

    hourly_avg_se = df_se.groupBy("hour").agg(
        round(avg("carbon"), 6).alias("carbon-intensity"),
        round(avg("cfe"), 6).alias("cfe")
    ).withColumn("country", lit("SE"))

    # Unione
    hourly_avg = hourly_avg_it.union(hourly_avg_se).cache()

    # Riorganizza per esportazione
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    output_path_hourly = f"/output/q3_df_hourly_{timestamp}"
    hourly_avg.select("hour", "country", "carbon-intensity", "cfe") \
        .orderBy("country", "hour") \
        .write.option("header", "true") \
        .csv(f"hdfs://namenode:9000{output_path_hourly}")

    # Percentili: calcolati sui 24 valori medi per ora
    def calc_percentiles(df, metric, country_code):
        return df.filter(col("country") == country_code) \
                 .select(col(metric).cast("double").alias("value")) \
                 .agg(
                     expr("percentile(value, array(0.0, 0.25, 0.5, 0.75, 1.0))").alias("percentiles")
                 ) \
                 .withColumn("country", lit(country_code)) \
                 .withColumn("data", lit(metric)) \
                 .select(
                     "country", "data",
                     round(col("percentiles")[0], 6).alias("min"),
                     round(col("percentiles")[1], 6).alias("25-perc"),
                     round(col("percentiles")[2], 6).alias("50-perc"),
                     round(col("percentiles")[3], 6).alias("75-perc"),
                     round(col("percentiles")[4], 6).alias("max")
                 )

    stats_it_carbon = calc_percentiles(hourly_avg, "carbon-intensity", "IT")
    stats_it_cfe = calc_percentiles(hourly_avg, "cfe", "IT")
    stats_se_carbon = calc_percentiles(hourly_avg, "carbon-intensity", "SE")
    stats_se_cfe = calc_percentiles(hourly_avg, "cfe", "SE")

    final_stats = stats_it_carbon.union(stats_it_cfe).union(stats_se_carbon).union(stats_se_cfe) \
                                 .orderBy("country", "data")

    # Esportazione percentili
    output_path_stats = f"/output/q3_df_stats_{timestamp}"
    final_stats.write.option("header", "true") \
        .csv(f"hdfs://namenode:9000{output_path_stats}")

    spark.stop()


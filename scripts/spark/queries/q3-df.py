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

    # Preprocessing comune: aggiungo hour, country e rinomino colonne
    def preprocess(df, country_code):
        return df.withColumn("datetime", col("datetime").cast("timestamp")) \
                 .withColumn("hour", hour("datetime")) \
                 .withColumn("carbon", col("carbon_intensity_direct").cast("double")) \
                 .withColumn("cfe", col("cfe_percent").cast("double")) \
                 .withColumn("country", lit(country_code)) \
                 .select("hour", "country", "carbon", "cfe")

    df_it = preprocess(df_it, "IT")
    df_se = preprocess(df_se, "SE")

    # Unione dati IT + SE
    df_all = df_it.union(df_se)

    # Calcolo media aggregata per country e hour
    hourly_avg = df_all.groupBy("country", "hour").agg(
        round(avg("carbon"), 6).alias("carbon-intensity"),
        round(avg("cfe"), 6).alias("cfe")
    ).cache()

    # Calcolo percentili esatti su valori medi orari usando expr("percentile(...)")
    def calc_percentiles(df, metric):
        return df.groupBy("country").agg(
            expr(f"percentile(`{metric}`, array(0.0, 0.25, 0.5, 0.75, 1.0))").alias("percentiles")
        ).withColumn("data", lit(metric)) \
        .select(
            "country",
            "data",
            round(col("percentiles")[0], 6).alias("min"),
            round(col("percentiles")[1], 6).alias("25-perc"),
            round(col("percentiles")[2], 6).alias("50-perc"),
            round(col("percentiles")[3], 6).alias("75-perc"),
            round(col("percentiles")[4], 6).alias("max")
        )

    stats_carbon = calc_percentiles(hourly_avg, "carbon-intensity")
    stats_cfe = calc_percentiles(hourly_avg, "cfe")

    final_stats = stats_carbon.union(stats_cfe).orderBy("country", "data")

    # Scrittura CSV percentili finali
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    output_path_stats = f"/output/q3_df_stats_{timestamp}"
    final_stats.write.option("header", "true") \
        .csv(f"hdfs://namenode:9000{output_path_stats}")

    # Scrittura CSV orari medi, ordinati per country e hour (IT 0-23 poi SE 0-23)
    output_path_hourly = f"/output/q3_df_hourly_{timestamp}"
    hourly_avg.select("hour", "country", "carbon-intensity", "cfe") \
        .orderBy("country", "hour") \
        .write.option("header", "true") \
        .csv(f"hdfs://namenode:9000{output_path_hourly}")

    spark.stop()


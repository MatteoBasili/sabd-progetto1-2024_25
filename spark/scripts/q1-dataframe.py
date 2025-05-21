from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, min, max, avg, round

if __name__ == "__main__":
    spark = SparkSession.builder.appName("ElectricityStatsQ1DF").getOrCreate()

    # Percorsi HDFS (modifica se necessario)
    path_it = "hdfs://namenode:9000/data/electricity/italy_2021_2024_clean.csv"
    path_se = "hdfs://namenode:9000/data/electricity/sweden_2021_2024_clean.csv"

    # Carica i CSV
    df_it = spark.read.option("header", True).csv(path_it)
    df_se = spark.read.option("header", True).csv(path_se)

    # Unisci i due DataFrame
    df = df_it.union(df_se)

    # Casting dei tipi
    df = df.withColumn("carbon_intensity_direct", col("carbon_intensity_direct").cast("float")) \
           .withColumn("cfe_percent", col("cfe_percent").cast("float")) \
           .withColumn("datetime", col("datetime").cast("timestamp"))

    # Estrai anno
    df = df.withColumn("year", year(col("datetime")))

    # GroupBy year e country_code, aggrega media, min, max per carbon e cfe
    agg_df = df.groupBy("year", "country_code") \
        .agg(
            round(avg("carbon_intensity_direct"), 6).alias("carbon_mean"),
            round(min("carbon_intensity_direct"), 2).alias("carbon_min"),
            round(max("carbon_intensity_direct"), 2).alias("carbon_max"),
            round(avg("cfe_percent"), 6).alias("cfe_mean"),
            round(min("cfe_percent"), 2).alias("cfe_min"),
            round(max("cfe_percent"), 2).alias("cfe_max"),
        ) \
        .orderBy("country_code", "year")

    # Salva in CSV (header incluso)
    agg_df.select(
        col("year").alias("date"),
        col("country_code").alias("country"),
        "carbon_mean", "carbon_min", "carbon_max",
        "cfe_mean", "cfe_min", "cfe_max"
    ).write.mode("overwrite").option("header", True).csv("hdfs://namenode:9000/output/energy_stats_df")

    spark.stop()


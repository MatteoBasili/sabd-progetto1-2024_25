from pyspark.sql import SparkSession
from pyspark.sql.functions import year, col, min as min_, max as max_, avg, round
from datetime import datetime

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ElectricityStatsQ1DF_Parquet") \
        .getOrCreate()

    hdfs_path_it = "hdfs://namenode:9000/data/electricity/italy_2021_2024_clean.parquet"
    hdfs_path_se = "hdfs://namenode:9000/data/electricity/sweden_2021_2024_clean.parquet"

    # Leggi i file Parquet
    df_it = spark.read.parquet(hdfs_path_it)
    df_se = spark.read.parquet(hdfs_path_se)

    # Unione dei DataFrame
    df = df_it.unionByName(df_se)

    # Conversione e normalizzazione colonne
    df = df.withColumn("datetime", col("datetime").cast("timestamp")) \
           .withColumn("carbon", col("carbon_intensity_direct").cast("double")) \
           .withColumn("cfe", col("cfe_percent").cast("double")) \
           .withColumnRenamed("country_code", "country")

    # Estrai l'anno
    df = df.withColumn("year", year("datetime"))

    # Aggrega e calcola statistiche
    result = df.groupBy("year", "country").agg(
        round(avg("carbon"), 6).alias("carbon-mean"),
        round(min_("carbon"), 2).alias("carbon-min"),
        round(max_("carbon"), 2).alias("carbon-max"),
        round(avg("cfe"), 6).alias("cfe-mean"),
        round(min_("cfe"), 2).alias("cfe-min"),
        round(max_("cfe"), 2).alias("cfe-max")
    ).orderBy("country", "year")

    # Salva il risultato in HDFS come CSV
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    output_path = f"/output/q1_df_{timestamp}"
    result.write.option("header", "true").csv(f"hdfs://namenode:9000{output_path}")

    spark.stop()


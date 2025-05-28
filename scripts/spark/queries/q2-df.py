from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round, lit, date_format, trunc
from datetime import datetime

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ElectricityStatsQ2_Full") \
        .getOrCreate()

    # Dataset Italia
    hdfs_path_it = "hdfs://namenode:9000/data/electricity/italy_2021_2024_clean.parquet"
    df = spark.read.parquet(hdfs_path_it)

    # Prepara colonne
    df = df.withColumn("datetime", col("datetime").cast("timestamp")) \
           .withColumn("carbon-intensity", col("carbon_intensity_direct").cast("double")) \
           .withColumn("cfe", col("cfe_percent").cast("double")) \
           .withColumn("month_start", trunc("datetime", "month")) \
           .withColumn("date", date_format(col("month_start"), "yyyy-MM"))

    # Aggregazione mensile
    monthly_df = df.groupBy("date").agg(
        round(avg("carbon-intensity"), 6).alias("carbon-intensity"),
        round(avg("cfe"), 6).alias("cfe")
    )

    # ---- Parte 1: Salva tutti i dati mensili (per grafici) ----
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    output_path_all = f"/output/q2_df_all_{timestamp}"
    monthly_df.orderBy("date").coalesce(1).write.option("header", "true") \
        .csv(f"hdfs://namenode:9000{output_path_all}")

    # ---- Parte 2: Top 5 classifiche ----
    top_carbon_desc = monthly_df.orderBy(col("carbon-intensity").desc()).limit(5) \
        .withColumn("order_type", lit("carbon_desc"))
    top_carbon_asc = monthly_df.orderBy(col("carbon-intensity").asc()).limit(5) \
        .withColumn("order_type", lit("carbon_asc"))
    top_cfe_desc = monthly_df.orderBy(col("cfe").desc()).limit(5) \
        .withColumn("order_type", lit("cfe_desc"))
    top_cfe_asc = monthly_df.orderBy(col("cfe").asc()).limit(5) \
        .withColumn("order_type", lit("cfe_asc"))

    final_top_df = top_carbon_desc.union(top_carbon_asc).union(top_cfe_desc).union(top_cfe_asc)

    output_path_top = f"/output/q2_df_top_{timestamp}"
    final_top_df.coalesce(1).write.option("header", "true") \
        .csv(f"hdfs://namenode:9000{output_path_top}")

    spark.stop()


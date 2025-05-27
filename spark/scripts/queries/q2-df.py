from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, avg, round, concat_ws, lit
from datetime import datetime

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ElectricityStatsQ2DF") \
        .getOrCreate()

    # Solo dataset Italia
    hdfs_path_it = "hdfs://namenode:9000/data/electricity/italy_2021_2024_clean.parquet"
    df = spark.read.parquet(hdfs_path_it)

    # Conversione e normalizzazione colonne
    df = df.withColumn("datetime", col("datetime").cast("timestamp")) \
           .withColumn("carbon-intensity", col("carbon_intensity_direct").cast("double")) \
           .withColumn("cfe", col("cfe_percent").cast("double"))

    # Estrazione anno e mese
    df = df.withColumn("year", year("datetime")) \
           .withColumn("month", month("datetime"))

    # Aggregazione per (anno, mese)
    agg_df = df.groupBy("year", "month").agg(
        round(avg("carbon-intensity"), 6).alias("carbon-intensity"),
        round(avg("cfe"), 6).alias("cfe")
    )

    # Colonna combinata "date" nel formato "YYYY_M"
    agg_df = agg_df.withColumn("date", concat_ws("_", col("year"), col("month"))) \
                   .select("date", "carbon-intensity", "cfe")

    # Top 5 ordinamenti richiesti
    top_carbon_desc = agg_df.orderBy(col("carbon-intensity").desc()).limit(5)
    top_carbon_asc = agg_df.orderBy(col("carbon-intensity").asc()).limit(5)
    top_cfe_desc = agg_df.orderBy(col("cfe").desc()).limit(5)
    top_cfe_asc = agg_df.orderBy(col("cfe").asc()).limit(5)

    # Aggiungi colonna "order_type" per distinguere i blocchi
    top_carbon_desc = top_carbon_desc.withColumn("order_type", lit("carbon_desc"))
    top_carbon_asc = top_carbon_asc.withColumn("order_type", lit("carbon_asc"))
    top_cfe_desc = top_cfe_desc.withColumn("order_type", lit("cfe_desc"))
    top_cfe_asc = top_cfe_asc.withColumn("order_type", lit("cfe_asc"))

    # Unione dei risultati
    final_df = top_carbon_desc.union(top_carbon_asc) \
                              .union(top_cfe_desc) \
                              .union(top_cfe_asc)

    # Salva il risultato in HDFS come CSV
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    output_path = f"/output/q2_df_{timestamp}"
    final_df.coalesce(1) \
        .write \
        .option("header", "true") \
        .csv(f"hdfs://namenode:9000{output_path}")

    spark.stop()


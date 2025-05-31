from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round, lit, date_format, trunc, row_number
from pyspark.sql.window import Window
from datetime import datetime

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ElectricityStatsQ2DF_Parquet") \
        .getOrCreate()

    # Path dataset Italia
    hdfs_path_it = "hdfs://namenode:9000/data/electricity/italy_2021_2024_clean.parquet"
    df = spark.read.parquet(hdfs_path_it)

    # Prepara colonne e parsing data
    df = df.withColumn("datetime", col("datetime").cast("timestamp")) \
           .withColumn("carbon-intensity", col("carbon_intensity_direct").cast("double")) \
           .withColumn("cfe", col("cfe_percent").cast("double")) \
           .withColumn("month_start", trunc("datetime", "month")) \
           .withColumn("date", date_format(col("month_start"), "yyyy-MM"))

    # Aggregazione mensile
    monthly_df = df.groupBy("date").agg(
        round(avg("carbon-intensity"), 6).alias("carbon-intensity"),
        round(avg("cfe"), 6).alias("cfe")
    ).cache()

    # ---- Parte 1: Salva CSV completo per grafici ----
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    output_path_all = f"/output/q2_df_all_{timestamp}"
    monthly_df.orderBy("date").write.option("header", "true") \
        .csv(f"hdfs://namenode:9000{output_path_all}")  # NO coalesce(1)

    # ---- Parte 2: Generazione classifiche top-5 ----
    def rank_top(df, order_col, ascending, label, priority):
        window = Window.orderBy(col(order_col).asc() if ascending else col(order_col).desc())
        return df.withColumn("order_type", lit(label)) \
                 .withColumn("order_priority", lit(priority)) \
                 .withColumn("rank", row_number().over(window)) \
                 .filter(col("rank") <= 5)

    top_carbon_desc = rank_top(monthly_df, "carbon-intensity", ascending=False, label="carbon_desc", priority=1)
    top_carbon_asc  = rank_top(monthly_df, "carbon-intensity", ascending=True,  label="carbon_asc",  priority=2)
    top_cfe_desc    = rank_top(monthly_df, "cfe",               ascending=False, label="cfe_desc",    priority=3)
    top_cfe_asc     = rank_top(monthly_df, "cfe",               ascending=True,  label="cfe_asc",     priority=4)

    # Unione e ordinamento secondo ordine personalizzato
    final_top_df = top_carbon_desc.union(top_carbon_asc).union(top_cfe_desc).union(top_cfe_asc) \
        .select("date", "carbon-intensity", "cfe", "order_type", "rank", "order_priority") \
        .orderBy("order_priority", "rank") \
        .drop("order_priority")

    # Scrittura classifiche
    output_path_top = f"/output/q2_df_top_{timestamp}"
    final_top_df.write.option("header", "true") \
        .csv(f"hdfs://namenode:9000{output_path_top}")  # NO coalesce(1)

    spark.stop()


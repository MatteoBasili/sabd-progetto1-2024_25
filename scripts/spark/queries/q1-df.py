from pyspark.sql import SparkSession
from pyspark.sql.functions import year, col, min as min_, max as max_, avg, round
from datetime import datetime

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ElectricityStatsQ1DF_Parquet") \
        .getOrCreate()

    hdfs_path_it = "hdfs://namenode:9000/data/electricity/italy_2021_2024_clean.parquet"
    hdfs_path_se = "hdfs://namenode:9000/data/electricity/sweden_2021_2024_clean.parquet"

    df = spark.read.parquet(hdfs_path_it).unionByName(spark.read.parquet(hdfs_path_se))

    df = df.withColumn("datetime", col("datetime").cast("timestamp")) \
           .withColumn("carbon", col("carbon_intensity_direct").cast("double")) \
           .withColumn("cfe", col("cfe_percent").cast("double")) \
           .withColumn("year", year("datetime")) \
           .withColumnRenamed("country_code", "country")

    result = df.groupBy("year", "country").agg(
        round(avg("carbon"), 6).alias("carbon-mean"),
        round(min_("carbon"), 2).alias("carbon-min"),
        round(max_("carbon"), 2).alias("carbon-max"),
        round(avg("cfe"), 6).alias("cfe-mean"),
        round(min_("cfe"), 2).alias("cfe-min"),
        round(max_("cfe"), 2).alias("cfe-max")
    ).orderBy("country", "year")

    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    output_path = f"/output/q1_df_{timestamp}"
    result.write.option("header", "true").csv(f"hdfs://namenode:9000{output_path}")

    spark.stop()


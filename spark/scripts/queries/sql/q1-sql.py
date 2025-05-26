from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ElectricityStatsQ1SQL_Parquet") \
        .getOrCreate()

    hdfs_path_it = "hdfs://namenode:9000/data/electricity/italy_2021_2024_clean.parquet"
    hdfs_path_se = "hdfs://namenode:9000/data/electricity/sweden_2021_2024_clean.parquet"

    # Leggi i file Parquet
    df_it = spark.read.parquet(hdfs_path_it)
    df_se = spark.read.parquet(hdfs_path_se)

    # Unione dei DataFrame
    df = df_it.unionByName(df_se)

    # Prepara i dati: cast e rinomina
    df = df.selectExpr(
        "CAST(datetime AS TIMESTAMP) AS datetime",
        "CAST(carbon_intensity_direct AS DOUBLE) AS carbon",
        "CAST(cfe_percent AS DOUBLE) AS cfe",
        "country_code AS country"
    )

    # Registra la view temporanea
    df.createOrReplaceTempView("electricity")

    # Query SQL
    query = """
        SELECT
            YEAR(datetime) AS year,
            country,
            ROUND(AVG(carbon), 6) AS `carbon-mean`,
            ROUND(MIN(carbon), 2) AS `carbon-min`,
            ROUND(MAX(carbon), 2) AS `carbon-max`,
            ROUND(AVG(cfe), 6) AS `cfe-mean`,
            ROUND(MIN(cfe), 2) AS `cfe-min`,
            ROUND(MAX(cfe), 2) AS `cfe-max`
        FROM electricity
        GROUP BY year, country
        ORDER BY country, year
    """

    result = spark.sql(query)

    # Scrivi in HDFS come CSV
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    output_path = f"/output/q1_sql_{timestamp}"
    result.write.option("header", "true").csv(f"hdfs://namenode:9000{output_path}")

    spark.stop()


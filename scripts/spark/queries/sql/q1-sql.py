from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ElectricityStatsQ1SQL_Parquet") \
        .getOrCreate()

    hdfs_path_it = "hdfs://namenode:9000/data/electricity/italy_2021_2024_clean.parquet"
    hdfs_path_se = "hdfs://namenode:9000/data/electricity/sweden_2021_2024_clean.parquet"

    # Leggi e unisci i DataFrame
    df = spark.read.parquet(hdfs_path_it).unionByName(spark.read.parquet(hdfs_path_se))

    # Crea una vista temporanea SQL con le colonne necessarie
    df.createOrReplaceTempView("energy_data")

    # Scrivi la query SQL
    query = """
        SELECT
            YEAR(CAST(datetime AS timestamp)) AS year,
            country_code AS country,
            ROUND(AVG(CAST(carbon_intensity_direct AS double)), 6) AS `carbon-mean`,
            ROUND(MIN(CAST(carbon_intensity_direct AS double)), 2) AS `carbon-min`,
            ROUND(MAX(CAST(carbon_intensity_direct AS double)), 2) AS `carbon-max`,
            ROUND(AVG(CAST(cfe_percent AS double)), 6) AS `cfe-mean`,
            ROUND(MIN(CAST(cfe_percent AS double)), 2) AS `cfe-min`,
            ROUND(MAX(CAST(cfe_percent AS double)), 2) AS `cfe-max`
        FROM energy_data
        GROUP BY YEAR(CAST(datetime AS timestamp)), country_code
        ORDER BY country, year
    """

    # Esegui la query SQL
    result = spark.sql(query)

    # Salva l'output in CSV
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    output_path = f"/output/q1_sql_{timestamp}"
    result.write.option("header", "true").csv(f"hdfs://namenode:9000{output_path}")

    spark.stop()


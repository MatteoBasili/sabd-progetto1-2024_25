from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ElectricityStatsQ3SQL_Parquet") \
        .getOrCreate()

    # Percorsi HDFS
    hdfs_path_it = "hdfs://namenode:9000/data/electricity/italy_2021_2024_clean.parquet"
    hdfs_path_se = "hdfs://namenode:9000/data/electricity/sweden_2021_2024_clean.parquet"

    # Caricamento dati
    df_it = spark.read.parquet(hdfs_path_it)
    df_se = spark.read.parquet(hdfs_path_se)

    # Registrazione come viste temporanee
    df_it.createOrReplaceTempView("raw_it")
    df_se.createOrReplaceTempView("raw_se")

    # Preprocessing + creazione viste preprocessate
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW data_it AS
        SELECT 
            HOUR(CAST(datetime AS TIMESTAMP)) AS hour,
            'IT' AS country,
            CAST(carbon_intensity_direct AS DOUBLE) AS carbon,
            CAST(cfe_percent AS DOUBLE) AS cfe
        FROM raw_it
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW data_se AS
        SELECT 
            HOUR(CAST(datetime AS TIMESTAMP)) AS hour,
            'SE' AS country,
            CAST(carbon_intensity_direct AS DOUBLE) AS carbon,
            CAST(cfe_percent AS DOUBLE) AS cfe
        FROM raw_se
    """)

    # Unione dei due dataset preprocessati
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW union_data AS
        SELECT * FROM data_it
        UNION ALL
        SELECT * FROM data_se
    """)

    # ✅ Calcolo media oraria e caching via SQL
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW hourly_avg AS
        SELECT
            hour,
            country,
            ROUND(AVG(carbon), 6) AS `carbon-intensity`,
            ROUND(AVG(cfe), 6) AS cfe
        FROM union_data
        GROUP BY hour, country
    """)

    # ✅ Caching
    spark.catalog.cacheTable("hourly_avg")
    spark.table("hourly_avg").count()  # trigger cache

    # Esportazione dei dati medi orari
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    output_path_hourly = f"/output/q3_sql_hourly_{timestamp}"

    spark.sql("""
        SELECT hour, country, `carbon-intensity`, cfe
        FROM hourly_avg
        ORDER BY country, hour
    """).write.option("header", "true") \
      .csv(f"hdfs://namenode:9000{output_path_hourly}")

    # Calcolo percentili per ciascun paese e metrica
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW percentiles_it_carbon AS
        SELECT 
            'IT' AS country,
            'carbon-intensity' AS data,
            ROUND(percentile(`carbon-intensity`, array(0.0, 0.25, 0.5, 0.75, 1.0))[0], 6) AS min,
            ROUND(percentile(`carbon-intensity`, array(0.0, 0.25, 0.5, 0.75, 1.0))[1], 6) AS `25-perc`,
            ROUND(percentile(`carbon-intensity`, array(0.0, 0.25, 0.5, 0.75, 1.0))[2], 6) AS `50-perc`,
            ROUND(percentile(`carbon-intensity`, array(0.0, 0.25, 0.5, 0.75, 1.0))[3], 6) AS `75-perc`,
            ROUND(percentile(`carbon-intensity`, array(0.0, 0.25, 0.5, 0.75, 1.0))[4], 6) AS max
        FROM hourly_avg
        WHERE country = 'IT'
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW percentiles_it_cfe AS
        SELECT 
            'IT' AS country,
            'cfe' AS data,
            ROUND(percentile(cfe, array(0.0, 0.25, 0.5, 0.75, 1.0))[0], 6) AS min,
            ROUND(percentile(cfe, array(0.0, 0.25, 0.5, 0.75, 1.0))[1], 6) AS `25-perc`,
            ROUND(percentile(cfe, array(0.0, 0.25, 0.5, 0.75, 1.0))[2], 6) AS `50-perc`,
            ROUND(percentile(cfe, array(0.0, 0.25, 0.5, 0.75, 1.0))[3], 6) AS `75-perc`,
            ROUND(percentile(cfe, array(0.0, 0.25, 0.5, 0.75, 1.0))[4], 6) AS max
        FROM hourly_avg
        WHERE country = 'IT'
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW percentiles_se_carbon AS
        SELECT 
            'SE' AS country,
            'carbon-intensity' AS data,
            ROUND(percentile(`carbon-intensity`, array(0.0, 0.25, 0.5, 0.75, 1.0))[0], 6) AS min,
            ROUND(percentile(`carbon-intensity`, array(0.0, 0.25, 0.5, 0.75, 1.0))[1], 6) AS `25-perc`,
            ROUND(percentile(`carbon-intensity`, array(0.0, 0.25, 0.5, 0.75, 1.0))[2], 6) AS `50-perc`,
            ROUND(percentile(`carbon-intensity`, array(0.0, 0.25, 0.5, 0.75, 1.0))[3], 6) AS `75-perc`,
            ROUND(percentile(`carbon-intensity`, array(0.0, 0.25, 0.5, 0.75, 1.0))[4], 6) AS max
        FROM hourly_avg
        WHERE country = 'SE'
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW percentiles_se_cfe AS
        SELECT 
            'SE' AS country,
            'cfe' AS data,
            ROUND(percentile(cfe, array(0.0, 0.25, 0.5, 0.75, 1.0))[0], 6) AS min,
            ROUND(percentile(cfe, array(0.0, 0.25, 0.5, 0.75, 1.0))[1], 6) AS `25-perc`,
            ROUND(percentile(cfe, array(0.0, 0.25, 0.5, 0.75, 1.0))[2], 6) AS `50-perc`,
            ROUND(percentile(cfe, array(0.0, 0.25, 0.5, 0.75, 1.0))[3], 6) AS `75-perc`,
            ROUND(percentile(cfe, array(0.0, 0.25, 0.5, 0.75, 1.0))[4], 6) AS max
        FROM hourly_avg
        WHERE country = 'SE'
    """)

    # Esportazione finale dei percentili
    spark.sql("""
        SELECT * FROM percentiles_it_carbon
        UNION ALL
        SELECT * FROM percentiles_it_cfe
        UNION ALL
        SELECT * FROM percentiles_se_carbon
        UNION ALL
        SELECT * FROM percentiles_se_cfe
        ORDER BY country, data
    """).write.option("header", "true") \
        .csv(f"hdfs://namenode:9000/output/q3_sql_stats_{timestamp}")

    # ❌ Rimozione del caching (facoltativa ma buona pratica)
    spark.catalog.uncacheTable("hourly_avg")

    spark.stop()


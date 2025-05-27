from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ElectricityStatsQ2SQL_CSV") \
        .getOrCreate()

    # Carica solo il dataset Italia
    hdfs_path_it = "hdfs://namenode:9000/data/electricity/italy_2021_2024_clean.parquet"
    df = spark.read.parquet(hdfs_path_it)

    # Prepara i dati
    df = df.selectExpr(
        "CAST(datetime AS TIMESTAMP) AS datetime",
        "CAST(carbon_intensity_direct AS DOUBLE) AS carbon",
        "CAST(cfe_percent AS DOUBLE) AS cfe"
    )

    df.createOrReplaceTempView("electricity")

    # Query SQL per aggregazione mensile e classifiche
    query = """
        WITH monthly_agg AS (
            SELECT
                YEAR(datetime) AS year,
                MONTH(datetime) AS month,
                ROUND(AVG(carbon), 6) AS `carbon-intensity`,
                ROUND(AVG(cfe), 6) AS cfe,
                CONCAT(YEAR(datetime), '_', MONTH(datetime)) AS date
            FROM electricity
            GROUP BY YEAR(datetime), MONTH(datetime)
        ),
        carbon_desc AS (
            SELECT date, `carbon-intensity`, cfe, 'carbon_desc' AS order_type
            FROM monthly_agg
            ORDER BY `carbon-intensity` DESC
            LIMIT 5
        ),
        carbon_asc AS (
            SELECT date, `carbon-intensity`, cfe, 'carbon_asc' AS order_type
            FROM monthly_agg
            ORDER BY `carbon-intensity` ASC
            LIMIT 5
        ),
        cfe_desc AS (
            SELECT date, `carbon-intensity`, cfe, 'cfe_desc' AS order_type
            FROM monthly_agg
            ORDER BY cfe DESC
            LIMIT 5
        ),
        cfe_asc AS (
            SELECT date, `carbon-intensity`, cfe, 'cfe_asc' AS order_type
            FROM monthly_agg
            ORDER BY cfe ASC
            LIMIT 5
        )
        SELECT * FROM carbon_desc
        UNION ALL
        SELECT * FROM carbon_asc
        UNION ALL
        SELECT * FROM cfe_desc
        UNION ALL
        SELECT * FROM cfe_asc
    """

    result = spark.sql(query)

    # Salva l'output in un solo file CSV con header una sola volta
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    output_path = f"/output/q2_sql_{timestamp}"
    result.coalesce(1).write.option("header", "true").csv(f"hdfs://namenode:9000{output_path}")

    spark.stop()


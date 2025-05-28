from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ElectricityStatsQ2_SQL_FULL") \
        .getOrCreate()

    # Carica il dataset Italia
    hdfs_path_it = "hdfs://namenode:9000/data/electricity/italy_2021_2024_clean.parquet"
    df = spark.read.parquet(hdfs_path_it)

    # Prepara la tabella temporanea
    df.selectExpr(
        "CAST(datetime AS TIMESTAMP) AS datetime",
        "CAST(carbon_intensity_direct AS DOUBLE) AS carbon",
        "CAST(cfe_percent AS DOUBLE) AS cfe"
    ).createOrReplaceTempView("electricity")

    # ------------------------------
    # 1. Calcolo dei valori mensili
    # ------------------------------
    monthly_query = """
        SELECT
            DATE_FORMAT(DATE_TRUNC('month', datetime), 'yyyy-MM') AS date,
            ROUND(AVG(carbon), 6) AS `carbon-intensity`,
            ROUND(AVG(cfe), 6) AS cfe
        FROM electricity
        GROUP BY DATE_FORMAT(DATE_TRUNC('month', datetime), 'yyyy-MM')
        ORDER BY date
    """
    monthly_df = spark.sql(monthly_query)

    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    output_path_all = f"/output/q2_sql_all_{timestamp}"

    monthly_df.coalesce(1).write.option("header", "true") \
        .csv(f"hdfs://namenode:9000{output_path_all}")

    # ------------------------------
    # 2. Calcolo delle classifiche
    # ------------------------------
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW monthly_agg AS
        SELECT
            DATE_FORMAT(DATE_TRUNC('month', datetime), 'yyyy-MM') AS date,
            ROUND(AVG(carbon), 6) AS `carbon-intensity`,
            ROUND(AVG(cfe), 6) AS cfe
        FROM electricity
        GROUP BY DATE_FORMAT(DATE_TRUNC('month', datetime), 'yyyy-MM')
    """)

    top_query = """
        WITH
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

    top_df = spark.sql(top_query)

    output_path_top = f"/output/q2_sql_top_{timestamp}"
    top_df.coalesce(1).write.option("header", "true") \
        .csv(f"hdfs://namenode:9000{output_path_top}")

    spark.stop()


from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from datetime import datetime

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ElectricityStatsQ2SQL_Parquet") \
        .getOrCreate()

    hdfs_path = "hdfs://namenode:9000/data/electricity/italy_2021_2024_clean.parquet"
    
    # Leggi parquet e crea vista temporanea
    spark.read.parquet(hdfs_path).createOrReplaceTempView("energy_data")

    # Caching utile perché useremo la stessa tabella in più query
    spark.catalog.cacheTable("energy_data")

    # 1) Aggregazione mensile: carbon-intensity e cfe medi per mese (yyyy-MM)
    monthly_query = """
        SELECT
            date_format(trunc(cast(datetime as timestamp), 'month'), 'yyyy-MM') as date,
            ROUND(AVG(CAST(carbon_intensity_direct AS double)), 6) AS `carbon-intensity`,
            ROUND(AVG(CAST(cfe_percent AS double)), 6) AS cfe
        FROM energy_data
        GROUP BY date_format(trunc(cast(datetime as timestamp), 'month'), 'yyyy-MM')
    """
    spark.sql(monthly_query).createOrReplaceTempView("monthly_agg")

    # 2) Per le classifiche top-5: definisco query per i 4 ranking
    ranking_query_template = """
        SELECT date, `carbon-intensity`, cfe, order_type, rank
        FROM (
            SELECT
                date,
                `carbon-intensity`,
                cfe,
                '{order_type}' AS order_type,
                ROW_NUMBER() OVER (ORDER BY {order_col} {order_dir}) AS rank
            FROM monthly_agg
        ) WHERE rank <= 5
    """

    # Costruisco le 4 classifiche
    top_carbon_desc = spark.sql(ranking_query_template.format(order_type='carbon_desc', order_col='`carbon-intensity`', order_dir='DESC'))
    top_carbon_asc  = spark.sql(ranking_query_template.format(order_type='carbon_asc',  order_col='`carbon-intensity`', order_dir='ASC'))
    top_cfe_desc    = spark.sql(ranking_query_template.format(order_type='cfe_desc',    order_col='cfe', order_dir='DESC'))
    top_cfe_asc     = spark.sql(ranking_query_template.format(order_type='cfe_asc',     order_col='cfe', order_dir='ASC'))

    # Unisco le classifiche
    top_all = top_carbon_desc.union(top_carbon_asc).union(top_cfe_desc).union(top_cfe_asc)

    # Aggiungo colonna per ordinamento personalizzato
    top_all = top_all.withColumn(
        "order_priority",
        when(col("order_type") == "carbon_desc", 1)
        .when(col("order_type") == "carbon_asc", 2)
        .when(col("order_type") == "cfe_desc", 3)
        .when(col("order_type") == "cfe_asc", 4)
        .otherwise(5)
    )

    # Scrittura dati aggregati completi per grafici
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    output_path_all = f"/output/q2_sql_all_{timestamp}"
    spark.sql("SELECT * FROM monthly_agg ORDER BY date") \
        .write.option("header", "true").csv(f"hdfs://namenode:9000{output_path_all}")

    # Scrittura classifiche top5 con ordinamento personalizzato e senza colonna di supporto
    output_path_top = f"/output/q2_sql_top_{timestamp}"
    top_all.orderBy("order_priority", "rank").drop("order_priority") \
        .write.option("header", "true").csv(f"hdfs://namenode:9000{output_path_top}")

    spark.catalog.uncacheTable("energy_data")
    spark.stop()


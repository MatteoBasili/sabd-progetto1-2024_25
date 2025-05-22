from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadFromHDFS") \
    .getOrCreate()

df = spark.read.csv("hdfs://namenode:9000/data/electricity", header=True)
df.show()


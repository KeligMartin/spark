from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, count, filter

spark = SparkSession \
    .builder \
    .appName("TP3") \
    .master("local[*]") \
    .getOrCreate()

file = "full.csv"

file_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file)

file_df.select("commit", "repo").groupBy("repo").agg(count("commit").alias("Total")).orderBy("Total", ascending=False).show(n=10, truncate=False)

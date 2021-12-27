from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, count, filter

spark = SparkSession \
    .builder \
    .appName("TP3") \
    .master("local[*]") \
    .getOrCreate()

file = "full.csv"

file_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file)

file_df.na.drop("any").select("repo").show(n=100, truncate=False)

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc, explode, split, col, current_date, to_date, months_between

spark = SparkSession \
    .builder \
    .appName("Spark-chan UwU") \
    .master("local[*]") \
    .getOrCreate()

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

file = "full.csv"

file_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file)

print('Afficher dans la console les 10 projets Github pour lesquels il y a eu le plus de commit.\n')

file_df \
    .na.drop("any") \
    .select("commit", "repo") \
    .groupBy("repo") \
    .agg(count("commit").alias("Total")) \
    .orderBy("Total", ascending=False) \
    .show(n=10, truncate=False)

print('Afficher dans la console le plus gros contributeur (la personne qui a fait le plus de commit) du projet apache/spark.\n')

file_df \
    .na.drop("any") \
    .filter(file_df.repo == 'apache/spark') \
    .select("author") \
    .groupBy("author") \
    .agg(count("author").alias("Commits")) \
    .orderBy(desc("Commits")) \
    .show(1, False)

print('Afficher dans la console les plus gros contributeurs du projet apache/spark sur les 24derniers mois')

file_df \
    .select(col("author"), col("commit"), col("date"), to_date(col("date"), "EEE LLL dd hh:mm:ss yyyy Z").alias("to_date")) \
    .where(col("repo") == "apache/spark") \
    .where(months_between(current_date(), col("to_date")) <= 24) \
    .groupBy(col("author")) \
    .agg(count(col("author")).alias("contribution")) \
    .orderBy("contribution") \
    .show(n=10, truncate=False)

print('Afficher dans la console les 10 mots qui reviennent le plus dans les messages de commit sur l’ensemble des projets.')

stop_words_path = "englishST.txt"
stop_words_file = open(stop_words_path, "r")
list_of_words = []

for line in stop_words_file:
    stripped_line = line.strip()
    line_list = stripped_line.split()
    list_of_words.append(stripped_line)

stop_words_file.close()

file_df.withColumn('word', explode(split(col('message'), ' '))) \
    .filter(col('word').isin(*list_of_words) != True) \
    .filter(col('word') != '') \
    .groupBy('word') \
    .count() \
    .sort('count', ascending=False) \
    .show(10, False)


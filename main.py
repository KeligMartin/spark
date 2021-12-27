from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, count, filter, desc, explode, split, col

spark = SparkSession \
    .builder \
    .appName("TP3") \
    .master("local[*]") \
    .getOrCreate()

# file = "full.csv"
file = "data/full.csv"

file_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file)

# print('Afficher dans la console les 10 projets Github pour lesquels il y a eu le plus de commit.\n')

# file_df.na.drop("any").select("commit", "repo").groupBy("repo").agg(count("commit").alias("Total")).orderBy("Total", ascending=False).show(n=10, truncate=False)

# print('Afficher dans la console le plus gros contributeur (la personne qui a fait le plus de commit) du projet apache/spark.\n')

# file_df\
#     .na.drop("any")\
#     .filter(file_df.repo == 'apache/spark')\
#     .select("author")\
#     .groupBy("author")\
#     .agg(count("author").alias("Commits"))\
#     .orderBy(desc("Commits"))\
#     .show(1, False)

print('Afficher dans la console les 10 mots qui reviennent le plus dans les messages de commit sur l’ensemble des projets.')

stop_words_path = "data/englishST.txt"
stop_words_file = open(stop_words_path, "r")
list_of_words = []

for line in stop_words_file:
  stripped_line = line.strip()
  # line_list = stripped_line.split()
  list_of_words.append(stripped_line)

stop_words_file.close()

print(f'List of stopwords {list_of_words}')

file_df.withColumn('word', explode(split(col('message'), ' '))) \
    .filter(col('word').isin(*list_of_words) == False)\
    .filter(col('word') != '')\
    .groupBy('word') \
    .count() \
    .sort('count', ascending=False) \
    .show(10, False)
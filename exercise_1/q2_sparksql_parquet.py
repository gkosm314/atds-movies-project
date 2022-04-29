from pyspark.sql import SparkSession
import time

#Initialize SparkSession and SparkContext
spark = SparkSession.builder.appName("Advanced_Databases_Project").getOrCreate()

#Define path to parquet files
ratings_parquet_file = "hdfs://master:9000/files/ratings.parquet"

start_time = time.time()

#Read parquet file
ratings = spark.read.format('parquet').load(ratings_parquet_file)

#Register ratings table
ratings = ratings.withColumnRenamed("_c0","user_id")
ratings = ratings.withColumnRenamed("_c2","rating")
ratings.registerTempTable("ratings")

#Execute query
#StackOverflow link = https://stackoverflow.com/a/8749095
#sql_query = "SELECT COUNT(*) / (SELECT COUNT(*) FROM ratings) FROM ratings GROUP BY user_id HAVING AVG(rating) > 3.0"
sql_query = "SELECT (COUNT(user_id)/(SELECT COUNT(DISTINCT user_id) FROM ratings))*100 AS percentage FROM (SELECT user_id FROM ratings GROUP BY user_id HAVING AVG(rating) > 3)"

query_time = time.time()

res = spark.sql(sql_query)
res.write.csv("hdfs://master:9000/outputs/q2_sparksql_parquet.csv")

end_time = time.time()
with open("query_times.txt", "a") as f:
	f.write("Q2 SparkSQL Parquet: " + str(end_time-start_time) + " seconds\n")
	f.write("Q2 SparkSQL Parquet (without reading): " + str(end_time-query_time) + " seconds\n")

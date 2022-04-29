from pyspark.sql import SparkSession
import time

#Initialize SparkSession and SparkContext
spark = SparkSession.builder.appName("Advanced_Databases_Project").getOrCreate()

#Define path to csv files
movies_csv_file = "hdfs://master:9000/files/movies.csv"

start_time = time.time()

#Read csv file
movies = spark.read.format('csv').options(header='false', inferSchema='true').load(movies_csv_file)

#Register movies table
movies = movies.withColumnRenamed("_c1","movie_title")
movies = movies.withColumnRenamed("_c3","timestamp")
movies = movies.withColumnRenamed("_c5","cost")
movies = movies.withColumnRenamed("_c6","earnings")
movies.registerTempTable("movies")

#Execute query
max_profits_table = "SELECT YEAR(timestamp) AS year, MAX((earnings-cost)/cost) AS max_profit FROM movies WHERE (timestamp IS NOT NULL) AND YEAR(timestamp) >= 2000 AND earnings <> 0 AND cost <> 0 GROUP BY year"
sql_query = "SELECT m2.year, m1.movie_title, (m2.max_profit)*100 FROM movies AS m1 JOIN (" + max_profits_table + ") AS m2 ON (YEAR(m1.timestamp) = m2.year AND ((m1.earnings-m1.cost)/m1.cost) = m2.max_profit) ORDER BY year"

query_time = time.time()

res = spark.sql(sql_query)
res.write.csv("hdfs://master:9000/outputs/q1_sparksql_csv.csv")

end_time = time.time()
with open("query_times.txt", "a") as f:
	f.write("Q1 SparkSQL CSV: " + str(end_time-start_time) + " seconds\n")
	f.write("Q1 SparkSQL CSV (without reading): " + str(end_time-query_time) + " seconds\n")

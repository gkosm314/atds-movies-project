from pyspark.sql import SparkSession
import time

#Initialize SparkSession and SparkContext
spark = SparkSession.builder.appName("Advanced_Databases_Project").getOrCreate()

#Define path to csv files
ratings_csv_file = "hdfs://master:9000/files/ratings.csv"
movie_genres_csv_file = "hdfs://master:9000/files/movie_genres.csv"

start_time = time.time()

#Read csv file
ratings = spark.read.format('csv').options(header='false', inferSchema='true').load(ratings_csv_file)
movie_genres = spark.read.format('csv').options(header='false', inferSchema='true').load(movie_genres_csv_file)

#Register ratings table
ratings = ratings.withColumnRenamed("_c1","movie_id")
ratings = ratings.withColumnRenamed("_c2","rating")
ratings.registerTempTable("ratings")

#Register movie_genres table
movie_genres = movie_genres.withColumnRenamed("_c0","movie_id")
movie_genres = movie_genres.withColumnRenamed("_c1","genre")
movie_genres.registerTempTable("movie_genres")

#Execute query
sql_query = "SELECT mg.genre, AVG(avg_r) as average_rating, COUNT(r.movie_id) FROM (SELECT movie_id,AVG(rating) AS avg_r FROM ratings GROUP BY movie_id) r JOIN movie_genres mg ON r.movie_id = mg.movie_id GROUP BY mg.genre"

query_time = time.time()

res = spark.sql(sql_query)
res.write.csv("hdfs://master:9000/outputs/q3_sparksql_csv.csv")

end_time = time.time()
with open("query_times.txt", "a") as f:
	f.write("Q3 SparkSQL CSV: " + str(end_time-start_time) + " seconds\n")
	f.write("Q3 SparkSQL CSV (without reading): " + str(end_time-query_time) + " seconds\n")

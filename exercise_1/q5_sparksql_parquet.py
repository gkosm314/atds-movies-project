from pyspark.sql import SparkSession
import time

#Initialize SparkSession and SparkContext
spark = SparkSession.builder.appName("Advanced_Databases_Project").getOrCreate()

#Define path to parquet files
movies_parquet_file = "hdfs://master:9000/files/movies.parquet"
movie_genres_parquet_file = "hdfs://master:9000/files/movie_genres.parquet"
ratings_parquet_file = "hdfs://master:9000/files/ratings.parquet"

start_time = time.time()

#Read parquet file
movies = spark.read.format('parquet').load(movies_parquet_file)
movie_genres = spark.read.format('parquet').load(movie_genres_parquet_file)
ratings = spark.read.format('parquet').load(ratings_parquet_file)

#Register movies table
movies = movies.withColumnRenamed("_c0","movie_id")
movies = movies.withColumnRenamed("_c1","movie_title")
movies = movies.withColumnRenamed("_c7","popularity")
movies.registerTempTable("movies")

#Register movie_genres table
movie_genres = movie_genres.withColumnRenamed("_c0","movie_id")
movie_genres = movie_genres.withColumnRenamed("_c1","genre")
movie_genres.registerTempTable("movie_genres")

#Register ratings table
ratings = ratings.withColumnRenamed("_c0","user_id")
ratings = ratings.withColumnRenamed("_c1","movie_id")
ratings = ratings.withColumnRenamed("_c2","rating")
ratings.registerTempTable("ratings")

#Execute query
movies_table = "SELECT movie_id, movie_title, popularity FROM movies"
movie_genres_table = "SELECT movie_id, genre FROM movie_genres"
movie_genres_with_popularity_table = "SELECT m.movie_id, m.movie_title, mg.genre, m.popularity FROM movies AS m JOIN movie_genres AS mg ON m.movie_id = mg.movie_id"
ratings_table = "SELECT m.genre, r.user_id, m.movie_title, r.rating, m.popularity FROM ratings AS r JOIN (" + movie_genres_with_popularity_table + ") AS m ON r.movie_id = m.movie_id"
genre_user_count_table = "SELECT genre, user_id, COUNT(*) AS counter FROM (" + ratings_table + ") GROUP BY genre, user_id"

with_query_1 = "WITH genre_user_count AS (" + genre_user_count_table + ") "
max_genre_user_count_table = "SELECT guc_1.genre,guc_1.user_id,guc_1.counter FROM genre_user_count AS guc_1 WHERE guc_1.counter = (SELECT MAX(guc_2.counter) FROM genre_user_count AS guc_2 WHERE guc_1.genre = guc_2.genre)"
main_table = "SELECT cnt.*,rat.movie_title,rat.rating as movie_rating,rat.popularity FROM (" + max_genre_user_count_table +  ") cnt JOIN (" + ratings_table + ") rat ON (cnt.genre = rat.genre AND cnt.user_id = rat.user_id)"

# join_conditions = "ON (t1.genre = t2.genre) AND (t1.movie_rating < t2.movie_rating OR (t1.movie_rating = t2.movie_rating AND t1.popularity < t2.popularity))"
# max_res_table = " SELECT t1.* FROM (" + main_table + ") t1 LEFT OUTER JOIN (" + main_table + ") t2 " + join_conditions +" WHERE t2.genre IS NULL ORDER BY t1.genre" 

with_query_2 = with_query_1 + ", t AS (" + main_table + ") "

max_join_conditions = "ON (t1.genre = t2.genre) AND (t1.movie_rating < t2.movie_rating OR (t1.movie_rating = t2.movie_rating AND t1.popularity < t2.popularity))"
max_res_table = " SELECT t1.genre, t1.user_id, t1.counter, t1.movie_title, t1.movie_rating FROM t t1 LEFT OUTER JOIN t t2 " + max_join_conditions +" WHERE t2.genre IS NULL" 

min_join_conditions = "ON (t1.genre = t2.genre) AND (t1.movie_rating > t2.movie_rating OR (t1.movie_rating = t2.movie_rating AND t1.popularity < t2.popularity))"
min_res_table = " SELECT t1.genre, t1.user_id, t1.counter, t1.movie_title, t1.movie_rating FROM t t1 LEFT OUTER JOIN t t2 " + min_join_conditions +" WHERE t2.genre IS NULL" 

sql_query = "SELECT max_table.genre, max_table.user_id, max_table.counter, max_table.movie_title as best_movie, max_table.movie_rating as best_rating, min_table.movie_title as worst_movie, min_table.movie_rating as worst_rating FROM (" + max_res_table + ") max_table INNER JOIN (" + min_res_table + ") min_table ON max_table.genre = min_table.genre  ORDER BY genre"

query_time = time.time()

res = spark.sql(with_query_2 + sql_query)
res.write.csv("hdfs://master:9000/outputs/q5_sparksql_parquet.csv")

end_time = time.time()
with open("query_times.txt", "a") as f:
	f.write("Q5 SparkSQL Parquet: " + str(end_time-start_time) + " seconds\n")
	f.write("Q5 SparkSQL Parquet (without reading): " + str(end_time-query_time) + " seconds\n")
from pyspark.sql import SparkSession
import time

#Initialize SparkSession and SparkContext
spark = SparkSession.builder.appName("Advanced_Databases_Project").getOrCreate()

#Define path to csv files
movies_csv_file = "hdfs://master:9000/files/movies.csv"
movie_genres_csv_file = "hdfs://master:9000/files/movie_genres.csv"

#Categorize entry to the correct year range or mark it as invalid
def decade_function(year):
	if year >= 2000 and year <= 2004:
		return "2000-2004"
	elif year >= 2005 and year <= 2009:
		return "2005-2009"
	elif year >= 2010 and year <= 2014:
		return "2010-2014"
	elif year >= 2015 and year <= 2019:
		return "2015-2019"
	else:
		return "invalid"

#Count words of each paragraph
def count_words(paragraph):
	return len(paragraph.split())

start_time = time.time()

#Read csv file
movies = spark.read.format('csv').options(header='false', inferSchema='true').load(movies_csv_file)
movie_genres = spark.read.format('csv').options(header='false', inferSchema='true').load(movie_genres_csv_file)

#Register movies table
movies = movies.withColumnRenamed("_c0","movie_id")
movies = movies.withColumnRenamed("_c2","description")
movies = movies.withColumnRenamed("_c3","timestamp")
movies.registerTempTable("movies")

#Register movie_genres table
movie_genres = movie_genres.withColumnRenamed("_c0","movie_id")
movie_genres = movie_genres.withColumnRenamed("_c1","genre")
movie_genres.registerTempTable("movie_genres")

#Register udf functions
spark.udf.register("decade",decade_function)
spark.udf.register("desc_length",count_words)

#Execute query
sql_table_1 = 'SELECT movie_id,description,YEAR(timestamp) as year_column FROM movies WHERE (timestamp IS NOT NULL) AND YEAR(timestamp) >= 2000 AND description <> ""'
sql_table_2 = 'SELECT * FROM movie_genres WHERE genre = "Drama"'
sql_query = 'SELECT decade(year_column) as dec, AVG(desc_length(description)) as desc_len FROM (' + sql_table_1 + ') AS m JOIN (' + sql_table_2 + ') AS mg ON m.movie_id = mg.movie_id GROUP BY dec' 

query_time = time.time()

res = spark.sql(sql_query)
res.write.csv("hdfs://master:9000/outputs/q4_sparksql_csv.csv")

end_time = time.time()
with open("query_times.txt", "a") as f:
	f.write("Q4 SparkSQL CSV: " + str(end_time-start_time) + " seconds\n")
	f.write("Q4 SparkSQL CSV (without reading): " + str(end_time-query_time) + " seconds\n")

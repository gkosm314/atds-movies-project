from pyspark.sql import SparkSession
from io import StringIO
import csv
import time

#Function to split line of movies.csv
def split_complex(x):
	return list(csv.reader(StringIO(x), delimiter=','))[0]

#Initialize SparkSession and SparkContext
spark = SparkSession.builder.appName("Advanced_Databases_Project").getOrCreate()
sc = spark.sparkContext

#Define path to csv files
movies_csv_file = "hdfs://master:9000/files/movies.csv"
movie_genres_csv_file = "hdfs://master:9000/files/movie_genres.csv"

#Categorize entry to the correct year range or mark it as invalid
def year_category(x):
	line = split_complex(x)

	if line[2] == '':
		return "invalid"

	year_str = line[3][0:4]
	if year_str == '':
		return "invalid"

	year = int(year_str)
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
movie_genres = sc.textFile(movie_genres_csv_file).flatMap(lambda x: [(int(x.split(",")[0]),x.split(",")[1])] if x.split(",")[1] == "Drama" else [])
movies = sc.textFile(movies_csv_file).flatMap(lambda x: [(int(split_complex(x)[0]),(count_words(split_complex(x)[2]),year_category(x)))] if year_category(x) != "invalid" else [])

main_rdd = movies.join(movie_genres).map(lambda x: (x[1][0][1],(x[1][0][0],1))).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).map(lambda x: (x[0],x[1][0]/x[1][1]))
main_rdd.saveAsTextFile("hdfs://master:9000/outputs/q4_rdd.csv")
end_time = time.time()

with open("query_times.txt", "a") as f:
	f.write("Q4 RDD API: " + str(end_time-start_time) + " seconds\n")
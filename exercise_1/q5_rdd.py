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
ratings_csv_file = "hdfs://master:9000/files/ratings.csv"

def genre_row_mapper(x):
	return ((x[1][1][0],x[1][0][0]),(1,(x[1][1][1][0],x[1][0][1],x[1][1][1][1]),(x[1][1][1][0],x[1][0][1],x[1][1][1][1])))

def genre_user_reducer(x,y):
	counter = x[0] + y[0]

	if (x[1][1] > y[1][1]) or (x[1][1] == y[1][1] and x[1][2] > y[1][2]):
		max_movie = x[1]
	else:
		max_movie = y[1]

	if (x[2][1] < y[2][1]) or (x[2][1] == y[2][1] and x[2][2] > y[2][2]):
		min_movie = x[2]
	else:
		min_movie = y[2]

	return (counter,max_movie,min_movie)

def genre_mapper(x): 
	return (x[0][0], (x[0][1],x[1][0],x[1][1][0],x[1][1][1],x[1][2][0],x[1][2][1]))

def genre_reducer(x,y):
	if(x[1] > y[1] or (x[1] == y[1] and x[0]>y[0])):
		return x
	else:
		return y

start_time = time.time()
ratings_rdd = sc.textFile(ratings_csv_file).map(lambda x: (int(x.split(",")[1]),(int(x.split(",")[0]),float(x.split(",")[2]))))
movie_genres_rdd = sc.textFile(movie_genres_csv_file).map(lambda x: (int(x.split(",")[0]),x.split(",")[1]))
movies_rdd = sc.textFile(movies_csv_file).map(lambda x: (int(split_complex(x)[0]),(split_complex(x)[1],float(split_complex(x)[7]))))

movie_genres_popularity = movie_genres_rdd.join(movies_rdd)
ratings_genre_rdd = ratings_rdd.join(movie_genres_popularity).map(lambda x: genre_row_mapper(x)).reduceByKey(lambda x,y: genre_user_reducer(x,y)).map(lambda x: genre_mapper(x)).reduceByKey(lambda x,y: genre_reducer(x,y)).sortByKey()
ratings_genre_rdd.saveAsTextFile("hdfs://master:9000/outputs/q5_rdd.csv")
end_time = time.time()

with open("query_times.txt", "a") as f:
	f.write("Q5 RDD API: " + str(end_time-start_time) + " seconds\n")

from pyspark.sql import SparkSession
import time

#Initialize SparkSession and SparkContext
spark = SparkSession.builder.appName("Advanced_Databases_Project").getOrCreate()
sc = spark.sparkContext

#Define path to csv files
ratings_csv_file = "hdfs://master:9000/files/ratings.csv"
movie_genres_csv_file = "hdfs://master:9000/files/movie_genres.csv"

start_time = time.time()
movies_mean_rating_rdd = sc.textFile(ratings_csv_file).map(lambda x: (int(x.split(",")[1]),(float(x.split(",")[2]),1))).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).map(lambda x: (x[0],x[1][0]/x[1][1]))
movie_genres_rdd = sc.textFile(movie_genres_csv_file).map(lambda x: (int(x.split(",")[0]),x.split(",")[1]))

main_rdd = movies_mean_rating_rdd.join(movie_genres_rdd).map(lambda x: (x[1][1],(x[1][0],1))).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).map(lambda x: (x[0],x[1][0]/x[1][1],x[1][1]))
main_rdd.saveAsTextFile("hdfs://master:9000/outputs/q3_rdd.csv")
end_time = time.time()

with open("query_times.txt", "a") as f:
	f.write("Q3 RDD API: " + str(end_time-start_time) + " seconds\n")

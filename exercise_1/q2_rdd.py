from pyspark.sql import SparkSession
import time

#Initialize SparkSession and SparkContext
spark = SparkSession.builder.appName("Advanced_Databases_Project").getOrCreate()
sc = spark.sparkContext

#Define path to csv files
ratings_csv_file = "hdfs://master:9000/files/ratings.csv"

#res = sc.textFile(ratings_csv_file).map(lambda x: (x.split(",")[0],(float(x.split(",")[2]),1))).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).map(lambda x: (1,1) if x[1][0]/x[1][1] > 3 else (0,1)).reduce(lambda x,y: (x[0]+y[0],x[1]+y[1]))

start_time = time.time()
res = sc.textFile(ratings_csv_file).map(lambda x: (x.split(",")[0],(float(x.split(",")[2]),1))).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).map(lambda x: (1,(1,1)) if x[1][0]/x[1][1] > 3 else (1,(0,1))).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).map(lambda x: (x[1][0]/x[1][1])*100)
res.saveAsTextFile("hdfs://master:9000/outputs/q2_rdd.csv")
end_time = time.time()

with open("query_times.txt", "a") as f:
	f.write("Q2 RDD API : " + str(end_time-start_time) + " seconds\n")

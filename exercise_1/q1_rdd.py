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

def valid_movie(csv_line):
	line = split_complex(csv_line)

	year = line[3][0:4]
	cost_of_production_str = line[5]
	total_earnings_str = line[6]

	if year == '' or cost_of_production_str == '0' or total_earnings_str == '0':
		return False
	elif int(year) < 2000:
		return False
	else:
		return True

def profit(csv_line):
	cost_of_production = int(split_complex(csv_line)[5])
	total_earnings = int(split_complex(csv_line)[6])
	return ((total_earnings - cost_of_production)/cost_of_production)*100

start_time = time.time()

#Define path to csv files
movies_csv_file = "hdfs://master:9000/files/movies.csv"

res = sc.textFile(movies_csv_file).flatMap(lambda x: [(int(split_complex(x)[3][0:4]),(split_complex(x)[1],profit(x)))] if valid_movie(x) else []).reduceByKey(lambda x,y: x if x[1] > y[1] else y).sortByKey()
res.saveAsTextFile("hdfs://master:9000/outputs/q1_rdd.csv")

end_time = time.time()
with open("query_times.txt", "a") as f:
	f.write("Q1 RDD API: " + str(end_time-start_time) + " seconds\n")

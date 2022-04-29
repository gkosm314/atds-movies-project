from pyspark.sql import SparkSession
from collections import defaultdict
from itertools import product
import time

#Initialize SparkSession and SparkContext
spark = SparkSession.builder.appName("Advanced_Databases_Project").getOrCreate()
sc = spark.sparkContext

#We suppose that L << R
def brodcast_join(L_rdd,R_rdd):

	#Convert RDD L to dictionary. In Python dictionary = hashmap
	#https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.collectAsMap.html
	local_L_dict = defaultdict(list)
	for x in L_rdd.collect():
		local_L_dict[x[0]].append(x[1])

	#Broadcast dictionary to the executors
	#https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.Broadcast.html
	L_dict = sc.broadcast(local_L_dict)

	#Perform map-side join
	join_rdd = R_rdd.flatMap(lambda x: [(x[0],(i,x[1])) for i in L_dict.value[x[0]]])

	return join_rdd


def repartition_join(L_rdd,R_rdd):

	#Tag each entry. tag = 0 for entries from L and tag = 1 for entries from R
	L_rdd_tagged = L_rdd.map(lambda x: (x[0],[(0,x[1])]))
	R_rdd_tagged = R_rdd.map(lambda x: (x[0],[(1,x[1])]))

	#Merge the two tables in a common rdd
	union_rdd = L_rdd_tagged.union(R_rdd_tagged)

	#Function that performs the reduce as described in the paper
	def my_reducer(x):

			#Initialize buffers
			L_buffer = []
			R_buffer = []

			#Iterate through entries with key = K
			for entry in x[1]:
					if entry[0] == 0:
							L_buffer.append(entry[1])
					elif entry[0] == 1:
							R_buffer.append(entry[1])

			return [(x[0],i) for i in product(L_buffer,R_buffer)]

	#Why I did not use groupByKey(): https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/prefer_reducebykey_over_groupbykey.html
	join_rdd = union_rdd.reduceByKey(lambda x,y: x+y).flatMap(lambda x: my_reducer(x))

	return join_rdd

if __name__ == '__main__':
	ratings_csv_file = "hdfs://master:9000/files/ratings.csv"
	movie_genres_100_csv_file = "hdfs://master:9000/files/movie_genres_100.csv"

	ratings_rdd = sc.textFile(ratings_csv_file).map(lambda x: (int(x.split(",")[1]),(x.split(",")[0],x.split(",")[2],x.split(",")[3])))
	movie_genres_100_rdd = sc.textFile(movie_genres_100_csv_file).map(lambda x: (int(x.split(",")[0]),x.split(",")[1]))

	broadcast_start_time = time.time()
	brodcast_join(movie_genres_100_rdd,ratings_rdd).saveAsTextFile("hdfs://master:9000/outputs/broadcast_join_results.csv")
	broadcast_end_time = time.time()

	repartition_start_time = time.time()
	repartition_join(movie_genres_100_rdd,ratings_rdd).saveAsTextFile("hdfs://master:9000/outputs/repartition_join_results.csv")
	repartition_end_time = time.time()

	with open("join_times.txt", "a") as f:
			f.write("Broadcast join: " + str(broadcast_end_time-broadcast_start_time) + " seconds\n")
			f.write("Repartition join: " + str(repartition_end_time-repartition_start_time) + " seconds\n")

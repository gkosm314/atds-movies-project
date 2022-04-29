from pyspark.sql import SparkSession

#Initialize SparkSession
spark = SparkSession.builder.appName("CSV_to_Parquet").getOrCreate()

#Define paths to csv files that already exist
movies_csv_file = "hdfs://master:9000/files/movies.csv"
movie_genres_csv_file = "hdfs://master:9000/files/movie_genres.csv"
ratings_csv_file = "hdfs://master:9000/files/ratings.csv"

#Define paths to parquet files that will be created
movies_parquet_file = "hdfs://master:9000/files/movies.parquet"
movie_genres_parquet_file = "hdfs://master:9000/files/movie_genres.parquet"
ratings_parquet_file = "hdfs://master:9000/files/ratings.parquet"

#Read csv files into Spark Dataframes
movies_dataframe = spark.read.csv(movies_csv_file, header=False, inferSchema = True)
movie_genres_dataframe = spark.read.csv(movie_genres_csv_file, header=False, inferSchema = True)
ratings_dataframe = spark.read.csv(ratings_csv_file, header=False, inferSchema = True)

#Generate parquet files from Spark Dataframes and save them
movies_dataframe.write.format("parquet").save(movies_parquet_file)
movie_genres_dataframe.write.format("parquet").save(movie_genres_parquet_file)
ratings_dataframe.write.format("parquet").save(ratings_parquet_file)
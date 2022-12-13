# atds-movies-project
This is a semester project for the course _Advanced Topics in Database Systems_ at the National Technical University of Athens (NTUA).

It was implemented during the winter semester of the academic year 2021-2022.

* This project was developed using Apache Spark.
* The dataset that was used is a subset of a Kaggle dataset named [The Movies Dataset](https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset).
* The full description of the assignment can be found in **homework_assignment.pdf** (in Greek).
* The report that describes the results can be found in **report.pdf** (in Greek).


## Exercise 1

The first part of the assignment demanded the execution of five different queries:
* using MapReduce queries that were implemented by the traditional RDD API
* using SparkSQL
* using SparkSQL and the Parquet file format for the input

The aforementioned executions were compared with regard to their execution times. The comparison led to an understanding of:
* the I/O and aggregation queries performance improvement from the use of the Parquet file format
* the effect of the Catalyst Optimizer on the performance of the SparkSQL queries
* the overhead related to the use of user-defined functions

## Exercise 2

The second part of the assignment involved both the implementation and the performance comparison of two different MapReduce join algorithms, namely the **broadcast join** and the **repartition join**.

Both joins are extensively presented in _" A Comparison of Join Algorithms for Log Processing in MapReduce”, Blanas et al , in Sigmod 2010"_.

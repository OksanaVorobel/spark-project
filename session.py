from pyspark import SparkConf
from pyspark.sql import SparkSession

# Create a SparkSession
spark_session = (
    SparkSession.builder
        .master('local')
        .appName('test app')
        .config(conf=SparkConf())
        .getOrCreate()
)


def stop_session():
    spark_session.stop()

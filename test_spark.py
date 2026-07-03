from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Test")
    .master("local[1]")
    .getOrCreate()
)

print("Spark started successfully!")

spark.stop()
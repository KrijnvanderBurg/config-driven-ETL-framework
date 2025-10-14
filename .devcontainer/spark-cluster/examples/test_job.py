import time

from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.Builder().appName("Simple PySpark Demo").getOrCreate()

df = spark.createDataFrame([(1, "John"), (2, "Jane")], ["id", "name"])

time.sleep(10)  # to allow opening the job in the UI

df.show()

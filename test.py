from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, date_format, collect_list

# Initialize Spark session
spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1/test.coll") \
    .config("spark.mongodb.write.connection.uri", "mongodb://127.0.0.1/test.coll") \
    .getOrCreate()

# Example pipeline steps (replace with your actual pipeline logic)

# Read data from MongoDB collection
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

# Query and process data
df.printSchema()
df.show()

spark.stop()


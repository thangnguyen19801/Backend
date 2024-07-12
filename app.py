from pyspark.sql import SparkSession
import time

# Create Spark session
spark = SparkSession.builder \
    .appName("MongoSparkConnector") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/testdb.testcollection") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/testdb.testcollection") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2") \
    .getOrCreate()

# Create a dummy dictionary
dummy_dict = {
    "name": "John Doe",
    "age": 25,
    "city": "New York"
}

# Convert the dictionary to a PySpark DataFrame
df_write = spark.createDataFrame([dummy_dict])

# Log the time to write the DataFrame to MongoDB
start_time = time.time()
df_write.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()
end_time = time.time()
print(f"Time taken to write DataFrame to MongoDB: {(end_time - start_time) * 1000:.2f} milliseconds")

# Log the time to read data from MongoDB
start_time = time.time()
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
end_time = time.time()
print(f"Time taken to read DataFrame from MongoDB: {(end_time - start_time) * 1000:.2f} milliseconds")
df.show()

# Log the time to perform the transformation
start_time = time.time()
df_filtered = df.filter(df.age > 20)
end_time = time.time()
print(f"Time taken to filter DataFrame: {(end_time - start_time) * 1000:.2f} milliseconds")
df_filtered.show()

# Stop the Spark session
spark.stop()

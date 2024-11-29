from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.feature import VectorAssembler
import pymongo

# Initialize Spark session with MongoDB connector
spark = SparkSession.builder \
    .appName("RealTimeMLIntegration") \
    .config("spark.mongodb.output.uri", "mongodb://<username>:<password>@<cluster-address>/<database>.<collection>") \
    .getOrCreate()

# Define the schema for the incoming data
schema = StructType([
    StructField("feature1", FloatType(), True),
    StructField("feature2", FloatType(), True),
    # Add other features as necessary
    StructField("label", IntegerType(), True)
])

# Read streaming data from a socket
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Assume each line is a JSON string, convert it into a DataFrame
data = lines.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Load a pre-trained Logistic Regression model
model = LogisticRegressionModel.load("path/to/your/model")

# Prepare features using VectorAssembler
assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
data = assembler.transform(data)

# Make predictions
predictions = model.transform(data)

# Select relevant columns
results = predictions.select("feature1", "feature2", "prediction")

# Write the streaming data to MongoDB
query = results.writeStream \
    .format("mongo") \
    .option("checkpointLocation", "/path/to/checkpoint/dir") \
    .outputMode("append") \
    .start()

# Await termination of the streaming query
query.awaitTermination()


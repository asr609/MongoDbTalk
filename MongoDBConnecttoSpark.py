#need to install pyspark and pymongo for executing the code
# Also need to download an external jar from https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.3.0/
# And Also https://repo1.maven.org/maven2/org/mongodb/mongo-java-driver/3.12.14/mongo-java-driver-3.12.14.jar
# And https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.2.3/mongodb-driver-sync-4.2.3.jar

#Implementation of Batch Processing

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# Paths to the MongoDB Spark Connector,MongoDB Java Driver JARs and sync path

mongo_spark_connector_path = "/home/amandeep/Documents/MongoDB/Jars/mongo-spark-connector_2.12-2.4.4.jar"
mongo_java_driver_path = "/home/amandeep/Documents/MongoDB/Jars/mongo-java-driver-3.12.4.jar"
mongo_java_sync_path = "/home/amandeep/Documents/MongoDB/mongodb-driver-sync-4.2.3.jar"


# Initialize Spark session with MongoDB connector
spark = SparkSession.builder \
    .appName("MongoDBBatchIntegration") \
    .config("spark.mongodb.input.uri", "mongodb+srv://AmanMongoDB:Aman123@amandeepclusterdb.re83d.mongodb.net/TEST?retryWrites=true&w=majority&appName=AmandeepClusterDB")\
    .config("spark.mongodb.output.uri","mongodb+srv://AmanMongoDB:Aman123@amandeepclusterdb.re83d.mongodb.net/TEST?retryWrites=true&w=majority&appName=AmandeepClusterDB")\
    .config("spark.jars", f"{mongo_spark_connector_path},{mongo_java_driver_path},{mongo_java_sync_path}") \
    .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
    .getOrCreate()

# Simulate reading a batch of JSON data from a socket or file
data = [
    {"Name": "ABC", "Age": 40, "Occupation": "HR"}
]

# Create DataFrame
df = spark.createDataFrame(data)

# Add a timestamp to each record
df_with_timestamp = df.withColumn("timestamp", current_timestamp())

# Print DataFrame schema and data
df_with_timestamp.printSchema()
df_with_timestamp.show()

# Write the processed data to MongoDB
df_with_timestamp.write \
    .format("mongo") \
    .mode("append") \
    .option("database", "TEST") \
    .option("collection", "TEST") \
    .save()

# Verify the data was written by reading from MongoDB 
df_verification = spark.read.format("mongo").option("uri", "mongodb+srv://AmanMongoDB:Aman123@amandeepclusterdb.re83d.mongodb.net/TEST.TEST?retryWrites=true&w=majority&appName=AmandeepClusterDB").load() 
df_verification.show()

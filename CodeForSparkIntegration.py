# Need to add BSON for translation from CSV to JSON https://repo1.maven.org/maven2/org/mongodb/bson/4.2.3/bson-4.2.3.jar
#need to install pyspark and pymongo for executing the code
# Also need to download an external jar from https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.3.0/
# And Also https://repo1.maven.org/maven2/org/mongodb/mongo-java-driver/3.12.14/mongo-java-driver-3.12.14.jar
# And https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.2.3/mongodb-driver-sync-4.2.3.jar


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Define the schema for the CSV file
csv_schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Occupation", StringType(), True)
])

# Paths to the MongoDB Spark Connector and MongoDB Java Driver JARs
mongo_spark_connector_path = "/home/amandeep/Documents/MongoDB/mongo-spark-connector_2.12-10.3.0.jar"
mongo_java_driver_path = "/home/amandeep/Documents/MongoDB/mongo-java-driver-3.12.14.jar"
mongo_java_sync_path = "/home/amandeep/Documents/MongoDB/mongodb-driver-sync-4.2.3.jar"
mongo_bson_path = "/home/amandeep/Documents/MongoDB/bson-4.2.3.jar"

# Create a SparkSession
spark = SparkSession.builder \
    .appName("RealTimeCSVtoMongoDB") \
    .config("spark.jars", f"{mongo_spark_connector_path},{mongo_java_driver_path},{mongo_java_sync_path},{mongo_bson_path}") \
    .getOrCreate()

# Define the streaming query to read the CSV file
csv_stream = (spark.readStream
    .format("csv")
    .option("header", "true")
    .schema(csv_schema)
    .load("/home/amandeep/Documents/MongoDB/TestDir")  # Specify the path to your CSV file directory
)

# Write the stream data to MongoDB
query = (csv_stream.writeStream
    .format("mongodb")
    .option("checkpointLocation", "/home/amandeep/Documents/MongoDB/Checkpoint")
    .option("uri", "mongodb+srv://AmanMongoDB:Aman123@amandeepclusterdb.re83d.mongodb.net/TEST.TEST?retryWrites=true&w=majority&appName=AmandeepClusterDB")
    .option("database", "TEST")
    .option("collection", "TEST")
    .outputMode("append")
    .start()
)

# Await termination of the query
query.awaitTermination()

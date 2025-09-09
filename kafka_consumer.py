import os
import sys
import findspark
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

# Initialize Spark Session
findspark.init()
spark = SparkSession.builder \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
    .config("spark.driver.memory", "3g") \
    .appName("KafkaConsumerToBatchFile") \
    .getOrCreate()

# Schema for the JSON data coming from Kafka
# This must match the structure of the DataFrame you sent to Kafka
schema = StructType([
    StructField("Contract", StringType(), True),
    StructField("Thể Thao", LongType(), True),
    StructField("Thiếu Nhi", LongType(), True),
    StructField("Giải Trí", LongType(), True),
    StructField("Phim Truyện", LongType(), True),
    StructField("Truyền Hình", LongType(), True),
    StructField("TotalDevices", LongType(), True)
])

def read_from_kafka_as_batch(kafka_broker, kafka_topic):
    """
    Reads all available data from a Kafka topic as a single batch DataFrame.
    """
    print(f"--- Starting to read from Kafka topic '{kafka_topic}' ---")
    df = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()
    return df

def process_data(df):
    """
    Processes the raw Kafka data by extracting and parsing the JSON data.
    """
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*") # This flattens the nested structure
    
    return parsed_df

def write_to_file(df, output_path):
    """
    Writes the processed batch data to a file sink.
    """
    print(f"--- Starting to write data to: {output_path} ---")
    df.write \
        .format("csv") \
        .mode("overwrite") \
        .save(output_path)
    print("--- Data written successfully. ---")

if __name__ == "__main__":
    # Define Kafka and output paths
    KAFKA_BROKER = "localhost:9092"
    KAFKA_TOPIC = "processed-data-topic"
    
    OUTPUT_FOLDER = "C:\\Users\\Admin\\Downloads\\kafka_consumed_data"

    print("--- Starting Kafka Consumer PySpark Batch Job ---")
    
    # 1. Read the data from Kafka as a batch
    batch_df = read_from_kafka_as_batch(KAFKA_BROKER, KAFKA_TOPIC)
    
    # 2. Process and transform the incoming data
    processed_df = process_data(batch_df)
    
    # 3. Write the batch to a file
    write_to_file(processed_df, OUTPUT_FOLDER)
    
    print("--- Kafka Consumer PySpark Batch Job Finished ---")
    
    spark.stop()

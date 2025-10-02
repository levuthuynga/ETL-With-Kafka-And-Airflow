import os 
import sys
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")\
                            .config("spark.driver.memory", "3g").getOrCreate()


    
def get_data(input_url): 
    """
    Reads the input JSON file into a DataFrame.
    """
    df = spark.read.json(input_url)
    print(f"Successfully read {df.count()} records.")
    return df



def select_fields(df):	
    """
    Flattens the data by selecting fields nested under the '_source' structure.
    """
    df = df.select("_source.*")
    return df 



def save_to_kafka(df, kafka_broker: str, kafka_topic: str):
    """
    Saves a DataFrame to a Kafka topic. The DataFrame is converted to a JSON string.
    """
    print(f"--- Sending data to Kafka topic '{kafka_topic}' ---")
    # Convert the entire DataFrame row to a single JSON string
    kafka_df = df.select(
        col("Contract").alias("key"),
        to_json(struct(col("*"))).alias("value")
    )

    # Write the DataFrame to the Kafka topic
    kafka_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("topic", kafka_topic) \
        .save()
    print("Data Sent to Kafka Successfully")



def main(input_url: str, kafka_broker: str, kafka_topic: str):
    df = get_data(input_url)
    df = select_fields(df)
    save_to_kafka(df, kafka_broker, kafka_topic)



if __name__ == "__main__":
    # Check for command-line arguments
    if len(sys.argv) < 2:
        print("Error: Input file path is required as a command-line argument.")
        sys.exit(1)
    
    input_url = sys.argv[1]
    KAFKA_BROKER = os.getenv("KAFKA_BROKER_ADDRESS", "localhost:9092")
    KAFKA_TOPIC = "log-content-topic"

    main(input_url, KAFKA_BROKER, KAFKA_TOPIC)

    spark.stop()


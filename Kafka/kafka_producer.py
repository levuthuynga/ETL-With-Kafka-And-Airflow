import os 
import sys
from datetime import datetime ,timedelta 
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *



spark = SparkSession.builder.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")\
                            .config("spark.driver.memory", "3g").getOrCreate()


    
def get_data(): 
    ## input_url = 'C:\\Users\\Admin\\Downloads\\log_content'
    input_url = '/mnt/c/Users/Admin/Downloads/log_content'
    date = '20220401'
    df = spark.read.json(input_url+"/"+date+'.json')
    return df



def select_fields(df):	
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



def main(kafka_broker, kafka_topic):
    df = get_data()
    df = select_fields(df)
    save_to_kafka(df, kafka_broker, kafka_topic)



if __name__ == "__main__":
    KAFKA_BROKER = "localhost:9092"
    KAFKA_TOPIC = "log-content-topic"
    main(KAFKA_BROKER, KAFKA_TOPIC)


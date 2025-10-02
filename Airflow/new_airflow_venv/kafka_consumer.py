import os 
import sys
from datetime import datetime
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
        .appName("KafkaConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")\
        .config("spark.driver.memory", "3g").getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark Session initialized successfully.")

def read_from_kafka(kafka_broker: str, kafka_topic: str):
    """
    Sets up the Structured Streaming source to read new data from a Kafka topic.
    """
    print(f"--------- Starting to read from Kafka topic '{kafka_topic}' ----------")
    
    schema = StructType([
        StructField("AppName", StringType(), True),
        StructField("Contract", StringType(), True),
        StructField("Mac", StringType(), True),
        StructField("TotalDuration", IntegerType(), True)])
    
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("kafka.default.api.timeout.ms", "600000") \
        .option("kafka.request.timeout.ms", "600000") \
        .load()
    
    
    
    df = df.withColumn("value", col("value").cast(StringType()))
    df_parsed = df.withColumn("data", from_json(col("value"), schema)).select("data.*")

    
    return df_parsed

    
def calculate_devices(df):
    """Calculates the total number of unique devices per contract."""
    print('-------------Calculating Devices --------------')
    total_devices = df.select("Contract","Mac").groupBy("Contract").count()
    total_devices = total_devices.withColumnRenamed('count','TotalDevices')
    return total_devices
    
def transform_category(df):
    """Adds a 'Type' column by categorizing applications."""
    print('-------------Transforming Category --------------')
    df = df.withColumn("Type",
           when((col("AppName") == 'CHANNEL') | (col("AppName") =='DSHD')| (col("AppName") =='KPLUS')| (col("AppName") =='KPlus'), "Truyền Hình")
          .when((col("AppName") == 'VOD') | (col("AppName") =='FIMS_RES')| (col("AppName") =='BHD_RES')| 
                 (col("AppName") =='VOD_RES')| (col("AppName") =='FIMS')| (col("AppName") =='BHD')| (col("AppName") =='DANET'), "Phim Truyện")
          .when((col("AppName") == 'RELAX'), "Giải Trí")
          .when((col("AppName") == 'CHILD'), "Thiếu Nhi")
          .when((col("AppName") == 'SPORT'), "Thể Thao")
          .otherwise("Error"))
    return df 

def calculate_statistics(df): 
    """Calculates total duration pivoted by content type."""
    print('-------------Calculating Statistics --------------')
    statistics = df.select('Contract','TotalDuration','Type').groupBy('Contract','Type').sum()
    statistics = statistics.withColumnRenamed('sum(TotalDuration)','TotalDuration')
    statistics = statistics.groupBy('Contract').pivot('Type').sum('TotalDuration').na.fill(0)
    return statistics 
    
def finalize_result(statistics,total_devices):
    """Joins statistics and device count into the final result."""
    print('-------------Finalizing result --------------')
    result = statistics.join(total_devices,'Contract','inner')
    return result 
    

def etl_process(df):
    print('-------------------Starting ETL Process---------------')
    total_devices = calculate_devices(df)
    df = transform_category(df)
    statistics = calculate_statistics(df)
    result = finalize_result(statistics,total_devices)
    
    # Print schema for debugging
    print('-------------DEBUG: Showing final result schema and sample --------------')
    result.printSchema()
      
    return result


def write_to_sink(stream_df, output_path, checkpoint_path):
    """
    Starts the Structured Streaming query with a one-time trigger 
    and writes the final result to the output path.
    """
    print(f"--- Writing result to: {output_path} ---")
    
    def process_micro_batch(micro_batch_df, batch_id):
        if micro_batch_df.count() > 0:
            final_result_df = etl_process(micro_batch_df)
            final_result_df = final_result_df.coalesce(1) 
            
            save_data_path = os.path.join(output_path, f"batch_{batch_id}")
            print(f"Saving final output of batch {batch_id} to {save_data_path}")

            final_result_df.write.option("header","true").csv(save_data_path, mode='overwrite')
        else:
            print(f"Micro-batch {batch_id} was empty. Skipping write.")

    # Start the query
    query = stream_df.writeStream \
        .foreachBatch(process_micro_batch) \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(once=True) \
        .start()
        
    query.awaitTermination()
    print("Stream finished (Trigger Once)")
    return True # Indicate successful processing



def main(kafka_broker: str, kafka_topic: str, output_path: str, checkpoint_path: str):
    """Main execution flow."""
    print(f"-------------------Consumer starting. Output path: {output_path}----------------------")
    stream_df = read_from_kafka(kafka_broker, kafka_topic)
    result = write_to_sink(stream_df, output_path, checkpoint_path)
    


if __name__ == "__main__":
    KAFKA_BROKER = os.getenv("KAFKA_BROKER_ADDRESS", "localhost:9092")
    KAFKA_TOPIC = "log-content-topic"

    CHECKPOINT_BASE = os.path.expanduser("~/kafka_checkpoints")  
    CHECKPOINT_PATH = os.path.join(CHECKPOINT_BASE, KAFKA_TOPIC)

    output_path = '/tmp/content_result_' + datetime.now().strftime('%Y%m%d%H%M%S')
    main(KAFKA_BROKER, KAFKA_TOPIC, output_path, CHECKPOINT_PATH)
      
    spark.stop()


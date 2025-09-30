import os 
import sys
from datetime import datetime ,timedelta 
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")\
                            .config("spark.driver.memory", "3g").getOrCreate()


def read_from_kafka(kafka_broker, kafka_topic):
    """
    Reads all available data from a Kafka topic as a single batch DataFrame.
    """
    print(f"--- Starting to read from Kafka topic '{kafka_topic}' ---")
	
    schema = StructType([
		StructField("AppName", StringType(), True),
		StructField("Contract", StringType(), True),
		StructField("Mac", StringType(), True),
		StructField("TotalDuration", IntegerType(), True)])
	
    df = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
		.option("endingOffsets", "latest") \
		.option("kafka.default.api.timeout.ms", "600000") \
	    .option("kafka.request.timeout.ms", "600000") \
        .load()
    
    
	
    df = df.withColumn("value", col("value").cast(StringType()))
    df_parsed = df.withColumn("data", from_json(col("value"), schema)).select("data.*")

    df_parsed = df_parsed.cache()
    num_records = df_parsed.count()
    print(f"---------------------Successfully read {num_records} records from Kafka and cached------------------")
	
    return df_parsed

	
def calculate_devices(df):
	total_devices = df.select("Contract","Mac").groupBy("Contract").count()
	total_devices = total_devices.withColumnRenamed('count','TotalDevices')
	return total_devices
	
def transform_category(df):
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
	statistics = df.select('Contract','TotalDuration','Type').groupBy('Contract','Type').sum()
	statistics = statistics.withColumnRenamed('sum(TotalDuration)','TotalDuration')
	statistics = statistics.groupBy('Contract').pivot('Type').sum('TotalDuration').na.fill(0)
	return statistics 
	
def finalize_result(statistics,total_devices):
	result = statistics.join(total_devices,'Contract','inner')
	return result 
	
def save_data(result,save_path):
	#result.repartition(1).write.option("header","true").csv(save_path, mode='overwrite')
	result.write.option("header","true").csv(save_path, mode='overwrite')
	return print("Data Saved Successfully")



def etl_main(df):
	print('-------------Calculating Devices --------------')
	total_devices = calculate_devices(df)
	print('-------------Transforming Category --------------')
	df = transform_category(df)
	print('-------------Calculating Statistics --------------')
	statistics = calculate_statistics(df)
	print('-------------Finalizing result --------------')
	result = finalize_result(statistics,total_devices)
	# --- NEW DEBUG STEP ---
	print('-------------DEBUG: Showing final result schema and sample --------------')
	result.printSchema()
	result.limit(5).show() # This forces execution up to this point
	# --- END DEBUG STEP ---
	print('-------------Saving Results --------------')
	return result




def main(kafka_broker, kafka_topic, output_path):
    df = read_from_kafka(kafka_broker, kafka_topic)
    result = etl_main(df)
    save_data(result,output_path)
    return result
	


if __name__ == "__main__":
    #KAFKA_BROKER = "localhost:9092"
	KAFKA_BROKER = os.getenv("KAFKA_BROKER_ADDRESS", "localhost:9092")
	KAFKA_TOPIC = "log-content-topic"
    #output_path = 'C:\\Users\\Admin\\Downloads\\content_result'
    #output_path = '/mnt/c/Users/Admin/Downloads/content_result'
	output_path = '/tmp/content_result_' + datetime.now().strftime('%Y%m%d%H%M%S')
	main(KAFKA_BROKER, KAFKA_TOPIC, output_path)


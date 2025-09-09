import os 
import sys
from datetime import datetime ,timedelta 
import findspark
findspark.init()
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import * 

spark = SparkSession.builder.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5")\
                            .config("spark.driver.memory", "3g").getOrCreate()

def read_data_from_path(path):
	df = spark.read.json(path)
	return df 

def select_fields(df):	
	df = df.select("_source.*")
	return df 
	
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
	result.repartition(1).write.option("header","true").csv(save_path, mode='overwrite')
	return print("Data Saved Successfully")


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



def etl_main(df):
	#df = select_fields(df)
	print('-------------Calculating Devices --------------')
	total_devices = calculate_devices(df)
	print('-------------Transforming Category --------------')
	df = transform_category(df)
	print('-------------Calculating Statistics --------------')
	statistics = calculate_statistics(df)
	print('-------------Finalizing result --------------')
	result = finalize_result(statistics,total_devices)
	print(result)
	print('-------------Saving Results --------------')
	#save_data(result,save_path)
	return result

def input_path():
    url = str(input('Please provide datadata source folder'))
    return url

def output_path():
    url = str(input('Please provide destination folder'))
    return url

def list_files(path):
    list_files = os.listdir(path)
    print(list_files)
    # print('How many files you want to ETL')
    return list_files

#input_path = input_path()
#output_path = output_path()
#input_path = 'C:\\Users\\Admin\\Downloads\\log_content'
#output_path = 'C:\\Users\\Admin\\Downloads\\content_result'


def input_period():
    #start_date = str(input('Please input start_date format yyyymmdd '))
    start_date = "20220401"
    start_date = datetime.strptime(start_date,"%Y%m%d").date()
    to_date = "20220402"
    #to_date = str(input("Please input to_date format yyyymmdd "))
    to_date = datetime.strptime(to_date,"%Y%m%d").date()
    return start_date, to_date

def generate_date_list(start_date, to_date):
    date_list = []
    current_date = start_date 
    end_date = to_date
    while (current_date <= end_date):
        date_list.append(current_date.strftime("%Y%m%d"))
        current_date += timedelta(days=1)
    print(date_list)
    return date_list 

def etl_data(input_path, date_list):
    start_time = datetime.now()
    df = spark.read.json(input_path+"\\"+date_list[0]+'.json')
    df = select_fields(df)
    result = etl_main(df)
    for i in date_list[1:]:
        print("ETL_TASK" + input_path + i + ".json")
        new_df = spark.read.json(input_path+"\\" +i + '.json')
        new_df = select_fields(new_df)
        new_result = etl_main(new_df)
        print("Union df with new df")
        result = result.union(new_result)
#print("Calculation on final output")
    end_time = datetime.now()
    print((end_time - start_time).total_seconds())
    return result
	
    
    

def main(kafka_broker, kafka_topic):
    #input_url = input_path()
    #output_url = output_path()
    input_url = 'C:\\Users\\Admin\\Downloads\\log_content'
    #if len(sys.argv) > 1:
    #    input_url = sys.argv[1]
    #else:
    #    print("Error: No input file path provided as a command-line argument.")
    #    sys.exit(1)
    output_url = 'C:\\Users\\Admin\\Downloads\\content_result'
    file_list = list_files(input_url) 
    start_date, to_date = input_period()
    date_list = generate_date_list(start_date, to_date)
    result = etl_data(input_url, date_list)
    print('---Saving data---')
    save_data(result,output_url)
    save_to_kafka(result, kafka_broker, kafka_topic)

if __name__ == "__main__":
    # INPUT_PATH = 'C:\\Users\\Admin\\Downloads\\log_content'
    # OUTPUT_PATH = 'C:\\Users\\Admin\\Downloads\\content_result'
    KAFKA_BROKER = "localhost:9092"
    KAFKA_TOPIC = "processed-data-topic"
    main(KAFKA_BROKER, KAFKA_TOPIC)


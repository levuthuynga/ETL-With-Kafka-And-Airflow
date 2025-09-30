import pendulum
import os
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator


KAFKA_EXTERNAL_BROKER = "172.31.1.223:9092"

# Define the default arguments that will be applied to all tasks in the DAG.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 1, 1, tz="UTC"),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Note: The 'check_for_daily_file' function is now defined directly inside this file
@task
def check_for_daily_file():
    """
    Checks if a file with today's date exists in the specified directory.
    If the file exists, it returns the file path.
    Otherwise, it raises a ValueError to fail the task.
    """
    # The Windows path is mounted at /mnt/<drive_letter> in WSL
    # CHANGE THIS PATH to match where your data folder is located on your Windows machine
    DATA_DIR = "/mnt/c/Users/Admin/Downloads/log_content"
    
    # Get the current date in YYYY-MM-DD format
    today_date = '20220401'
    #today_date = pendulum.now().format("YYYY-MM-DD")
    
    # Construct the expected file name
    file_name = f"{today_date}.json"
    file_path = os.path.join(DATA_DIR, file_name)

    # Check if the file exists
    if os.path.exists(file_path):
        print(f"File found: {file_path}")
        return file_path
    else:
        # If the file is not found, raise an error to stop the DAG run
        raise ValueError(f"File not found: {file_path}")

@dag(
    dag_id="etl_data_pipeline",
    default_args=default_args,  # We are now using the default arguments here
    schedule=timedelta(minutes=30),  # Check for a new file every 5 minutes
    catchup=False,
    tags=["spark", "kafka", "pipeline"],
)
def etl_data_pipeline():
    """
    An ETL pipeline that checks for a daily file, and if found,
    triggers a PySpark producer and consumer.
    """
    
    # Step 1: Check for the daily file
    # The returned file path will be stored in XCom.
    check_file_task = check_for_daily_file()

    # Step 2: Run the Spark producer script
    # This BashOperator executes the spark-submit command for the producer.
    # It uses XCom to pull the file path from the previous task.
    run_producer_script = BashOperator(
        task_id='run_producer_script',
        bash_command=f"export KAFKA_BROKER_ADDRESS={KAFKA_EXTERNAL_BROKER} && spark-submit \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
            /mnt/c/Users/Admin/Documents/study_da/ETL-With-Kafka-And-Airflow/kafka_producer.py \
            {{{{ task_instance.xcom_pull(task_ids='check_for_daily_file') }}}} ",
    )
    
    # Step 3: Run the Spark consumer script
    # This BashOperator executes the spark-submit command for the consumer.
    run_consumer_script = BashOperator(
        task_id='run_consumer_script',
        bash_command=f"export KAFKA_BROKER_ADDRESS={KAFKA_EXTERNAL_BROKER} && spark-submit \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
            /mnt/c/Users/Admin/Documents/study_da/ETL-With-Kafka-And-Airflow/kafka_consumer.py",
    )

    # Set the dependencies for the tasks
    check_file_task >> run_producer_script >> run_consumer_script

# Instantiate the DAG
etl_data_pipeline()
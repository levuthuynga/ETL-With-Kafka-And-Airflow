import pendulum
import os
from datetime import timedelta
from os.path import expanduser, join
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator


KAFKA_EXTERNAL_BROKER = "localhost:9092"
PROJECT_BASE_DIR = expanduser("~/new_airflow_venv")
DATA_INPUT_DIR = join(PROJECT_BASE_DIR, "log_content")

# Define the default arguments.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 1, 1, tz="UTC"),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@task
def check_for_daily_file():
    """
    Checks if a file with today's date exists in the specified directory.
    If the file exists, it returns the file path.
    Otherwise, it raises a ValueError to fail the task.
    """
    
    # Get the current date in YYYYMMDD format
    today_date = pendulum.now().format("YYYYMMDD")
    
    # Construct the expected file name
    file_name = f"{today_date}.json"
    file_path = os.path.join(DATA_INPUT_DIR, file_name)

    # Check if the file exists
    if os.path.exists(file_path):
        print(f"---------------File found: {file_path}------------------")
        return file_path
    else:
        # If the file is not found, raise an error to stop the DAG run
        raise ValueError(f"--------------------File not found: {file_path}------------------")

@dag(
    dag_id="etl_data_pipeline",
    default_args=default_args,  
    schedule='0 1 * * *', 
    catchup=False,
    tags=["spark", "kafka", "pipeline"],
)
def etl_data_pipeline():
    """
    An ETL pipeline that checks for a daily file, and if found,
    triggers a PySpark producer and consumer.
    """
    
    # Step 1: Check for the daily file
    check_file_task = check_for_daily_file()

    PRODUCER_SCRIPT_PATH = join(PROJECT_BASE_DIR, "kafka_producer.py")
    CONSUMER_SCRIPT_PATH = join(PROJECT_BASE_DIR, "kafka_consumer.py")

    # Step 2: Run the Spark producer script
    # It pulls the file path from the previous task.
    run_producer_script = BashOperator(
        task_id='run_producer_script',
        bash_command=f"export KAFKA_BROKER_ADDRESS={KAFKA_EXTERNAL_BROKER} && spark-submit \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
            {PRODUCER_SCRIPT_PATH} \
            {{{{ task_instance.xcom_pull(task_ids='check_for_daily_file') }}}} ",
    )
    
    # Step 3: Run the Spark consumer script
    run_consumer_script = BashOperator(
        task_id='run_consumer_script',
        bash_command=f"export KAFKA_BROKER_ADDRESS={KAFKA_EXTERNAL_BROKER} && spark-submit \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
            {CONSUMER_SCRIPT_PATH}",
    )

    # Set the dependencies for the tasks
    check_file_task >> run_producer_script >> run_consumer_script

# Instantiate the DAG
etl_data_pipeline()

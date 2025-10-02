# Install Python on WSL
sudo apt-get update 

sudo apt-get install python3-full

# Create virtual environment
cd ~ 
python3 -m venv new_airflow_venv

# Activate the virtual environment
source new_airflow_venv/bin/activate

# Install airflow
pip install "apache-airflow[celery,postgres,google,ssh,redis,mysql,statsd]"

# Create a directory
mkdir ~/airflow

# Set the environment variables
Export AIRFLOW_HOME=~/airflow

# Initialize database
Cd ~/airflow
Airflow db migrate

# Create user for web ui
airflow standalone

# Change encoding
set PYTHONIOENCODING=utf-8

# Open UI
http://localhost:8080


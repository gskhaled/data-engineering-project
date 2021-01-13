# step 1 - import modules
import os
from datetime import datetime

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow import DAG

# step 2 - define default args
# These args will get passed on to each operator
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 28)
}

# step 3 - instantiate DAG
dag = DAG(
    'milestone_3_DAG',
    default_args=default_args,
    description='ETL Pipeline to estimate the sentiment of tweets from 2 countries',
    schedule_interval='@daily'
)

THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
extract_file = os.path.join(THIS_FOLDER, 'Extract.py')
transform_file = os.path.join(THIS_FOLDER, 'Transform.py')
average_file = os.path.join(THIS_FOLDER, 'Average.py')

# Using a bash operator in order to fetch data from Twitter API
extract_from_Twitter_API = BashOperator(
    task_id='extract_data_from_Twitter_API',
    bash_command="python3 " + extract_file,
    dag=dag,
)

# Using a bash operator in order to calculate sentiment from Tweets
clean_and_save_tweets = BashOperator(
    task_id='clean_and_load_tweets_csv_file',
    bash_command="python3 " + transform_file,
    dag=dag,
)

calculate_day_average = BashOperator(
    task_id='calculate_sentiment_average_for_the_day',
    bash_command="python3 " + average_file,
    dag=dag,
)

# step 5 - define dependencies
extract_from_Twitter_API >> clean_and_save_tweets >> calculate_day_average
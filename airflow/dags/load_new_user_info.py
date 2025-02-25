from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from scripts.common import insert_user, download_user_info

TABLE_NAME = os.environ['AUTH_DB_TABLE']
USER = os.environ['AUTH_DB_USER']
PASSWORD = os.environ['AUTH_DB_PASSWORD']
HOST = os.environ['AUTH_DB_HOST']
DB = os.environ['AUTH_DB_DATABASE']

db_connection = {'table_name': TABLE_NAME, 
                 'user': USER,
                 'password': PASSWORD,
                 'host': HOST,
                 'db': DB}

load_new_user_dag = DAG(
   dag_id = 'load_new_user_info',
   schedule_interval=None,
   catchup=False
)

with load_new_user_dag:
   
   load_new_user_data = PythonOperator(
      task_id = 'load_new_user_data',
      python_callable=insert_user,
      op_kwargs=db_connection,
      provide_context=True
   )

   download_new_user_data = PythonOperator(
      task_id='download_new_user_data',
      python_callable=download_user_info,
      op_kwargs=db_connection,
      provide_context=True
   )

   load_new_user_data >> download_new_user_data
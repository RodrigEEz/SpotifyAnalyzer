from airflow import DAG
from airflow.operators.python import PythonOperator
from common.download_user_data import download_user_data

dag =  DAG( 
      dag_id='download_new_user_data',
      catchup=False,
      schedule_interval=None,
      max_active_runs=5
      )


with dag:

   download_new_user_data = PythonOperator(
      task_id='download_new_user_data_task',
      python_callable=download_user_data,
      provide_context=True,
      op_args=[True]
   )



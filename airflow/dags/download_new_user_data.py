from airflow import DAG
from airflow.operators.python import PythonOperator
from common.download_user_data import download_user_data
from common.save_user_info import save_user_tokens, save_user_updates

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
      op_kwargs={'user_type':'new'}
   )

   save_user_tokens = PythonOperator(
      task_id='save_user_tokens_task',
      python_callable=save_user_tokens,
      op_kwargs={'user_type':'new'}
   )
   
   save_user_updates = PythonOperator(
      task_id='save_user_updates_task',
      python_callable=save_user_updates,
      op_kwargs={'user_type':'new'}
   )


   download_new_user_data >> save_user_tokens >> save_user_updates



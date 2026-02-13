from airflow import DAG
for airflo
from airflow.operators.python import PythonOperator
from common.download_user_data import download_user_data
from common.save_user_info import save_user_tokens, save_user_updates


def refresh_access_token():
   """
   Refreshes the access token for the user.
   """
   



dag = DAG(
      dag_id='download_existing_user_data',
      catchup=False,
      schedule_interval=None,
      max_active_runs=10
      )


with dag:

   retrieve_access_token = PythonOperator(
      task_id='refresh_access_token_task',
      python_callable=refresh_access_token
   )

   download_existing_user_data = PythonOperator(
      task_id='download_existing_user_data_task',
      python_callable=download_user_data,
      provide_context=True,
      op_args=[False]
   )

   save_user_tokens = PythonOperator(
      task_id='save_user_tokens_task',
      python_callable=save_user_tokens
   )


   download_existing_user_data >> save_user_tokens >> save_user_updates
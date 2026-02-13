import os, requests, base64
from airflow import DAG
from airflow.operators.python import PythonOperator
from common.download_user_data import download_user_data
from common.save_user_info import create_connection, save_user_tokens, save_user_updates
from sqlalchemy import select, MetaData


def refresh_access_token(user_email):
   """
   Refreshes the access token for the user.
   """
   engine = create_connection()

   metadata = MetaData()
   metadata.reflect(bind=engine)

   table = os.environ.get('AUTH_DB_TOKEN_TABLE_NAME')
   token_table = metadata.tables[table]
   
   stmt = select(token_table.c.refresh_token).where(token_table.c.email == user_email)

   with engine.connect() as conn:
      with conn.begin():
         refresh_token = conn.execute(stmt)

   client_credentials = f'{client_id}:{client_secret}'
   client_credentials_base64 = base64.b64encode(client_credentials.encode()).decode()

   headers = {
      'Authorization' : f'Basic {client_credentials_base64}',
      'Content-Type' : 'application/x-www-form-urlencoded'
   }

   payload = {
      'grant-type': 'refresh-token',
      'refresh-token' : refresh_token
   }

   response = requests.post(token_url, headers=headers, data=payload)

   access_token = response.json()['access_token']

   return access_token

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

   save_user_tokens = PythonOperator(
      task_id='save_user_tokens_task',
      python_callable=save_user_tokens,
      op_kwargs={'usert_type': 'existing', 'email': email}
   )

   download_existing_user_data = PythonOperator(
      task_id='download_existing_user_data_task',
      python_callable=download_user_data
   )


   download_existing_user_data >> save_user_tokens >> save_user_updates
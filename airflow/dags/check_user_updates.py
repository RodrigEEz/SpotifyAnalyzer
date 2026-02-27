import os
from sqlalchemy import select, union_all, literal, func, text, MetaData
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from connections import create_connection

@dag(
   start_date=datetime(2024, 6, 1),
   schedule='@daily',
   catchup=False,
)
def check_user_updates():

   @task
   def query_outdated_users():
      # Logic to check for user updates goes here

      engine = create_connection()

      metadata = MetaData()
      metadata.reflect(bind=engine)

      table = os.environ.get('AUTH_DB_UPDATES_TABLE_NAME')
      updates_table = metadata.tables[table]
      

      q_short = (
         select(
            updates_table.c.user_id,
            literal("short").label("mode")
         )
         .where(
            updates_table.c.last_updated_short <=
            func.current_date() - text("INTERVAL '7 days'")
         )
      )

      # --- MEDIUM (1 mes)
      q_medium = (
         select(
            updates_table.c.user_id,
            literal("medium").label("mode")
         )
         .where(
            updates_table.c.last_updated_medium <=
            func.current_date() - text("INTERVAL '1 month'")
         )
      )

      # --- LONG (1 año)
      q_long = (
         select(
            updates_table.c.user_id,
            literal("long").label("mode")
         )
         .where(
            updates_table.c.last_updated_long <=
            func.current_date() - text("INTERVAL '1 year'")
         )
      )

      # --- UNION
      query = union_all(q_short, q_medium, q_long)

      with engine.connect() as conn:
         result = conn.execute(query)

         data = [
            {'email': row.user_id, 'term': row.mode} for row in result
         ]

      return data
   
   outdated_users = query_outdated_users()

   TriggerDagRunOperator.partial(
      task_id='trigger_download_existing_user_data',
      trigger_dag_id='download_existing_user_data',
      conf=outdated_users
   ).expand(conf=outdated_users)



from sqlalchemy import create_engine, insert, MetaData
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import SQLAlchemyError
import os
from connections import create_connection


def save_user_tokens(**kwargs):
   """
   Save user token information in the database.
    
   Args:
      email (str): User's email address.
      access_token (str): Access token for the user.
      refresh_token (str): Refresh token for the user.
   """

   engine = create_connection()

   metadata = MetaData()
   metadata.reflect(bind=engine)

   table = os.environ.get('AUTH_DB_TOKEN_TABLE_NAME')
   token_table = metadata.tables[table]

   user_type = kwargs.get('user_type')

   dag_run = kwargs.get('dag_run')
   email = dag_run.conf.get("email")

   if user_type == 'new':

      access_token = dag_run.conf.get("access_token")
      refresh_token = dag_run.conf.get("refresh_token")
      stmt = insert(token_table).values(email=email, access_token=access_token, refresh_token=refresh_token)

   elif user_type == 'existing':

      access_token = kwargs.get('ti').xcom_pull(task_ids='refresh_access_token_task')
      stmt = token_table.update().where(token_table.c.email == email).values(access_token=access_token)
   
   try:
      with engine.connect() as conn:
         with conn.begin():
            conn.execute(stmt)
   except SQLAlchemyError as e:
      print(f"Error saving user info: {e}")
      return f"Error saving user info: {e}"


def save_user_updates(**kwargs):
   """
   Save user update information in the database.
    
   Args:
      email (str): User's email address.
      update_info (str): Information about the user's update.
   """

   engine = create_connection()
   metadata = MetaData()
   metadata.reflect(bind=engine)

   table = os.environ.get('AUTH_DB_UPDATES_TABLE_NAME')
   update_table = metadata.tables[table]

   user_type = kwargs.get('user_type')

   dag_run = kwargs.get('dag_run')
   email = dag_run.conf.get("email")

   if user_type == 'new' :

      update_date = kwargs.get("ds")

      short_term = update_date
      medium_term = update_date
      long_term = update_date

      stmt = insert(update_table).values(email=email, 
                                          last_update_short_term=short_term, 
                                          last_update_medium_term=medium_term,
                                             last_update_long_term=long_term
                                          )

   elif user_type == 'existing' :

      term = dag_run.conf.get("term")

      if term == "short_term":
         stmt = update_table.update().where(update_table.c.email == email).values(last_update_short_term=update_date)
      elif term == "medium_term":
         stmt = update_table.update().where(update_table.c.email == email).values(last_update_medium_term=update_date)
      elif term == "long_term":
         stmt = update_table.update().where(update_table.c.email == email).values(last_update_long_term=update_date)

   try:
      with engine.connect() as conn:
         with conn.begin():
            conn.execute(stmt)
   except SQLAlchemyError as e:
      print(f"Error updating user info: {e}")
      return f"Error updating user info: {e}"

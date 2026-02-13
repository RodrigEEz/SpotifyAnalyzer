from sqlalchemy import create_engine, insert, MetaData
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import SQLAlchemyError
import os


def create_connection():
   """Creates a connection to the database using SQLAlchemy.
    
   Returns:
      engine: A SQLAlchemy engine object for database interactions.
   """

   AUTH_DB_USERNAME= os.environ.get('AUTH_DB_USERNAME')
   AUTH_DB_PASSWORD=  os.environ.get('AUTH_DB_PASSWORD')
   AUTH_DB_HOST= os.environ.get('AUTH_DB_HOST')
   AUTH_DB_PORT= os.environ.get('AUTH_DB_PORT')
   AUTH_DB_DATABASE= os.environ.get('AUTH_DB_DATABASE')

   url_object = URL.create(
      drivername="postgresql",
      username=AUTH_DB_USERNAME,
      password=AUTH_DB_PASSWORD,
      host=AUTH_DB_HOST,
      port=AUTH_DB_PORT,
      database=AUTH_DB_DATABASE
   )
   engine = create_engine(url_object)
   return engine


def save_user_tokens(**kwargs):
   """
   Save user token information in the database.
    
   Args:
      email (str): User's email address.
      access_token (str): Access token for the user.
      refresh_token (str): Refresh token for the user.
   """

   user_type = kwargs.get('user_type')


   if user_type == 'new':

      dag_run = kwargs.get('dag_run')
      email = dag_run.conf.get("email")
      access_token = dag_run.conf.get("access_token")
      refresh_token = dag_run.conf.get("refresh_token")

   elif user_type == 'existing':

      email = kwargs.get('email')
      access_token = kwargs.get('access_token')
      refresh_token = kwargs.get('access_token')

   engine = create_connection()

   metadata = MetaData()
   metadata.reflect(bind=engine)

   table = os.environ.get('AUTH_DB_TOKEN_TABLE_NAME')
   token_table = metadata.tables[table]

   stmt = insert(token_table).values(email=email, access_token=access_token, refresh_token=refresh_token)

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

   dag_run = kwargs.get('dag_run')
   email = dag_run.conf.get("email")
   update_date = kwargs.get("ds")

   print(update_date)

   engine = create_connection()
   metadata = MetaData()
   metadata.reflect(bind=engine)

   table = os.environ.get('AUTH_DB_UPDATES_TABLE_NAME')
   update_table = metadata.tables[table]

   user_type = kwargs.get('user_type')

   if user_type == 'new' :

      short_term = update_date
      medium_term = update_date
      long_term = update_date

      stmt = insert(update_table).values(email=email, 
                                         last_update_short_term=short_term, 
                                         last_update_medium_term=medium_term,
                                           last_update_long_term=long_term
                                       )

      try:
         with engine.connect() as conn:
            with conn.begin():
               conn.execute(stmt)
      except SQLAlchemyError as e:
         print(f"Error saving user update info: {e}")
         return f"Error saving user update info: {e}"
   
   elif user_type == 'existing' :

      if mode == "short_term":
         stmt = update_table.update().where(update_table.c.email == email).values(last_update_short_term=update_date)
      elif mode == "medium_term":
         stmt = update_table.update().where(update_table.c.email == email).values(last_update_medium_term=update_date)
      elif mode == "long_term":
         stmt = update_table.update().where(update_table.c.email == email).values(last_update_long_term=update_date)

      try:
         with engine.connect() as conn:
            with conn.begin():
               conn.execute(stmt)
      except SQLAlchemyError as e:
         print(f"Error updating user info: {e}")
         return f"Error updating user info: {e}"

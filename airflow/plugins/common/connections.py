import os, io, json, datetime
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine

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


def save_user_data(client, term, type, data, email):
   """Saves user data to MinIO.
   """
   bucket_name = 'spotify-raw'
   object_name = f'{email}_{type}_{term}_{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}.json'
   client.put_object(
      bucket_name=bucket_name,
      object_name=object_name,
      data=io.BytesIO(json.dumps(data).encode('utf-8')),
      length=len(json.dumps(data).encode('utf-8'))
   )
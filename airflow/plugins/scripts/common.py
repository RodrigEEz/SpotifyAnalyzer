from sqlalchemy import insert, create_engine, MetaData
from sqlalchemy.exc import SQLAlchemyError
import os
import requests

def insert_user(table_name, user, password, host, db, **context):
   """Inserts Spotify user email, access token and refresh token for future data extraction"""

   email, access_token, refresh_token = context['dag_run'].conf.values()

   engine = create_engine(f'postgresql://{user}:{password}@{host}:5432/{db}')

   metadata = MetaData()
   metadata.reflect(bind=engine)

   table = metadata.tables[table_name]

   try:
      with engine.connect() as conn:
         with conn.begin():
            stmt = table.insert().values(email=email, refresh_token=refresh_token, access_token=access_token)
            conn.execute(stmt)

         return 'Successfully saved user'
   except SQLAlchemyError as e:
      return f'database error: {e}'
   
def download_user_info(**context):
   """Download Spotify user info: most listened songs & tracks, for multiple time periods"""

   _, access_token, refresh_token  = context['dag_run'].conf.values()

   url = 'https://api.spotify.com/v1/me/top/artists?time_range=medium_term&limit=50&offset=0'

   headers = {'Authorization' : f'Bearer {access_token}'}



   # download top artists
   response = requests.get(url, headers=headers)

   print(response.content)
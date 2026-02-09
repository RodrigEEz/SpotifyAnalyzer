import requests
from minio import Minio
import os
import datetime
import json
import io

def download_user_data(new_user, **context):
   """
   Downloads user top tracks and artists in a period based on the specified parameters.
   """

   def save_user_data(client, term, type, data):
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

   email, access_token  = context['dag_run'].conf.values()

   headers = {'Authorization' : f'Bearer {access_token}'}

   MINIO_HOST = os.environ.get('MINIO_HOST') + ':' + os.environ.get('MINIO_PORT')
   ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
   SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')

   #creates minio connection
   MINIO_CLIENT = Minio(MINIO_HOST, ACCESS_KEY, SECRET_KEY, secure=False)

   url_prefix = 'https://api.spotify.com/v1/me/top/'


   for type in ('artists', 'tracks'):

      url = f'{url_prefix}{type}'

      # If user is new downloads every data available
      if new_user:
         terms = ('short_term', 'medium_term', 'long_term')

      for term in terms:
         full_url = f'{url}?time_range={term}&limit=50&offset=0'

         # Download data
         response = requests.get(full_url, headers=headers)
         data = response.json()
         #store in minio
         save_user_data(MINIO_CLIENT, term, type, data)

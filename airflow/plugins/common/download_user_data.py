import requests, os
from minio import Minio
from connections import save_user_data


def download_user_data(**kwargs):
   """
   Downloads user top tracks and artists in a period based on the specified parameters.
   """

   dag_run = kwargs.get('dag_run')
   email = dag_run.conf.get("email")
   user_type = kwargs.get("user_type")

   if user_type == 'new' :
      access_token = dag_run.conf.get("access_token")
   else:
      access_token = kwargs.get('ti').xcom_pull(task_ids='refresh_access_token_task')

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
      if user_type == 'new':
         terms = ('short_term', 'medium_term', 'long_term')
      else:
         terms = (dag_run.conf.get("term"))

      for term in terms:
         full_url = f'{url}?time_range={term}&limit=50&offset=0'

         # Download data
         response = requests.get(full_url, headers=headers)
         data = response.json()
         #store in minio
         save_user_data(MINIO_CLIENT, term, type, data, email)

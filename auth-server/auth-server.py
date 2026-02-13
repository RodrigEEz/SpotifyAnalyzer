from flask import Flask, request, redirect
import requests
import urllib
import os

app = Flask(__name__)   

app.secret_key = os.environ['FLASK_SECRET_KEY'] 
CLIENT_ID = os.environ['SPOTIFY_CLIENT_ID']
CLIENT_SECRET = os.environ['SPOTIFY_CLIENT_SECRET']
REDIRECT_URI = os.environ['SPOTIFY_REDIRECT_URI']
SCOPE = "user-read-email user-top-read"
AUTH_URL = "https://accounts.spotify.com/authorize"
TOKEN_URL = "https://accounts.spotify.com/api/token"
EMAIL_RETRIEVE_URL = "https://api.spotify.com/v1/me"

@app.route('/')
def home():
   params = {
      "client_id": CLIENT_ID,
      "response_type": "code",
      "redirect_uri": REDIRECT_URI,
      "scope": SCOPE
   }
   auth_url = f"{AUTH_URL}?{urllib.parse.urlencode(params)}"
   return redirect(auth_url)
   

@app.route("/callback")
def callback():
    code = request.args.get("code")
    if not code:
        print('error')
        return "Authorization code not received.", 400

    # Exchange code for token
    payload = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    response = requests.post(TOKEN_URL, data=payload)
    if response.status_code == 200:

        # retrieve access and refresh token
        token_data = response.json()
        access_token = token_data["access_token"]
        refresh_token = token_data.get("refresh_token")

        #retrieve email
        auth_header = {'Authorization': f'Bearer {access_token}'}
        email = requests.get(EMAIL_RETRIEVE_URL, headers=auth_header).json()['email']

        # call airflow dag to initialize user data ingestion
        dag_name = 'download_new_user_data'
        dag_run_url = f'http://webserver:8080/api/v1/dags/{dag_name}/dagRuns'

        body = {
        "dag_run_id": f'load_new_user_{email.split("@")[0]}'
        ,
        "conf": {
            "email": email,
            "access_token": access_token,
            "refresh_token": refresh_token
            }
        }

        response = requests.post(
            dag_run_url,
            auth=('admin', 'admin'), 
            headers={"Content-Type": "application/json"},
            json=body
        )

        return f"Authorization complete. Token saved.{response.status_code}"
    else:
        return f"Error obtaining token: {response.json()}", 400

app.run(host='0.0.0.0', port=8888, debug=True)

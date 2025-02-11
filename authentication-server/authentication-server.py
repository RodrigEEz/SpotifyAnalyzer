from flask import Flask, request, redirect, session
import requests
import urllib.parse
import os
from scripts.save_token import insert_user
import logging

app = Flask(__name__)
app.secret_key = os.environ['FLASK_SECRET_KEY']
CLIENT_ID = os.environ['SPOTIFY_CLIENT_ID']
CLIENT_SECRET = os.environ['SPOTIFY_CLIENT_SECRET']
REDIRECT_URI = os.environ['SPOTIFY_REDIRECT_URI']
SCOPE = "user-read-email"
AUTH_URL = "https://accounts.spotify.com/authorize"
TOKEN_URL = "https://accounts.spotify.com/api/token"
EMAIL_RETRIEVE_URL = "https://api.spotify.com/v1/me"

logging.basicConfig(level=logging.INFO)

@app.route("/")
def home():
    params = {
        "client_id": CLIENT_ID,
        "response_type": "code",
        "redirect_uri": REDIRECT_URI,
        "scope": SCOPE,
    }
    auth_url = f"{AUTH_URL}?{urllib.parse.urlencode(params)}"
    return redirect(auth_url)

@app.route("/callback")
def callback():
    code = request.args.get("code")
    if not code:
        print('error')
        return "No se recibió el código de autorización.", 400

    # Intercambiar el código por un token
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

        # save in database
        AUTH_DB_USER = os.environ['AUTH_DB_USER']
        AUTH_DB_PASSWORD = os.environ['AUTH_DB_PASSWORD']
        AUTH_DB_DATABASE = os.environ['AUTH_DB_DATABASE']
        AUTH_DB_HOST =  os.environ['AUTH_DB_HOST']
        app.logger.info(insert_user(email, access_token, refresh_token, 'users', \
                    AUTH_DB_USER, AUTH_DB_PASSWORD, AUTH_DB_HOST, AUTH_DB_DATABASE))

        return "Autorización completada. Token almacenado."
    else:
        return f"Error al obtener el token: {response.json()}", 400

app.run(host='0.0.0.0', port=8888, debug=True)
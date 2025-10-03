import os
from dotenv import load_dotenv
import requests

load_dotenv()

API_USERNAME = os.getenv("JANELL_API_USERNAME")
API_PASSWORD = os.getenv("JANELL_API_PASSWORD")
API_BASE_URL = os.getenv("JANELL_API_BASE_URL")

def get_token():
    url = f'{API_BASE_URL}/auth'

    credentials = {
        'username': API_USERNAME,
        'password': API_PASSWORD
    }

    response = requests.post(url, data=credentials)
    response.raise_for_status()

    response_json = response.json()
    token = response_json.get('token', None)

    return token

def main():
    token = get_token()

    if token is not None:
        print(f"Successfully retrieved token: {token}")

if __name__ == "__main__":
    main()
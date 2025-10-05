import os
from dotenv import load_dotenv
import requests
import json

load_dotenv()

API_USERNAME = os.getenv("JANELL_API_USERNAME")
API_PASSWORD = os.getenv("JANELL_API_PASSWORD")
API_BASE_URL = os.getenv("JANELL_API_BASE_URL")

def get_token(base_url):
    url = f'{base_url}/auth'

    credentials = {
        'username': API_USERNAME,
        'password': API_PASSWORD
    }

    response = requests.post(url, data=credentials)
    response.raise_for_status()

    response_json = response.json()
    token = response_json.get('token', None)

    return token

def get_people(base_url, token, limit=10, offset=0):
    url = f'{base_url}/people'
    headers = {
        'Authorization': f'Bearer {token}'
    }
    params = {
        'limit': limit,
        'offset': offset
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()

    response_json = response.json()

    people = response_json['data']

    return people

def get_clients(base_url, token, limit=10, offset=0):
    url = f'{base_url}/clients'
    
    headers = {
        'Authorization': f'Bearer {token}'
    }
    
    params = {
        'limit': limit,
        'offset': offset
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()

    response_json = response.json()

    clients = response_json['data']

    return clients

def main():
    base_url = API_BASE_URL
    token = get_token(base_url)

    limit = 10
    offset = 0
    max_records = 100

    if token is not None:
        # people_initial_batch = get_people(base_url, token, limit=limit, offset=offset)
        clients_initial_batch = get_clients(base_url, token, limit=limit, offset=offset)

        # with open('people_data.json', 'w') as file:
        #     json.dump(people_initial_batch, file, indent=2)
        with open('./data/clients_data.json', 'w') as file:
            json.dump(clients_initial_batch, file, indent=2)

        # while offset < max_records:
        #     people_next_batch = get


if __name__ == "__main__":
    main()
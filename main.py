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

def save_sample_json(data, filename):
    with open(f'./data/{filename}', 'w') as file:
        json.dump(data, file, indent=2)

def process_people(base_url, token):
    limit = 10
    offset = 0
    max_records = 100

    people_initial_batch = get_people(base_url, token, limit=limit, offset=offset)
    save_sample_json(people_initial_batch, 'people_sample.json')
    offset += limit

    while offset < max_records:
        people_batch = get_people(base_url, token, limit=limit, offset=offset)
        if not people_batch:
            break
        offset += limit

def process_clients(base_url, token):
    limit = 10
    offset = 0
    max_records = 100

    clients_initial_batch = get_clients(base_url, token, limit=limit, offset=offset)
    save_sample_json(clients_initial_batch, 'clients_sample.json')
    offset += limit

    while offset < max_records:
        clients_batch = get_clients(base_url, token, limit=limit, offset=offset)
        if not clients_batch:
            break
        offset += limit


def main():
    base_url = API_BASE_URL
    token = get_token(base_url)

    if token is not None:
        process_people(base_url, token)
        process_clients(base_url, token)


if __name__ == "__main__":
    main()
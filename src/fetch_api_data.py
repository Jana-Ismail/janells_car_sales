import requests

from settings import (
    API_USERNAME, API_PASSWORD
)

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
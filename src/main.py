import os
import requests
import json
from sqlalchemy import create_engine, MetaData
from sqlalchemy.dialects.postgresql import insert

from alchemy_core import define_people_table, define_clients_table
from settings import (
    API_USERNAME, API_PASSWORD, API_BASE_URL, 
    POSTGRES_USER, POSTGRES_PASSWORD, 
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB
)

def main():
    base_url = API_BASE_URL
    token = get_token(base_url)

    engine = initialize_db_engine()
    metadata = MetaData()
    
    people_table = define_people_table(metadata)
    clients_table = define_clients_table(metadata)
    
    metadata.create_all(engine)

    if token is not None:
        process_people(base_url, token, engine, people_table)
        process_clients(base_url, token, engine, clients_table)

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
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(os.path.dirname(script_dir), 'data')
    filepath = os.path.join(data_dir, filename)
    
    with open(filepath, 'w') as file:
        json.dump(data, file, indent=2)

def process_people(base_url, token, engine, people_table):
    limit = 10
    offset = 0
    max_records = 100
    
    with engine.connect() as conn:
        people_initial_batch = get_people(base_url, token, limit=limit, offset=offset)
        save_sample_json(people_initial_batch, 'people_sample.json')

        insert_statement = insert(people_table).values(people_initial_batch)
        insert_statement = insert_statement.on_conflict_do_update(
            index_elements=['id'],
            set_=dict(
                first_name=insert_statement.excluded.first_name,
                last_name=insert_statement.excluded.last_name,
                email=insert_statement.excluded.email,
                address=insert_statement.excluded.address
            )
        )
        conn.execute(insert_statement)
        conn.commit()
        print(f"Successfully processed initial batch")

        offset += limit

        while offset < max_records:
            people_batch = get_people(base_url, token, limit=limit, offset=offset)
            
            if not people_batch:
                break
            
            try:
                stmt = insert(people_table).values(people_batch)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['id'],
                    set_=dict(
                        first_name=stmt.excluded.first_name,
                        last_name=stmt.excluded.last_name,
                        email=stmt.excluded.email,
                        address=stmt.excluded.address
                    )
                )
                conn.execute(stmt)
                conn.commit()
                print(f"Successfully processed batch at offset {offset}")
            except Exception as e:
                print(f"Error inserting batch at offset {offset}: {e}")
            finally:
                offset += limit

def process_clients(base_url, token, engine, clients_table):
    limit = 10
    offset = 0
    max_records = 100
    
    with engine.connect() as conn:
        clients_initial_batch = get_clients(base_url, token, limit=limit, offset=offset)
        save_sample_json(clients_initial_batch, 'clients_sample.json')
        
        insert_statement = insert(clients_table).values(clients_initial_batch)
        insert_statement = insert_statement.on_conflict_do_update(
            index_elements=['id'],
            set_=dict(
                company=insert_statement.excluded.company,
                name=insert_statement.excluded.name,
                address=insert_statement.excluded.address,
                email=insert_statement.excluded.email,
                phone=insert_statement.excluded.phone,
                sales_rep=insert_statement.excluded.sales_rep
            )
        )
        conn.execute(insert_statement)
        conn.commit()
        print(f"Successfully processed initial clients batch")
        
        offset += limit

        while offset < max_records:
            clients_batch = get_clients(base_url, token, limit=limit, offset=offset)
            if not clients_batch:
                break
            
            try:
                insert_statement = insert(clients_table).values(clients_batch)
                insert_statement = insert_statement.on_conflict_do_update(
                    index_elements=['id'],
                    set_=dict(
                        company=insert_statement.excluded.company,
                        name=insert_statement.excluded.name,
                        address=insert_statement.excluded.address,
                        email=insert_statement.excluded.email,
                        phone=insert_statement.excluded.phone,
                        sales_rep=insert_statement.excluded.sales_rep
                    )
                )
                conn.execute(insert_statement)
                conn.commit()
                print(f"Successfully processed clients batch at offset {offset}")
            except Exception as e:
                print(f"Error inserting clients batch at offset {offset}: {e}")
            finally:
                offset += limit

def initialize_db_engine():
    """Initialize and return a SQLAlchemy engine for PostgreSQL database."""
    engine = create_engine(f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}', echo=True)
    return engine

if __name__ == "__main__":
    main()
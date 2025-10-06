import os
import json
import csv
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from fetch_api_data import get_token, get_people, get_clients
from settings import (
    API_BASE_URL, 
    POSTGRES_USER, POSTGRES_PASSWORD, 
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
    CLIENT_CONTACT_FILE
)
from models import Base
from orm_operations import create_sales_rep, create_clients, create_contact_permissions, update_contact_permissions, create_people

def main():
    engine = initialize_db_engine()
    create_postgres_tables(engine)
    
    base_url = API_BASE_URL
    token = get_token(base_url)

    if token is not None:
        process_people(base_url, token, engine)
        process_clients(base_url, token, engine)

    process_contact_permissions(CLIENT_CONTACT_FILE, engine)

def initialize_db_engine():
    """Initialize and return a SQLAlchemy engine for PostgreSQL database."""
    engine = create_engine(f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}', echo=True)
    return engine

def create_postgres_tables(engine):
    Base.metadata.create_all(engine)

def save_sample_json(data, filename):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(os.path.dirname(script_dir), 'data')
    filepath = os.path.join(data_dir, filename)
    
    with open(filepath, 'w') as file:
        json.dump(data, file, indent=2)

def process_people(base_url, token, engine):
    limit = 10
    offset= 0
    max_records = 100

    while offset < max_records:
        people_batch = get_people(base_url, token, limit=limit, offset=offset)
        
        if not people_batch:
            break

        if offset == 0:
            save_sample_json(people_batch, 'people_sample.json')
    
        with Session(engine) as session:
            try:
                create_people(session, people_batch)
                session.commit()
            except Exception as e:
                session.rollback()
            finally:
                offset += limit

def process_clients(base_url, token, engine):
    limit = 10
    offset = 0
    max_records = 100

    while offset < max_records:
        clients_batch = get_clients(base_url, token, limit=limit, offset=offset)
        
        if not clients_batch:
            break

        if offset == 0:
            save_sample_json(clients_batch, 'clients_sample.json')
    
        with Session(engine) as session:
            try:
                rep_lookup = create_sales_rep(session, clients_batch)
                create_clients(session, clients_batch, rep_lookup)
                create_contact_permissions(session, clients_batch)
                session.commit()
            except Exception as e:
                print(f"Error processing clients at offset {offset}: {e}")
                session.rollback()
            finally:
                offset += limit

def process_contact_permissions(file_path, engine):
    with open(file_path) as file:
        contact_permissions = list(csv.DictReader(file))

    with Session(engine) as session:
        try:
            update_contact_permissions(session, contact_permissions)
            session.commit()
        except Exception as e:
            session.rollback()


if __name__ == "__main__":
    main()
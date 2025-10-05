import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String

load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")

def connect_engine():
    engine = create_engine(f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}', echo=True)
    return engine

def define_people_table(metadata):
    people = Table(
        'people',
        metadata,
        Column('id', String, primary_key=True),
        Column('first_name', String),
        Column('last_name', String),
        Column('email', String),
        Column('address', String)
    )
    return people

def define_clients_table(metadata):
    clients = Table(
        'clients',
        metadata,
        Column('id', String, primary_key=True),
        Column('company', String),
        Column('name', String),
        Column('address', String),
        Column('email', String),
        Column('phone', String),
        Column('sales_rep', String)
    )
    return clients

def create_db_tables(metadata, engine):
    metadata.create_all(engine)

def main():
    engine = connect_engine()
    metadata = MetaData()

    define_people_table(metadata)
    define_clients_table(metadata)

    create_db_tables(metadata, engine)

    
if __name__ == '__main__':
    main()
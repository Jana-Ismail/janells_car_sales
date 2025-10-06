import os
from dotenv import load_dotenv
from fetch_api_data import get_people
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
                statement = insert(people_table).values(people_batch)
                statement = statement.on_conflict_do_update(
                    index_elements=['id'],
                    set_=dict(
                        first_name=statement.excluded.first_name,
                        last_name=statement.excluded.last_name,
                        email=statement.excluded.email,
                        address=statement.excluded.address
                    )
                )
                conn.execute(statement)
                conn.commit()
                print(f"Successfully processed batch at offset {offset}")
            except Exception as e:
                print(f"Error inserting batch at offset {offset}: {e}")
            finally:
                offset += limit

def main():
    engine = connect_engine()
    metadata = MetaData()

    define_people_table(metadata)
    define_clients_table(metadata)

    create_db_tables(metadata, engine)

    
if __name__ == '__main__':
    main()
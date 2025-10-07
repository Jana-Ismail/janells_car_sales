import os
import json
import pandas as pd
import numpy as np
import csv
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from models import Base
from fetch_api_data import get_token, get_people, get_clients
from settings import (
    API_BASE_URL, 
    POSTGRES_USER, 
    POSTGRES_PASSWORD, 
    POSTGRES_HOST, 
    POSTGRES_PORT, 
    POSTGRES_DB,
    CLIENT_CONTACT_FILE,
    DATA_DIR
)
from orm_operations import (
    create_sales_rep, 
    create_clients, 
    create_contact_permissions, 
    update_contact_permissions, 
    create_people,
    join_clients_and_contact_permissions
)

def initialize_db_engine():
    """Initialize and return a SQLAlchemy engine for PostgreSQL database."""
    engine = create_engine(f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')
    return engine

def create_postgres_tables(engine):
    Base.metadata.create_all(engine)

def save_sample_json(data, filename):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(os.path.dirname(script_dir), 'data')
    filepath = os.path.join(data_dir, filename)
    
    with open(filepath, 'w') as file:
        json.dump(data, file, indent=2)

def handle_nulls(data, nulls_values):
    dataframe = pd.DataFrame(data)
    dataframe.replace(nulls_values, np.nan, inplace=True)

    dataframe = dataframe.where(pd.notnull(dataframe), None)

    return dataframe.to_dict(orient='records')

def process_people(base_url, token, engine):
    limit = 10
    offset= 0
    max_records = 100

    while offset < max_records:
        people_batch = get_people(base_url, token, limit=limit, offset=offset)
        
        if not people_batch:
            break
        
        nulls = ['null', 'null', None, 'NULL', 'na', 'N/A', '', ' ']

        people_clean = handle_nulls(people_batch, nulls)

        if offset == 0:
            save_sample_json(people_batch, 'people_sample.json')
    
        with Session(engine) as session:
            try:
                create_people(session, people_clean)
                session.commit()
            except Exception as e:
                print(f"Error processing people at offset {offset}: {e}")
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
        
        nulls = ['null', 'null', None, 'NULL', 'na', 'N/A', '', ' ']
        clients_clean = handle_nulls(clients_batch, nulls)
    
        with Session(engine) as session:
            try:
                rep_lookup = create_sales_rep(session, clients_clean)
                create_clients(session, clients_clean, rep_lookup)
                create_contact_permissions(session, clients_clean)
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

def create_excel_report(engine, output_file):
    with Session(engine) as session:
        try:
            results = join_clients_and_contact_permissions(session)

            clients_with_permissions = pd.DataFrame(
                results,
                columns=[
                    'client_id', 'name', 'company', 'email', 'phone',
                    'can_call', 'can_email'
                ]
            )

            # clients_with_permissions.to_excel(output_file, index=False)
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                clients_with_permissions.to_excel(writer, index=False, sheet_name='ClientContactPermissions')

                worksheet = writer.sheets['ClientContactPermissions']

                column_widths = {
                    'A': 12,
                    'B': 20,
                    'C': 25,
                    'D': 30,
                    'E': 15,
                    'F': 10,
                    'G': 10
                }

                for col, width in column_widths.items():
                    worksheet.column_dimensions[col].width = width
        
        except Exception as e:
            print(f"Error generating Excel report: {e}")
            return
    

def main():
    engine = initialize_db_engine()
    create_postgres_tables(engine)
    
    base_url = API_BASE_URL
    token = get_token(base_url)

    if token is not None:
        process_people(base_url, token, engine)
        process_clients(base_url, token, engine)

    process_contact_permissions(CLIENT_CONTACT_FILE, engine)
    
    excel_file = f'{DATA_DIR}/client_contact_report.xlsx'
    create_excel_report(engine, excel_file)

if __name__ == "__main__":
    main()
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert
from models import SalesRep, Client, Person, ContactPermission

def create_sales_rep(session, clients_data):
    sales_rep_names = {client.get('sales_rep') for client in clients_data if client.get('sales_rep')}

    existing_sales_reps = session.execute(
        select(SalesRep).where(SalesRep.name.in_(sales_rep_names))
    ).scalars().all()

    sales_rep_lookup = {sales_rep.name: sales_rep for sales_rep in existing_sales_reps}

    new_sales_reps = []
    for name in sales_rep_names - sales_rep_lookup.keys():
        sales_rep = SalesRep(name=name)
        session.add(sales_rep)
        new_sales_reps.append(sales_rep)
    
    session.flush()

    for sales_rep in new_sales_reps:
        sales_rep_lookup[sales_rep.name] = sales_rep
    
    return sales_rep_lookup

def create_clients(session, clients_data, sales_rep_lookup):
    table = Client.__table__
    
    client_records = []
    for client in clients_data:
        sales_rep_name = client.get('sales_rep')
        sales_rep_id = sales_rep_lookup.get(sales_rep_name).id if sales_rep_name in sales_rep_lookup else None
    
        client_records.append({
            'id': client.get('id'),
            'name': client.get('name'),
            'company': client.get('company'),
            'address': client.get('address'),
            'email': client.get('email'),
            'phone': client.get('phone'),
            'sales_rep_id': sales_rep_id
        })

    insert_statement = insert(table).values(client_records)
    upsert_statement = insert_statement.on_conflict_do_update(
        index_elements=['id'],  # match on primary key
        set_={
            'name': insert_statement.excluded.name,
            'company': insert_statement.excluded.company,
            'address': insert_statement.excluded.address,
            'email': insert_statement.excluded.email,
            'phone': insert_statement.excluded.phone,
            'sales_rep_id': insert_statement.excluded.sales_rep_id,
        }
    )

    session.execute(upsert_statement)

def create_people(session, people_data):
    people_table = Person.__table__

    person_records = [
        {
            'id': person.get('id'),
            'first_name': person.get('first_name'),
            'last_name': person.get('last_name'),
            'email': person.get('email'),
            'address': person.get('address')
        }
        for person in people_data
    ]

    if not person_records:
        return

    insert_statement = insert(people_table).values(person_records)
    upsert_statement = insert_statement.on_conflict_do_update(
        index_elements=['id'],  # Primary key constraint
        set_={
            'first_name': insert_statement.excluded.first_name,
            'last_name': insert_statement.excluded.last_name,
            'email': insert_statement.excluded.email,
            'address': insert_statement.excluded.address
        }
    )

    session.execute(upsert_statement)

def create_contact_permissions(session, clients_data):
    table = ContactPermission.__table__

    contact_records = [
        {'client_id': contact.get('id'), 'can_call': False, 'can_email': False}
        for contact in clients_data
        if contact.get('id')  # safety check
    ]

    if not contact_records:
        return

    insert_stmt = insert(table).values(contact_records)

    upsert_stmt = insert_stmt.on_conflict_do_nothing(index_elements=['client_id'])

    session.execute(upsert_stmt)



def update_contact_permissions(session, contact_permissions):
    table = ContactPermission.__table__

    contact_records = []
    for contact in contact_permissions:
        client_id = contact.get('id')
        if not client_id:
            continue

        can_call_raw = contact.get('can_call')
        can_email_raw = contact.get('can_email')

        # Normalize truthy values — handle messy CSVs (strings, ints, etc.)
        can_call = str(can_call_raw).strip().lower() in ('1', 'true', 'yes', 'y')
        can_email = str(can_email_raw).strip().lower() in ('1', 'true', 'yes', 'y')

        contact_records.append({
            'client_id': client_id,
            'can_call': can_call,
            'can_email': can_email,
        })

    if not contact_records:
        return

    insert_statement = insert(table).values(contact_records)

    upsert_statement = insert_statement.on_conflict_do_update(
        index_elements=['client_id'],
        set_={
            'can_call': insert_statement.excluded.can_call,
            'can_email': insert_statement.excluded.can_email
        }
    )

    session.execute(upsert_statement)

def join_clients_and_contact_permissions(session):
    join_statement = select(
        Client.id,
        Client.name,
        Client.company,
        Client.email,
        Client.phone,
        ContactPermission.can_call,
        ContactPermission.can_email
    ).join(
        ContactPermission, 
        Client.id == ContactPermission.client_id
    )

    results = session.execute(join_statement).all()
    return results
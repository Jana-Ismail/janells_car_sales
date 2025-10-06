from sqlalchemy import (String, Column, ForeignKey, Boolean)
from sqlalchemy.orm import declarative_base, relationship
import uuid

Base = declarative_base()

class Person(Base):
    __tablename__ = 'people'

    id = Column(String, primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    address = Column(String)
    email = Column(String)

class Client(Base):
    __tablename__ = 'clients'

    id = Column(String, primary_key=True)
    company = Column(String)
    name = Column(String)
    address = Column(String)
    email = Column(String)
    phone = Column(String)
    sales_rep_id = Column(String, ForeignKey('sales_reps.id'), nullable=True)

    sales_rep = relationship('SalesRep', back_populates='clients')

class ContactPermission(Base):
    __tablename__ = 'contact_permissions'

    client_id = Column(String, ForeignKey('clients.id'), primary_key=True)
    can_call = Column(Boolean, default=False)
    can_email = Column(Boolean, default=False)

class SalesRep(Base):
    __tablename__ = 'sales_reps'

    id = Column(String, primary_key=True, default=lambda:str(uuid.uuid4()))
    name = Column(String, nullable=False, unique=True)

    clients = relationship('Client', back_populates='sales_rep')
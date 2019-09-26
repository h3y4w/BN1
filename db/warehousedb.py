from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, Float, Text , Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime, timedelta
import json
import os

Base = declarative_base()

class AmazonCategoryNode (Base):
    __tablename__ = 'AmazonCategoryNode'
    id = Column(Integer, primary_key=True)

    category_node_id = Column(Integer, nullable=False, unique=True)
    category_name = Column(String(100), nullable=False)

    def __init__(self, payload):
        self.category_node_id = payload['id']
        self.category_name = payload['name']


class TicketGroup (Base):
    __tablename__ = 'TicketGroup'
    id = Column(Integer, primary_key=True)

    qr_code_url = Column(String(250), nullable=False)
    confirmation_code = Column(String(150), nullable=False)
    event_date = Column(DateTime, nullable=False)
    cnt = Column(Integer, nullable=False)

    bot_email = Column(String(100), nullable=False)
    purchase_time = Column(DateTime, nullable=False)

    ticket_drop_id = Column(ForeignKey('TicketDrop.id'), nullable=True)
    ticket_order_id = Column(ForeignKey('TicketOrder.id'), nullable=True)
    active = Column(Boolean, default=True)

    def __init__(self, payload):
        self.bot_email = payload['bot_email']
        self.confirmation_code = payload['confirmation_code']
        self.purchase_time = payload['purchase_time']
        self.qr_code_url = payload['qr_code_url']
        self.purchase_time = payload['purchase_time']
        self.event_time = payload['event_time']
        self.cnt = payload['cnt']


class TicketOrder (Base):
    __tablename__ = 'TicketOrder'

    id = Column(Integer, primary_key=True)
    purchase_time = Column(DateTime, nullable=False)
    email = Column(String(100), nullable=False)
    phone_number = Column(String(20))
    tickets = relationship('TicketGroup', backref='drop') #lazy?
    total_amount = Column(Integer, nullable=True)

    def __init__(self, payload):
        self.purchase_time = datetime.utcnow() 
        self.email = payload['email']
        self.total_amount = payload['total_amount']
        
        cols = payload.keys()
        if 'phone_number' in cols:
            self.phone_number = payload['phone_number']
        if 'purchase_time' in cols:
            self.purchase_time = payload['purchase_time']



class PrepaidCardUser (Base):
    __tablename__ = 'PrepaidCardUser'
    id = Column(Integer, primary_key=True)

    first_name = Column(String(25), nullable=False)
    last_name = Column(String(25), nullable=False)

    street_address = Column(String(50), nullable=False)
    zipcode_address = Column(Integer, nullable=False)
    city_address = Column(String(25), nullable=False)
    state_address = Column(String(25), nullable=False)

    number = Column(String(20), nullable=False)
    security_code = Column(String(4), nullable=False)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    balance = Column(Integer, nullable=False)
    active = Column(Boolean, default=True)

    def __init__(self, payload):
        self.number = payload['number']
        self.security_code = payload['security_code']
        self.year = payload['year']
        self.month = payload['month']
        self.balance = payload['balance']

class TicketDropType (Base):
    __tablename__ = 'TicketDropType'
    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)
    days_in_advance = Column(Integer, nullable=False)
    grace_days = Column(Integer, nullable=False)


    def __init__(self, payload):
        self.name = payload['name']
        self.days_in_advance = payload['days_in_advance']
        self.grace_days = payload['grace_days']

class TicketDrop (Base):
    __tablename__ = 'TicketDrop'
    id = Column(Integer, primary_key=True)
    unlock_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)

    ticket_drop_type_id = Column(ForeignKey('TicketDropType.id'), nullable=False)
    price_multiplier = Column(Float, default=1.0)
    tickets = relationship('TicketGroup') #lazy?


    def __init__(self, payload):
        self.unlock_date = payload['unlock_date']
        self.end_date = payload['end_date']
        self.ticket_drop_type_id = payload['ticket_drop_type_id']
        
        cols = payload.keys()
        if 'price_multiplier' in cols:
            self.price_multiplier = payload['price_multiplier']


class ModelTest(Base):
    __tablename__ = 'ModelTest'
    id = Column(Integer, primary_key=True)

    def __init__(self, payload):
        pass


class CryptoChunk (Base):
    __tablename__ = "CryptoChunk"
    id = Column(Integer, primary_key=True)
    buy_price = Column(Float, nullable=False)
    sell_price = Column(Float, nullable=False)
    amount = Column(Float, nullable=False)
    cryptoblock_id = Column(ForeignKey('CryptoBlock.id'), nullable=False)
    date_sold = Column(DateTime, nullable=False)

    def __init__(self):
        self.buy_price = payload['buy_price']
        self.sell_price = payload['sell_price']
        self.date_sold = payload['date_sold']
        self.amount = payload['amount']
        self.cryptoblock_id = payload['cryptoblock_id']


class CryptoBlock (Base):
    __tablename__ = 'CryptoBlock'
    id = Column(Integer, primary_key=True)
    buy_price = Column(Float, nullable=False)
    total_amount = Column(Float, nullable=False)
    date_purchased = Column(DateTime, nullable=False)
    date_completed = Column(DateTime, nullable=True)


    def __init__(self, payload):
        self.buy_price = payload['buy_price']
        self.total_amount = payload['total_amount']
        self.date_purchased = payload['date_purchased'] 

class BinanceApiKey(Base):
    __tablename__ = 'BinanceApiKey'
    id =  Column(Integer, primary_key=True)

    label = Column(String(50), nullable=True)
    key = Column(String(200), nullable=False)
    secret = Column(String(200), nullable=False)
    
    read = Column(Boolean, nullable=False)
    write = Column(Boolean, nullable=False)
    withdraw = Column(Boolean, nullable=False)

    def __init__(self, payload):
        self.key = payload['api_key']
        self.secret = payload['api_secret']

        self.read = payload['read']
        self.write = payload['write']
        self.withdraw = payload['withdraw']

        if 'label' in payload.keys():
            self.label = payload['label']

class RCB (Base):
    __tablename__ = 'RCB'
    id = Column(Integer, primary_key=True)
    invested_amount = Column(Float, nullable=False)

    def __init__(self, payload):
        self.invested_amount = payload['invested_amount']


class RCB_tmp (Base):
    __tablename__ = 'RCB_tmp'
    id = Column(Integer, primary_key=True)
    reference_id = Column(Integer)
    invested_amount = Column(Float, nullable=False)


    def __init__(self, payload):
        self.invested_amount = payload['invested_amount']
        if 'reference_id' in payload.keys():
            self.reference_id = payload['reference_id']


from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, Float, Text , Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import json
import os

Base = declarative_base()

class Scraper_Currency (Base):
    __tablename__ = 'Scraper_Currency'
    id = Column(Integer, primary_key=True)
    name = Column('name', String(20), nullable=False)
    symbol = Column('symbol', String(5))
    code = Column('code', String(10), nullable=False)

    def __init__(self, payload):
        self.name = payload['name']
        self.symbol = payload['symbol']
        self.code = payload['code']

class Scraper_BalstaItem (Base):
    __tablename__ = 'Scraper_BalstaItem'
    id = Column(Integer, primary_key=True)
    site_item_id = Column('site_item_id', Integer, nullable=False)
    name = Column('name', String(40), nullable=False)
    end_date = Column('end_date', DateTime, nullable=False)
    est_worth = Column('est_worth', Float, nullable=False)
    currency_id = Column(Integer, ForeignKey('Scraper_Currency.id'), nullable=False) 

    def __init__(self, payload):
        self.site_item_id = payload['site_item_id']
        self.name = payload['name']
        self.end_date = payload['end_date']
        self.currency_id = payload['currency_id']
        self.est_worth = payload['est_worth']

class Scraper_BalstaItemBid (Base):
    __tablename__ = 'Scraper_BalstaItemBid'
    id = Column(Integer, primary_key=True)
    item_id = Column(Integer, ForeignKey('Scraper_BalstaItem.id'), nullable=False)
    currency_id = Column(Integer, ForeignKey('Scraper_Currency.id'), nullable=False) 
    bid_price = Column('bid_price', Float, nullable=False)
    time = Column('time', DateTime)

    def __init__(self, payload):
        self.item_id = payload['item_id']
        self.currency_id = payload['currency_id']
        self.bid_price = payload['bid_price']
        self.time = payload['time']


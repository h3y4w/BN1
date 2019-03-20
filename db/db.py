from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, Float, Text , Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import json
import os

class DB (object):
    def __init__(self, file_, base, sql_type, create=False):
        args = None 
        engine_name = None
        if sql_type == 'mysql':
            engine_name = sql_type + '+pymysql://'
        elif sql_type == 'sqlite':
            engine_name = 'sqlite:///'

        engine_name = engine_name + file_

        self.file = file_
        self.engine = create_engine(engine_name)
        self.Session = sessionmaker(bind=self.engine)

        self.session = self.Session()

        if create:
            print "Creating tables..."
            base.metadata.create_all(self.engine)
            self.session.commit()

    def get_all(self, model):
        return self.session.query(model).all()

    def find_by_id(self, model, id_):
        return self.session.query(model).filter_by(id=id_).first()

    def add(self, model, payload):
        m = model(payload)
        self.session.add(m)
        self.session.commit()
        return m

    def delete(self, m):
        pass

    def to_dict(self, m, cols, serialize=False):
        d = {}
        for col in cols:
            value = getattr(m, col)
            if serialize:
                try:
                    json.dumps([value])
                except:
                    value = str(value)
            d[col] = value
        return d


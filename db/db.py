from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, Float, Text , Boolean, text
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

    def query_raw(self, raw):
        sql = text(raw)
        result = self.session.execute(sql)
        data = []
        for row in result:
            data.append(dict(row))
        return data

    def get_tables(self):
        pass
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
        self.session.delete(m)
        self.session.commit()

    def as_dict(self, model, cols=[]):
        if not cols:
            cols = [col.name for col in model.__table__.columns]

        return {col: getattr(model, col) for col in cols}

    def as_json(self, model, cols=[]):
        m = self.as_dict(model, cols)
        return self.to_json(m)
        
    def to_json(self, dic):
        d = {}
        for col in dic.keys():
            val = dic[col]
            try:
                if type(val) in [str, int, float]:
                    pass
                json.dumps([val])
            except:
                val = str(val)
            d[col] = val
        return d

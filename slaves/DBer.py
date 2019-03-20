from bot import Slave
import db
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime


class DBer(Slave):
    target_mapping = None
    engine = None
    Session = None

    table_mapping = {
        'BalstaItem': db.BalstaItem,
        'BasltaItemBid': db.BalstaItemBid 
    }

    def set_target_mapping(self):
        self.target_mapping = {
            'insert_into': self.insert_into,
            'delete_from': self.delete_from
        }

    def __init__(self, payload):
        super(DBer, self).__init__(payload)
        self.engine = create_engine('sqlite:///fucku.db')
        self.Session = sessionmaker(bind=self.engine)
        self.set_target_mapping()

    def insert_into(self, payload):
        session = self.Session()
        model = self.table_mapping[payload['table']](payload['data'])
        session.add(model)
        session.commit()
        session.close()

    def delete_from(self, payload):
        session = self.Session()
        session.commit()
        session.close()

slave_payload = {
    'master_id': 0,
    'config': {
        'id': 0,
        'type': 'slave'
    }
}

slave = DBer(slave_payload)

table_payload = {
    'table': 'BalstaItem',
    'data': {
        'site_item_id': 2546,
        'name': 'Silver Watch',
        'end_date': datetime.utcnow()
    }
}

slave.insert_into(table_payload)

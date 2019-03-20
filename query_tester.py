from db import db
from db import masterdb
from sqlalchemy import and_

master_db = db.DB('test/master.db', masterdb.Base, 'sqlite', create=False)

'''
uuids = master_db.session.query(masterdb.SlaveType)\
        .filter(
            #masterdb.SlaveType=='CPV1'
        ).join(
                masterdb.SlaveType,
                and_(masterdb.SlaveType=='CPV1', masterdb.Slave.slave_type_id==masterdb.SlaveType.id)\
        )\
        .all()

        #.filter( masterdb.Slave.active==True)\
'''

print uuids

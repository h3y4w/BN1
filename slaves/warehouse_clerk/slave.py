# -*- coding: utf-8 -*-
import json
import traceback
import os
from drivers.slavedriver import SlaveDriver
import time
from datetime import datetime
from utils.mpinbox import create_local_task_message, INBOX_SYS_MSG, INBOX_TASK1_MSG, OUTBOX_SYS_MSG, OUTBOX_TASK_MSG


from db import warehousedb
from db import db


class WarehouseClerk(SlaveDriver):
    model_id = 'WCV1'
    def __init__(self, config):
        super(WarehouseClerk, self).__init__(config)
        self.add_command_mappings({
            'bd.sd.@WCV1.echo': self.echo,


            'bd.sd.@WCV1.query': self.bd_sd_WCV1_query,

            'bd.sd.@WCV1.check.table.tmp': self.bd_sd_WCV1_check_table_tmp,

            'bd.sd.@WCV1.model.insert': self.bd_sd_WCV1_model_insert,
            'bd.sd.@WCV1.model.update': self.bd_sd_WCV1_model_update,
            'bd.sd.@WCV1.model.remove': self.bd_sd_WCV1_model_remove,

            'bd.sd.@WCV1.models.insert': None,
            'bd.sd.@WCV1.models.update': None,
            'bd.sd.@WCV1.models.remove': None,

            'bd.sd.@WCV1.models.get.CPV1': self.bd_sd_WCV1_models_get_CPV1

        })

        self.warehouse_db = db.DB(config['warehousedb'], warehousedb.Base, config['db_engine'], create=True)

    def echo(self, data, route_meta):
        print 'WarehouseClerk.echo > data: {}'.format(data)

    def get_model_from_table_name(self, table_name):
        model = getattr(warehousedb, table_name)
        if not model:
            raise ValueError("Table name {} is not in database".format(table_name))

        return model

    def bd_sd_WCV1_check_table_tmp (self, data, route_meta):
        tmp_table = self.get_model_from_table_name(data['table_name'])
        table_name = data['table_name'].split('_tmp')[0] #cuts out _tmp post-fix
        table = self.get_model_from_table_name(table_name)
        rows = self.warehouse_db.session.query(tmp_table).all()
        reference_rows = []
        for row in rows:
            if (row.reference_id):
                print "REFERENCE ROW ID {} ADDED".format(row.reference_id)
                reference_rows.append(row)
            else:
                info = self.warehouse_db.as_dict(row)
                print 'info: {}'.format(info)
                obj = self.warehouse_db.add(table, info)
                if obj:
                    self.warehouse_db.delete(row)
                else:
                    raise ValueError("Receievd None when adding obj from tmp to main table")

        if reference_rows:
            cols = reference_rows[0].__table__.columns.keys()
            #rows = self.warehouse_db.session.query(table).filter(table.id==)
            ids = [row.reference_id for row in reference_rows]

            rows = self.warehouse_db.session.query(table).filter(table.id.in_(ids)).all() #searches for rows in real table with reference ids from tmp table
            for row in rows:
                length = len(reference_rows)
                for i in xrange(0, length):
                    if row.id == reference_rows[i].reference_id:
                        ref = reference_rows.pop(i) 
                        if i!=0: 
                            i-=1 #account for pop
                        length = len(reference_rows)
                        for col in cols:
                            if col != 'reference_id':
                                setattr(row, col, getattr(ref, col))

                        self.warehouse_db.delete(ref)
                        self.warehouse_db.session.commit()

            for row in reference_rows: #deletes all other reference updates that didnt match"
                self.warehouse_db.delete(row)
                self.warehouse_db.session.commit()



    def bd_sd_WCV1_query(self, data, route_meta):
        query = data['query']
        tag = data['tag']
        queried = self.warehouse_db.query_raw(query)
        tdata = {'queried': queried}
        self.taskrunner_send_data_to_tag(tag, tdata)
    
    def bd_sd_WCV1_models_get_CPV1(self, data, route_meta):
        model = self.get_model_from_table_name(data['obj_type'])

        sid = data['sid']
        uuid = data['uuid']
        cnt = data.get('page_index')
        per = data.get('per_page')
        
        if not cnt:
            cnt = 0
        if not per:
            per = 30

        obj_type = data['obj_type']
        obj_args = data.get('obj_args')

        if obj_args:
            obj_id = obj_args.get('id')
        

        #args = [getattr(obj_class, key)==value for key, value in obj_args.items()]
        #.filter(*args)\

        objs = self.warehouse_db.session.query(model)\
                .offset(cnt)\
                .limit(per)\
                .all()

        dicts = [self.warehouse_db.as_json(o) for o in objs]
        out = {
            'action': 'data.rows.set',
            'action_data': dicts,
            'session_id': sid,
            'uuid': uuid,
        }
        msg = create_local_task_message (
            'bd.@md.Slave.CPV1.notify',
            out
        )
        self.send_message_to_master(msg)

    def bd_sd_WCV1_model_insert(self, data, route_meta):
        #Add insert options here like
        model = self.get_model_from_table_name(data['table_name'])
        args = data['args']

        ids = []
        for arg in args:
            a = self.warehouse_db.add(model, arg)
            if a:
                ids.append(a.id)

        if 'tag' in data.keys():
            tag = data['tag']
            self.taskrunner_send_data_to_tag(tag, ids)

        if '_tmp' in data['table_name']:
            payload = {'table_name': data['table_name']}
            self.add_global_task('bd.sd.@WCV1.check.table.tmp', payload, 'Copying rows from {} to permanent storage'.format(data['table_name']), job_id=route_meta['job_id'])


    def bd_sd_WCV1_model_remove(self, data, route_meta):
        model = self.get_model_from_table_name(data['table_name'])
        args = data['args']
        for arg in args:
            m = self.warehouse_db.session.query(model)\
                    .filter(arg['id'])\
                    .first()
            if m:
                self.warehouse_db.delete(m)


    def bd_sd_WCV1_model_update(self, data, route_meta):
        models_ex = {
            'overwrite': True,
            'table_name': 'HYIPPrices', 
            'args': {'id': 3, 'price': 5}
        }

        model = self.get_model_from_table_name(data['table_name'])
        args = data['args']
        overwrite = False
        if 'overwrite' in data.keys():
            overwrite = data['overwrite']

        for arg in args:
            m = self.warehouse_db.session.query(model)\
                    .filter(model.id==arg['id'])\
                    .first()
            
            if m:
                m_id = arg['id']
                del arg['id']
                for key in arg.keys():
                    v = None
                    try:
                        v = getattr(m, key)
                    except Exception as e:
                        raise ValueError(e)
                    except:
                        raise ValueError("Table '{}' doesn't have key '{}'".format(data['table_name'], key))

                    else:
                        if v != None  and not overwrite:
                            continue

                        setattr(m, key, arg[key])
                        m.updated = datetime.utcnow()
        self.warehouse_db.session.commit()



# -*- coding: utf-8 -*-
import json
import traceback
import os
from bot import SlaveDriver, OUTBOX_TASK_MSG
from selenium import webdriver
import time
from datetime import datetime
from mpinbox import create_local_task_message, INBOX_SYS_MSG, INBOX_TASK1_MSG, OUTBOX_SYS_MSG, OUTBOX_TASK_MSG


from db import warehousedb
from db import db


class WarehouseClerk(SlaveDriver):
    model_id = 'WCV1'
    def __init__(self, config):
        super(WarehouseClerk, self).__init__(config)
        self.add_command_mappings({
            'bd.sd.@WCV1.echo': self.echo,


            'bd.sd.@WCV1.query': self.bd_sd_WCV1_query,

            'bd.sd.@WCV1.model.insert': self.bd_sd_WCV1_model_insert,
            'bd.sd.@WCV1.model.update': self.bd_sd_WCV1_model_update,
            'bd.sd.@WCV1.model.remove': self.bd_sd_WCV1_model_remove,

            'bd.sd.@WCV1.models.insert': None,
            'bd.sd.@WCV1.models.update': None,
            'bd.sd.@WCV1.models.remove': None

        })

        db_fn = os.path.join(self.__exc_dir__, 'warehouse.db')
        self.warehouse_db = db.DB(db_fn, warehousedb.Base, 'sqlite', create=True)

    def echo(self, data, route_meta):
        print 'WarehouseClerk.echo > data: {}'.format(data)


    def get_model_from_table_name(self, table_name):
        model = getattr(warehousedb, table_name)
        if not model:
            raise ValueError("Table name {} is not in database".format(table_name))

        return model

    def bd_sd_WCV1_query(self, data, route_meta):
        query = data['query']
        tag = data['tag']
        queried = self.warehouse_db.query_raw(query)
        tdata = {'queried': queried}
        self.taskrunner_send_data_to_tag(tag, tdata)

    def bd_sd_WCV1_model_insert(self, data, route_meta):
        model = self.get_model_from_table_name(data['table_name'])
        args = data['args']
        for arg in args:
            a = self.warehouse_db.add(model, arg)


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



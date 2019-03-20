from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, Float, Text , Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime, timedelta
import json
import os

Base = declarative_base()

class SlaveTaskGroup (Base):
    __tablename__ = "SlaveTaskGroup"
    id = Column(Integer, primary_key=True)
    job_id = Column(ForeignKey('SlaveTaskGroup.id'))
    completed_cnt = Column(Integer, default=0)
    error_cnt = Column(Integer, default=0)
    total_cnt = Column(Integer, nullable=False)

    def __init__(self, payload):
        self.total_cnt = payload['total_cnt']
        self.job_id = payload['job_id']

class Scheduler(Base):
    __tablename__ = 'Scheduler'
    id = Column(Integer, primary_key=True)
    frequency_min = Column(Integer) # repeats every x minutes 
    run_time = Column(DateTime)
    route = Column(String(100))
    data = Column(Text, default='{}')

    def __init__(self, payload):
        cols = payload.keys()
        if 'frequency_min' in cols:
            self.frequency_min = payload['frequency_min']
        if not 'run_time' in cols:
            self.run_time = datetime.utcnow() + timedelta(minutes=self.frequency_min)

        self.route = payload['route']
        if type(payload['data']) == type({}):
            payload['data'] = json.dumps(payload['data'])
        self.data = payload['data']

class Slave (Base):
    __tablename__ = 'Slave'
    id = Column(Integer, primary_key=True)
    slave_type_id = Column(ForeignKey('SlaveType.id'))
    last_pulse = Column(DateTime)
    is_ec2 = Column(Boolean)
    active = Column(Boolean)
    uuid = Column(String(36))
    tasks = relationship('SlaveTask')

    def __init__(self, payload):
        if 'slave_type_id' in payload.keys():
            self.slave_type_id = payload['slave_type_id']
        self.is_ec2 = payload['is_ec2']
        self.uuid = payload['uuid']
        self.active = True

    def free(self):
        self.active = False

    def pulse(self):
        self.last_pulse = datetime.utcnow()

class SlaveType (Base):
    __tablename__ = 'SlaveType'
    id = Column(Integer, primary_key=True)
    model_id = Column(String(10), nullable=False)
    module_url = Column(String(200))
    name = Column(String(15))

    def __init__(self, payload):
        self.name = payload['name']
        if 'module_url' in payload.keys():
            self.module_url = payload['module_url']
        self.model_id = payload['model_id']

class SlaveTaskChainer (Base):
    __tablename__ = 'SlaveTaskChainer'
    id = Column(Integer, primary_key=True)

    parent_task_id = Column(ForeignKey('SlaveTask.id'), nullable=False)
    parent_task = relationship("SlaveTask", foreign_keys=[parent_task_id])

    child_task_id = Column(ForeignKey('SlaveTask.id'), nullable=False)
    child_task = relationship("SlaveTask", foreign_keys=[child_task_id])

    run_child_with_error = Column(Boolean, default=False)
    chained = Column(Boolean, nullable=True) #if the following task was executed = true , if it fails = false 
    #args = Column(Text, default='{}')


    def __init__(self, payload):
        self.parent_task_id = payload['parent_task_id']
        self.child_task_id = payload['child_task_id']

        cols = payload.keys()

        if 'run_child_with_error' in cols:
            self.run_child_with_error = payload['run_child_with_error']

        #self.args = payload['args']

class SlaveJob (Base):
    __tablename__ = 'SlaveJob'
    id = Column(Integer, primary_key=True)
    route = Column(String(100), nullable=True)
    data = Column(Text, default='{}')

    name = Column(String(50))
    completed = Column(Boolean, default=False)
    error = Column(Boolean, default=False)
    msg = Column(String(100))
    stage = Column(String(100), default="Creating...")
    tasks = relationship("SlaveTask")

    def __init__(self, payload):
        self.name = payload['name']
        cols = payload.keys()
        if 'stage' in cols:
            self.stage = payload['stage']



class SlaveTask (Base):
    __tablename__ = 'SlaveTask'

    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    job_id = Column(ForeignKey('SlaveJob.id'))

    task_group_id = Column(ForeignKey('SlaveTaskGroup.id'), nullable=True)

    active = Column(Boolean, default=True) #if task is active for processing (can be false when task chainer has been chained)

    started = Column(Boolean, default=False) 
    completed = Column(Boolean, default=False)

    msg = Column(String(100))
    error = Column(Boolean, default=False)


    time_created = Column(DateTime, nullable=False)
    time_started = Column(DateTime, nullable=True)
    time_completed = Column(DateTime, nullable=True)

    route = Column(String(100))
    data = Column(Text, default='{}')
    slave_type_id = Column(ForeignKey('SlaveType.id'), nullable=False)
    assigned_slave_id = Column(ForeignKey('Slave.id'))
    assigned_slave = relationship('Slave')

    def __init__(self, payload):
        print "PAYLOAD: {}".format(payload)
        self.time_created = datetime.utcnow()
        self.slave_type_id = payload['slave_type_id']
        self.name = payload['name']
        if type(payload['data']) == type({}):
            payload['data'] = json.dumps(payload['data'])
        self.data = payload['data']
        self.route = payload['route']
    
        cols = payload.keys()
        if 'job_id' in cols:
            self.job_id = payload['job_id']

        if 'task_group_id' in cols:
            self.task_group_id = payload['task_group_id']

    def is_error(self, msg=None):
        self.error = True
        self.msg = msg

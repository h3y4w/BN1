import dill
import multiprocessing #as multiprocessing
#from Queue import PriorityQueue, Queue
import Queue
from datetime import datetime, timedelta
from time import sleep
import os
import json
import uuid

import traceback
from mpinbox import create_local_task_message, INBOX_SYS_MSG, INBOX_TASK1_MSG, INBOX_TASK2_MSG, INBOX_BLOCKING_MSG, INBOX_SYS_BLOCKING_MSG, OUTBOX_SYS_MSG, OUTBOX_TASK_MSG

from mpinbox import MPPriorityQueue, create_local_task_message 
from heartbeat import Heartbeat, Pulse

from comms.client import BotClientFactory
from comms.server import BotServerFactory
from twisted.internet import reactor, protocol, task
from twisted.internet.endpoints import TCP4ClientEndpoint
from db import db
from db import masterdb
from sqlalchemy import func, desc
from db import warehouse
#from db import db


import importlib
from operator import itemgetter
driver = None
def prnt(reason):
    print reason
    print reason.printTraceback()

def dead_process_cb(data):
    driver.dead_system_process_relaunch(data)




class BotDriver(object):
    INBOX_PRIORITIES = 5
    OUTBOX_PRIORITIES = 2

    inbox = None
    outbox = None

    bot_route_header = "bd"
    msg_id = 0
    init_start_task_funcs = []
    is_running = False

    outbox_cb = None
    bot_cmd_running = False

    __bd_command_mappings = {} #store default botdriver command maps
    command_mappings = {} #this stores a combined list of all maps

    def __init__(self, config):
        print "Init BotDriver"
        self.__bd_command_mappings = {
            '@bd.echo': self.bd_echo,
            '@bd.comms.launch': self.bd_comms_launch,

            '@bd.process.kill': self.bd_process_kill,
            '@bd.watchdog.launch': self.bd_watchdog_launch,

            '@bd.instance.free': self.bd_instance_free 
        }

        self.heartbeat = Heartbeat(grace=10)
        self.uuid = str(uuid.uuid1())


        self.max_running_instances = 2
        self.running_instances = 1

        self.add_command_mappings(self.__bd_command_mappings)

        msg = create_local_task_message('@bd.watchdog.launch', {'cb': dead_process_cb}, {'type': 'process'})
        wd_task = lambda: self.inbox.put(msg, INBOX_SYS_MSG)

        self.init_start_task_funcs.append(wd_task)

        msg2 = create_local_task_message('@bd.comms.launch',{}, {'type': 'process'})
        comms_task = lambda: self.inbox.put(msg2, INBOX_SYS_MSG)

        self.init_start_task_funcs.append(comms_task)

        global driver
        driver = self


    def send_msg(self, msg, priority):
        self.outbox.put(msg, priority)

    def func_execute(self, func, data, route_meta):
        func(data, route_meta)
        msg = create_local_task_message('@bd.instance.free', {})
        self.inbox.put(msg, INBOX_SYS_MSG)

    @staticmethod
    def create_global_task_group_message(route, data, name, job_id, route_meta=None):
        if not route_meta:
            route_meta = {
                'type': 'default'
            }
        msg = {
            'route': 'bd.@md.slave.task.group.add',
            'data': data, 
            'route_meta': route_meta
        }
        msg['data']['route'] = route
        msg['data']['name'] = name
        msg['data']['job_id'] = job_id
        return msg

    @staticmethod
    def create_global_task_message(route, data, name, parent_task_id=None, task_group_id=None, job_id=None, route_meta=None):
        if not route_meta:
            route_meta = {
                'type': 'default'
            }

        msg = {
            'route': 'bd.@md.slave.task.add',
            'data': {
                'task_data': {
                    'route': route,
                    'data': data,
                    'name': name
                }
            },
            'route_meta': route_meta
        }
        if task_group_id:
            msg['data']['task_data']['task_group_id'] = task_group_id

        if job_id:
            msg['data']['task_data']['job_id'] = job_id

        if parent_task_id:
            msg['data']['task_chainer'] = {}
            msg['data']['task_chainer']['parent_task_id'] = parent_task_id

        return msg

    def add_local_task(self, route, args, priority=INBOX_TASK1_MSG, route_meta=None):
        msg = create_local_task_message(route, args, route_meta)

        self.inbox.put(msg, priority)

    def router_process_launch(self, func, data, route_meta, daemon=False):
        process = multiprocessing.Process(target=self.func_execute, args=(func, data, route_meta))

        process.daemon = daemon
        process.start()
        return process

    def dead_system_process_relaunch(self, data):
        pass
        #if data['type'] in ['comms']:

        #    self.add_local_task('@bd.comms.launch', relaunch_args, INBOX_SYS_MSG, {'type': 'process', 'daemon': True})


    def get_model_id_from_route(self, route):
        model_id = route[route.find('@')+1:].split('.')[0]
        return model_id

    def bd_echo(self, data, route_meta):
        print "__\n<BotDriver Echo>\n{}\n___".format(data)

    def bd_comms_launch(self, data, route_meta):
        self.comms()

    def bd_instance_free(self, data, route_meta):
        self.running_instances = self.running_instances - 1
        print 'Freeing instance... now: {}'.format(self.running_instances)


    def bd_process_launch(self, data, route_meta):
        #DO STUFF HERE WITHIN DATA FOR PROCESSING OPTion
        daemon = True
        if 'daemon' in data['config'].keys():
            daemon = data['config']['daemon']

        print "function bd_process_launch executed {} pid {}".format(data['rdata']['route'], os.getpid())
        process = self.process_launch(self.command_mappings[data['rdata']['route']], data=data['rdata']['data'], daemon=daemon)


    def bd_process_kill(self, data, route_meta):
        raise NotImplemented



    def bd_watchdog_launch(self, data, route_meta):
        self.heartbeat.watchdog(data['cb'])


    def add_command_mappings (self, maps):
        self.command_mappings.update(maps) 

    def init_command_mappings (self, maps=None):
        self.command_mappings = self.__bd_command_mappings
        if maps:
            self.add_command_mappings(maps)


    def init_start (self):
        for task_func in self.init_start_task_funcs:
            task_func()


    def init_outbox_callback(self):
        if self.outbox_cb:
            self.outbox_cb.stop()

        self.outbox_cb = task.LoopingCall(self.check_outbox)
        ob = self.outbox_cb.start(.1, now=True)
        ob.addErrback(prnt)

    def init_mailbox(self, manager):
        self.inbox = MPPriorityQueue(self.INBOX_PRIORITIES, manager=manager)
        self.outbox = MPPriorityQueue(self.OUTBOX_PRIORITIES, manager=manager)
        self.init_outbox_callback()


    def check_outbox(self):
        msg = self.outbox.get()
        if (msg):
            print "___\n<BotDriver Outbox {}>\n{}\n___".format(os.getpid(), msg)
            self.factory.send_it(msg)


    def router(self, route, data, route_meta):
        error = True
        msg = 'Uncaught Error'
        try:
            resp = self.command_mappings[route](data, route_meta)

        except KeyError as e:
            msg = "Most likely routing error: {}".format(e)
        except Exception as e:
            msg = str(e)
            print traceback.print_exc()

        else:
            error = False
            msg = None
        
        return error, msg

    def router_msg_process(self, msg):
        route_meta = {'type': 'default'}
        if 'route_meta' in msg.keys():
            route_meta = msg['route_meta']

            cols = route_meta.keys()
            if not 'type' in cols:
                route_meta['type'] = 'default'

        route_meta['route'] = msg['route']

        if msg['route'] == 'bd.@sd.task.global.start': #checks to see if global task 
    #        if msg['data']['route'].find('#') != -1:
            if self.running_instances < self.max_running_instances:
                print "Creating instance (DOESNT REALLY MANAGE INSTANCES ACCURATELY)"
                self.running_instances +=1
                func = self.command_mappings[msg['route']]
                self.router_process_launch(func, msg['data'],route_meta)
                return

            else:
                print self.running_instances
                print '--'
                self.inbox.put(msg, INBOX_BLOCKING_MSG)
                #print 'Not enough resources to create instance... waiting'
                return
       
        if route_meta['type'] == 'process':
            daemon = False
            func = self.command_mappings[msg['route']]

            if 'daemon' in route_meta.keys():
                daemon = route_meta['daemon']
            self.router_process_launch(func, msg['data'],route_meta, daemon=daemon)
            return

        resp = self.router(msg['route'], msg['data'], route_meta)
        print "RESP: {}".format(resp)

        pass
    def check_inbox(self):
        msg = self.inbox.get()
        if (msg):
            self.msg_id+=1
            now = datetime.utcnow().strftime("%m/%d/%y %H:%M:%S")
            print "_________________________________________\n<BotDriver Inbox {} msgid {} {}>\n{}\n____________________________".format(os.getpid(), self.msg_id, now, msg)


            if (type(msg) != type({})):
                print "ABOVE MSG IS CORRUPTED OR NOT FORMATTED\n-------------------------\n"
                return

            self.router_msg_process(msg)


    def start(self):
        with multiprocessing.Manager() as manager:
            self.manager = manager
            print "Main BotDriver Pid: {}".format(os.getpid())
            self.init_mailbox(manager)
            
            self.init_start()

            while True:
                self.loop()

class SlaveDriver (BotDriver):
    master_server_ip = None 
    master_server_port = None 
    def __init__(self, config):
        self.master_server_ip = config['master_server_ip']
        self.master_server_port = config['master_server_port']

        super(SlaveDriver, self).__init__(config)
        self.bot_route_header = self.bot_route_header + ".sd.@" + self.model_id


        maps = {
            'bd.@sd.slave.auth': self.bd_sd_slave_auth,

            'bd.@sd.task.global.start': self.bd_sd_task_global_start,
            'bd.@sd.task.global.add': self.bd_sd_task_global_add
        }
        self.add_command_mappings(maps)


    def comms(self):
        self.comms_on = True
        pid = os.getpid()
        self.comms_pid = pid

        print 'Starting SlaveDriver Comms'
        self.heartbeat.__track_process__(pid, type_='comms', name='SlaveDriver Comms', route='@bd.comms.launch')
        self.comms_pulse_cb = task.LoopingCall(self.heartbeat.send_pulse, pid=pid)

        cp = self.comms_pulse_cb.start(5)
        cp.addErrback(prnt)

        self.factory = BotClientFactory()
        self.factory.set_driver(self)

        reactor.connectTCP(self.master_server_ip, self.master_server_port, self.factory)
        reactor.run()


    def add_global_task(self, route, data, name, parent_task_id=None, task_group_id=None, job_id=None, route_meta=None):
        msg = BotDriver.create_global_task_message(route, data, name, parent_task_id, task_group_id, job_id, route_meta=route_meta)
        self.outbox.put(msg, OUTBOX_SYS_MSG)
    
    def add_global_task_group(self, route, data, name, job_id, route_meta=None):
        msg = self.create_global_task_group_message(route, data, name, job_id, route_meta=route_meta)
        self.outbox.put(msg, OUTBOX_SYS_MSG)

    def bd_sd_slave_auth(self, data, route_meta):
        print "Sending Authentication info to Master"
        msg = create_local_task_message(
            'bd.@md.slave.register',
            {
                'uuid': self.uuid,
                'model_id': self.model_id
            }
        )
        self.send_msg(msg, OUTBOX_SYS_MSG)

    def bd_sd_task_global_start(self, data, route_meta): #THIS FUNCTION STARTS ALL GLOBALLY TRACKED TASKS
        task_id = route_meta["task_id"]
        print 'Starting Global Task id {}'.format(task_id)
        route = data['route']
        data = data['data']
        
        task_msg = create_local_task_message(
            'bd.@md.slave.task.started',
            {
                'task_id':task_id,
                'time_started': str(datetime.utcnow())
            }
        )
        self.send_msg(task_msg, OUTBOX_TASK_MSG)
        err, msg = self.router(route, json.loads(data), route_meta)

        task_msg1 = None
        if err:
            task_msg1 = create_local_task_message(
                'bd.@md.slave.task.error',
                {
                    'task_id': task_id,
                    'msg': msg
                }
            )
            print 'Error Global Task id {}'.format(task_id)

        else:
            task_msg1 = create_local_task_message(
                'bd.@md.slave.task.completed',
                {
                    'task_id': task_id,
                    'time_completed': str(datetime.utcnow())
                }
            )
            print 'Completed Global Task id {}'.format(task_id)
        self.send_msg(task_msg1, OUTBOX_TASK_MSG)


        if route.find('#') != -1: #checks to see if its a dynamic task
            self.add_local_task('@bd.instance.free', {}, INBOX_SYS_MSG)

    def bd_sd_task_global_add(self, data, route_meta):

        msg = create_local_task_message(
            'bd.@sd.task.global.start', 
            data,
            route_meta
        )
        self.inbox.put(msg, INBOX_TASK1_MSG)

    def bd_sd_task_remove(self, data, route_meta):
        pass

    def loop(self):
        self.check_inbox()

class MasterDriver(BotDriver):
    def __init__(self, config):
        ### SET MASTERDRIVER SETTINGS FROM CONFIG HERE
        self.__exc_dir__ = config['exc_dir']
        self.port = config['server_port'] 
        self.host = config['server_host']
        super(MasterDriver, self).__init__(config)

        self.bot_route_header = self.bot_route_header + ".md.@" + self.model_id

        print 'Init MasterDriver'
        maps = {
            'bd.@md.slave.job.add': self.bd_md_slave_job_add,
            'bd.@md.slave.job.remove': self.bd_md_slave_job_remove,
            'bd.@md.slave.job.stage': self.bd_md_slave_job_stage,
            'bd.@md.slave.job.completed': self.bd_md_slave_job_completed,
            'bd.@md.slave.job.error': self.bd_md_slave_job_error,

            'bd.@md.slave.task.add': self.bd_md_slave_task_add,
            'bd.@md.slave.task.remove': self.bd_md_slave_task_remove,
            'bd.@md.slave.task.error': self.bd_md_slave_task_error,
            'bd.@md.slave.task.started': self.bd_md_slave_task_started,
            'bd.@md.slave.task.completed': self.bd_md_slave_task_completed,

            'bd.@md.slave.task.group.add': self.bd_md_slave_task_group_add,

            'bd.@md.slave.task.schedule.add': self.bd_md_slave_task_schedule_add,
            'bd.@md.slave.task.schedule.remove': self.bd_md_slave_task_schedule_remove,

            'bd.@md.slave.connect': self.bd_md_slave_connect,
            'bd.@md.slave.register': self.bd_md_slave_register,
            'bd.@md.slave.lost': self.bd_md_slave_lost,
            'bd.@md.slave.launch': None,
            'bd.@md.slave.free': None,


            'bd.@md.Slave.APV1.forwarded': self.bd_md_Slave_APV1_forwarded,
            'bd.@md.Slave.CPV1.ping': self.bd_md_Slave_CPV1_ping,
        }
        self.add_command_mappings(maps)
        self.slave_tasks = MPPriorityQueue(2)
        
        self.CPV1_grace_minutes = 120 #change this later
        self.CPV1_pinged_times = {} 

        args = []
        db_fn = self.__exc_dir__ + '/master.db'
        try:
            os.remove(db_fn) #added for deebugging
        except:
            pass

        if not os.path.exists(db_fn):
            args = [
                {'name': 'AccessPoint', 'model_id': 'APV1'},
                {'name': 'Command Portal', 'model_id': 'CPV1'},
                {'name': 'Scraper', 'model_id': 'SCV1'}
            ]
        self.master_db = db.DB(db_fn, masterdb.Base, 'sqlite', create=True)
        for a in args:
            self.master_db.add(masterdb.SlaveType, a)
        

    def send_message(self, uuid, msg, priority=OUTBOX_TASK_MSG):
        print "MASTER_DRIVER SENDING MSG TO: {} (priority is hardcoded)".format(uuid)
        print "msg: {}\n-----------------------------".format(msg)
        payload = {'uuid': uuid, 'data': msg}
        self.outbox.put(payload, priority) 

    def check_schedule(self):
        schedulers = self.master_db.session.query(masterdb.Scheduler)\
                .filter(
                    masterdb.Scheduler.run_time<=datetime.utcnow()
                ).all()
        for scheduler in schedulers:
            bot_header = self.get_model_id_from_route(scheduler.route)

            if len(scheduler.route.split('md')) >= 2: #this is a master function
                self.add_local_task(scheduler.route, json.loads(scheduler.data), INBOX_SYS_MSG)
            else: 
                print 'Launching Schedule task {}'.format(scheduler.route)
                self.inbox.put(createGlobalTask(scheduler.route, scheduler.data), INBOX_SYS_MSG)
            if scheduler.frequency_min:
                scheduler.run_time = scheduler.run_time + timedelta(minutes=scheduler.frequency_min) #generates next stop time
                self.master_db.session.commit()

    def send_global_task(self, uuid, route, data, task_id, job_id=None, route_meta={}):
        msg = {
            'route_meta': {
                'task_id': task_id,
                'job_id': job_id
            },
            'route': 'bd.@sd.task.global.add',
            'data': {
                'route': route,
                'data': data
            }
        } 
        self.send_message(uuid, msg)

        #polls database for scheduler
    def check_slave_chainers(self):
        chainers = self.master_db.session.query(masterdb.SlaveTaskChainer)\
                .filter(
                    masterdb.SlaveTaskChainer.chained==None 
                ).all()

        if len(chainers):
            for chainer in chainers:
                chained = False
                if chainer.parent_task.completed == True:
                    #run child task
                    chainer.child_task.active = True
                    chained = True

                elif chainer.parent_task.error:
                    if chainer.run_child_with_error:
                        chainer.child_task.active = True
                        chained = True
                        #run child_task
                if chained:
                    chainer.chained = True #maybe make this within the child tasks running
            self.master_db.session.commit()#maybe make this after every assignments so its faster?

    def check_slave_jobs(self):
        jobs = self.master_db.session.query(masterdb.SlaveJob)\
                .filter(
                    masterdb.SlaveJob.started==False
                ).all()

        if len(jobs):
            pass

    def check_slave_tasks(self):
        # add priority
        tasks = self.master_db.session.query(masterdb.SlaveTask)\
                 .filter(
                     masterdb.SlaveTask.assigned_slave_id.is_(None)
                 ).all()

        if len(tasks):
            slave_type_needed = []
            for task in tasks:
                slave_type_needed.append(task.slave_type_id)

            slaves = self.master_db.session.query(masterdb.Slave, func.count(masterdb.SlaveTask.id).label('cnt'))\
                    .filter(
                        masterdb.Slave.active==True,
                        masterdb.Slave.slave_type_id.in_(slave_type_needed))\
                    .outerjoin(masterdb.SlaveTask)\
                    .group_by(masterdb.Slave)\
                    .order_by(desc('cnt'))\
                    .all()

            if not len(slaves):
                print "Slave type {} not in network.  Please launch slave to execute tasks {}".format(slave_type_needed, tasks[0].route)
                return

            slave_list = {}
            for i in xrange(0, len(slaves)):
                slave = slaves[i][0]
                ctasks = slaves[i][1] #current tasks

                key = str(slave.slave_type_id)
                if not key in slave_list.keys():
                    slave_list[key] = [] 

                slave_list[key].append({'slave':slave, 'ctasks': ctasks, 'slave_id': slave.id, 'slave_type_id': slave.slave_type_id})

            for task in tasks:
                key = str(task.slave_type_id)
                if len(slave_list[key]):
                    slave_meta = slave_list[key].pop()
                    slave = slave_meta['slave']

                    task.assigned_slave_id = slave.id
                    self.master_db.session.commit()
                    
                    self.send_global_task(slave.uuid, task.route, task.data, task.id, job_id=task.job_id)

    def bd_md_slave_task_status_check(self, data, route_meta):
        #updates task as it moves through the stages
        pass

    def bd_md_slave_task_cleanup(self, data, route_meta):
        #tasks after a certain time are removed from the database 
        pass

    def loop(self):
        self.check_inbox()
        self.check_slave_tasks()
        self.check_slave_chainers()
        self.check_schedule()

    def comms(self):
        self.comms_on = True
        pid = os.getpid()
        self.comms_pid = pid
        self.heartbeat.__track_process__(pid, type_='comms', name='MasterDriver Comms')
        print 'Starting Master Comms'

        self.factory = BotServerFactory(self)
        reactor.listenTCP(self.port, self.factory)
        self.comms_pulse_cb = task.LoopingCall(self.heartbeat.send_pulse, pid=pid)
        cp = self.comms_pulse_cb.start(5)
        cp.addErrback(prnt)
        reactor.run()

    def notify_CPV1(self, action, data):

        expire_time = datetime.utcnow() + timedelta(minutes=self.CPV1_grace_minutes)
        uuids = self.master_db.session.query(masterdb.SlaveType.id, masterdb.Slave.uuid)\
                .filter(
                    masterdb.SlaveType.model_id=='CPV1'
                ).join(
                        masterdb.Slave,
                        masterdb.Slave.slave_type_id==masterdb.SlaveType.id
                ).filter( masterdb.Slave.active==True)\
                .all()

        stored = self.CPV1_pinged_times.keys()
        
        for uuid in uuids:
            uuid = uuid[1] #second col is uuid, first col is slavetype id 
            if not uuid in stored:
                self.CPV1_pinged_times[uuid] = datetime.utcnow()

            if self.CPV1_pinged_times[uuid] < expire_time: #less than means expired 
                data = {'action_data': data, 'action': action}
                msg = create_local_task_message('bd.sd.@CPV1.data.send', data)
                self.send_message(uuid, msg, OUTBOX_SYS_MSG)

            else:
                print "GRACE PERIOD PASSED"
    def __slave_task_add(self, data):
        model_id = self.get_model_id_from_route(data['route'])

        if not model_id in ['bd', 'md', 'sd']: #to prevent adding driver sys tasks 
            slave_type_id = self.master_db.session.query(masterdb.SlaveType.id)\
                    .filter(masterdb.SlaveType.model_id==model_id)\
                    .first()

            if slave_type_id:
                slave_type_id = slave_type_id[0]

                data["slave_type_id"] = slave_type_id

                return self.master_db.add(masterdb.SlaveTask, data) 
                

    def bd_md_slave_job_add(self, data, route_meta):
        job = self.master_db.add(masterdb.SlaveJob, data['job_data'])

        job_cols = [
            'id',
            'stage',
            'msg',
            'name',
            'error',
            'completed']
        obj = self.master_db.to_dict(job, job_cols)
        print "\n\nCALLING NOTIFY {}".format(obj)
        self.notify_CPV1('job.add', obj)
        route_meta['job_id'] = job.id


        if 'tasks_data' in data.keys():
            tasks = []
            task_cols = ['id', 'completed', 'active', 'error', 'msg', 'job_id', 'name', 'time_started']

            tasks_notify_data = []
            for task in data['tasks_data']:
                task['job_id'] = job.id
                task_obj = self.__slave_task_add(task)
                d = self.master_db.to_dict(task_obj, task_cols)
                tasks_notify_data.append(d)
            self.notify_CPV1('tasks.add', tasks_notify_data)

    def bd_md_slave_job_remove(self, data, route_meta):
        raise NotImplemented

    def bd_md_slave_job_stage(self, data, route_meta):
        job = self.master_db.session.query(masterdb.SlaveJob)\
                .filter(masterdb.SlaveJob.id==data['job_id'])\
                .first()
        if job:
            job.stage = data['stage']
            if 'msg' in data.keys():
                job.msg = data['msg']

            self.master_db.session.commit()

            job_cols = [
                'id',
                'stage',
                'msg',
                'name',
                'error',
                'completed']
            j = self.master_db.to_dict(job, job_cols)
            self.notify_CPV1('job.add', j)

    def bd_md_slave_job_error(self, data, route_meta):
        job = self.master_db.session.query(masterdb.SlaveJob)\
                .filter(masterdb.SlaveTask.id==data['job_id'])\
                .first()
        if job:
            job.error = True
            job.msg = data['msg']
            self.master_db.session.commit()

            job_cols = [
                'id',
                'stage',
                'msg',
                'name',
                'error',
                'completed']
            j = self.master_db.to_dict(job, job_cols)
            self.notify_CPV1('job.add', j)


    def bd_md_slave_job_completed(self, data, route_meta):
        job = self.master_db.session.query(masterdb.SlaveJob)\
                .filter(masterdb.SlaveJob.id==data['job_id'])\
                .first()
        if job:
            job.completed = True
            if 'msg' not in data.keys():
                data['msg'] = 'Done!'
                job.msg = data['msg']
            self.master_db.session.commit()
            job_cols = [
                'id',
                'stage',
                'msg',
                'name',
                'error',
                'completed']
            j = self.master_db.to_dict(job, job_cols)
            self.notify_CPV1('job.add', j)


    def bd_md_slave_task_add(self, data, route_meta):
        print '\nbd.md.slave.task.add {}'.format(data)

        model_id = self.get_model_id_from_route(data['task_data']['route'])

        if not model_id in ['bd', 'md', 'sd']: #to prevent adding driver sys tasks 
            slave_type_id = self.master_db.session.query(masterdb.SlaveType.id)\
                    .filter(masterdb.SlaveType.model_id==model_id)\
                    .first()

            if slave_type_id:
                slave_type_id = slave_type_id[0]

                task_args = {
                    "slave_type_id": slave_type_id
                }
                task_args.update(data['task_data'])

                #cols = data['task_data'].keys()

                #if 'job_id' in cols:
                #    task_args['job_id'] = data['task_data']['job_id']


                chaining = False
                cols = data.keys()
                if 'task_chainer' in cols:
                    chaining = True
                    task_args['active'] = False

                if 'job' in cols:
                    if 'job_id' in data['job'].keys():
                        task_args['job_id'] = data['job']['job_id']

                    else:
                        job = self.master_db.add(masterdb.SlaveJob, data['job'])
                        task_args['job_id'] = job.id
                    
                task = self.master_db.add(masterdb.SlaveTask, task_args) 
                
                if task and chaining :
                    chainer_args = {
                        'parent_task_id': data['task_chainer']['parent_task_id'],
                        'child_task_id': task.id,
                        'error_task_id': None,
                        'run_child_with_error': None
                    }

                    cols = data['task_chainer'].keys()

                    if 'error_task_id' in cols:
                        chainer_args['error_task_id'] = data['task_chainer']['error_task_id']

                    if 'run_child_with_error' in cols:
                        chainer_args['run_child_with_error'] = data['task_chainer']['run_child_with_error']

                    chainer = self.master_db.add(masterdb.SlaveTaskChainer, chainer_args)
                   
    def bd_md_slave_task_group_add(self, data, route_meta):
        cnt = len(data['tasks']) 

        task_group_args = {'total_cnt': cnt, 'job_id': data['job_id']}

        task_group = self.master_db.add(masterdb.SlaveTaskGroup, task_group_args)

        task_cols = ['id', 'completed', 'active', 'error', 'msg', 'job_id', 'name', 'time_started']

        for task_data in data['tasks']:
            task_args = {}
            task_args['data'] = task_data

            task_args['job_id'] = data['job_id']
            task_args['name'] = data['name']
            task_args['route'] = data['route']
            task_args['task_group_id'] = task_group.id 
            task = self.__slave_task_add(task_args)

            t = self.master_db.to_dict(task, task_cols, serialize=True)
            self.notify_CPV1('task.add', t)


    def bd_md_slave_task_remove(self, data, route_meta):
        raise NotImplemented

    def bd_md_slave_task_started(self, data, route_meta):
        task = self.master_db.session.query(masterdb.SlaveTask)\
                .filter(masterdb.SlaveTask.id==data['task_id'])\
                .first()
        if not task: 
            raise ValueError("Task id doesnt exist")

        task.started = True
        task.time_started = datetime.strptime(data['time_started'], "%Y-%m-%d %H:%M:%S.%f")

        self.master_db.session.commit()
    
    def bd_md_slave_task_error(self, data, route_meta):
        task = self.master_db.session.query(masterdb.SlaveTask)\
                .filter(masterdb.SlaveTask.id==data['task_id'])\
                .first()

        task.error = True
        task.msg = data['msg']

        if task.task_group_id:
            task_group = self.master_db.session.query(masterdb.SlaveTaskGroup)\
                    .filter(masterdb.SlaveTaskGroup.id == task.task_group_id)\
                    .first()
            if task_group:
                task_group.error_cnt = task_group.error_cnt + 1
                task_group_cols = ['total_cnt', 'complete_cnt', 'error_cnt']
                tg = self.master_db.to_dict(task_group, task_group_cols)
                self.notify_CPV1('taskgroup.add', tg)

        task_cols = ['id', 'completed', 'active', 'error', 'msg', 'job_id', 'name', 'time_started']

        t = self.master_db.to_dict(task, task_cols, serialize=True)
        self.master_db.session.commit()
        self.notify_CPV1('task.add', t)

    def bd_md_slave_task_completed(self, data, route_meta):
        try:
            task = self.master_db.session.query(masterdb.SlaveTask)\
                    .filter(masterdb.SlaveTask.id==data['task_id'])\
                    .first()

            if not task:
                raise ValueError("Task id doesn't exist")

            task.completed = True
            task.time_completed = datetime.strptime(data['time_completed'], "%Y-%m-%d %H:%M:%S.%f")
            if task.task_group_id:
                task_group = self.master_db.session.query(masterdb.SlaveTaskGroup)\
                        .filter(masterdb.SlaveTaskGroup.id == task.task_group_id)\
                        .first()
                task_group.completed_cnt = task_group.completed_cnt + 1

            if 'msg' in data.keys():
                task.msg = data['msg']
            self.master_db.session.commit()
            task_cols = ['id', 'completed', 'active', 'error', 'msg', 'job_id', 'name', 'time_started']
            t = self.master_db.to_dict(task, task_cols, serialize=True)
            self.notify_CPV1('task.add', t)
        except Exception as e:
            print e

    def bd_md_slave_task_schedule_add(self, data, route_meta):
        print 'Adding schedul for {}'.format(data['scheduler']['route'])
        self.master_db.add(masterdb.Scheduler, data['scheduler'])

    def bd_md_slave_task_schedule_remove(self, data, route_meta):
        raise NotImplemented

    def bd_md_slave_connect(self, data, route_meta):
        print "SLAVE {} IS TRYING TO CONNECT".format(data['uuid'])
        s = self.master_db.session.query(masterdb.Slave).filter(masterdb.Slave.uuid==data['uuid']).first()
        if not s: #if slave is not registered, it asks for authentication
            msg = create_local_task_message(
                'bd.@sd.slave.auth',
                {},
                OUTBOX_SYS_MSG
            )
            self.send_message(data['uuid'], msg)
        else:
            print "Resuming previous session with slave {}".format(data['uuid'])

    def bd_md_slave_register(self, data, route_meta):
        slave_type = self.master_db.session.query(masterdb.SlaveType)\
                .filter(masterdb.SlaveType.model_id==data['model_id'])\
                .first()
        if slave_type:
            data['slave_type_id'] = slave_type.id
        else:
            raise ValueError("MODEL ID {} NOT IN SLAVE TYPE TABLE".format(data['model_id']))

        data['is_ec2'] = False
        print "Registered Slave Type {} id {}".format(slave_type.name, slave_type.id)

        slave_model = self.master_db.add(masterdb.Slave, data)
        print 'SLAVES AUTOMATICALLY SET AS NON EC2 instances'


    def bd_md_slave_lost(self, data, route_meta):
        slave = self.master_db.session.query(masterdb.Slave)\
                .filter(masterdb.Slave.uuid==data['uuid'])\
                .first()
        if slave:
            slave.active = False
            self.master_db.session.commit()

    def bd_md_Slave_APV1_forwarded(self, data, route_meta):
        data = data['data'] #the original data has priority
        self.router_msg_process(data)

    def bd_md_Slave_CPV1_ping(self, data, route_meta):
        #checks to see when the last time CPV1 checked in
        #to know whether or not to forward messages to CPV1
        self.CPV1_pinged_times[data['uuid']] = datetime.utcnow()

    def cmd_slaves_launch(self, args):
        args['is_ec2'] = True
        slave_model = self.master_db.add(masterdb.Slave, args)

    def cmd_slaves_free(self, args):
        slave_model = self.master_db.find_by_id(masterdb.Slave, args['slave_id'])
        slave_model.free()


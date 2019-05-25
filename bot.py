import dill
import multiprocessing #as multiprocessing
#from Queue import PriorityQueue, Queue
import Queue
from datetime import datetime, timedelta
from time import sleep
import os
import sys
import signal
import json
import uuid

import traceback
from mpinbox import create_local_task_message, INBOX_SYS_MSG, INBOX_TASK1_MSG, INBOX_TASK2_MSG, INBOX_BLOCKING_MSG, INBOX_SYS_CRITICAL_MSG, OUTBOX_SYS_MSG, OUTBOX_TASK_MSG

from mpinbox import MPPriorityQueue, create_local_task_message 
from heartbeat import Heartbeat, Pulse

from comms.client import BotClientFactory
from comms.server import BotServerFactory
from twisted.internet import reactor, protocol, task
from twisted.internet.endpoints import TCP4ClientEndpoint
from db import db
from db import masterdb
from sqlalchemy import func, desc, and_, or_
from db import warehouse
#from db import db


import importlib
from operator import itemgetter
driver = None
def prnt(reason):
    #USE THIS FUNCTION FOR FILE LOGGING
    print reason
    print reason.printTraceback()

def dead_process_cb(route, data):
    if route:
        print "\n*************************PROCESS DIED LAUNCHED: {}\n\n".format(route)
        driver.add_local_task(route, data, INBOX_SYS_MSG, {'type': 'process'})


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

    
    KILL_BOT = False

    factory = None #comms factory
    RUNNING_GLOBAL_TASK = False
    taskrunner_idc = 0
    task_id = 0
    job_id = 0

    def __init__(self, config):
        self.__exc_dir__ = config['exc_dir']

        print "Init BotDriver"
        self.__bd_command_mappings = {
            '@bd.echo': self.bd_echo,
            '@bd.comms.launch': self.bd_comms_launch,

            '@bd.process.kill': self.bd_process_kill,
            '@bd.watchdog.launch': self.bd_watchdog_launch,
            '@bd.heartbeat.pulse.send': self.bd_heartbeat_pulse_send,

            '@bd.die': self.bd_die,

            '@bd.error':self.bd_error,

            '@bd.instance.free': self.bd_instance_free,

            '@bd.taskrunner.inbox.add': self.bd_taskrunner_inbox_add
        }

        self.uuid = str(uuid.uuid1())

        self.msg_timers = [] 

        self.heartbeat_pulse_interval = 10

        self.running_instance = False

        self.add_command_mappings(self.__bd_command_mappings)

        msg = create_local_task_message('@bd.watchdog.launch', {}, {'type': 'process', 'daemon': True})
        wd_task = lambda: self.inbox.put(msg, INBOX_SYS_CRITICAL_MSG)
        
        self.init_start_task_funcs.append(wd_task)

        msg1 = create_local_task_message('@bd.heartbeat.pulse.send', {}) 
        pulse_task = lambda: self.inbox.put(msg1, INBOX_SYS_MSG)

        self.init_start_task_funcs.append(pulse_task)

        msg2 = create_local_task_message('@bd.comms.launch',{}, {'type': 'process'})
        comms_task = lambda: self.inbox.put(msg2, INBOX_SYS_MSG)

        self.init_start_task_funcs.append(comms_task)

        global driver
        driver = self

    def report_error(self, error_type, error_msg, kill=False):
        data = {
            'type': error_type,
            'msg': error_msg,
            'die': kill
        }
        self.add_local_task('@bd.error', data, INBOX_SYS_CRITICAL_MSG)


    def send_msg(self, msg, priority):
        self.outbox.put(msg, priority)

    def send_message_to_master(self, msg, priority=OUTBOX_TASK_MSG):
        self.outbox.put(msg, priortiy)

    def send_message_to(self, uuid, msg, priority=OUTBOX_TASK_MSG):
        data = {'uuid': uuid, 'data': msg}
        out = create_local_task_message('bd.@md.bot.message.send', data)
        self.outbox.put(out, priority)

    def split_array(self, arr, size):
        arrs = []
        while len(arr) > size:
            pice = arr[:size]
            arrs.append(pice)
            arr   = arr[size:]
        arrs.append(arr)
        return arrs

    def func_execute(self, func, data, route_meta):
        #ASsign global task instance variables
        self.RUNNING_GLOBAL_TASK = True
        keys = route_meta.keys()
        if 'task_id' in keys:
            self.task_id = route_meta['task_id']
            
            if 'job_id' in keys:
                self.job_id = route_meta['job_id']

        func(data, route_meta)
        msg = create_local_task_message('@bd.instance.free', {})
        self.inbox.put(msg, INBOX_SYS_MSG)

    @staticmethod
    def create_global_task_group_message(route, data, name, job_id, job_ok_on_error=False, route_meta=None):
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
    def create_global_task_message(route, data, name, parent_task_id=None, task_group_id=None, job_id=None, job_ok_on_error=False, route_meta=None):
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

        if job_ok_on_error:
            msg['data']['task_data']['job_ok_on_error'] = True

        if parent_task_id:
            msg['data']['task_chainer'] = {}
            msg['data']['task_chainer']['parent_task_id'] = parent_task_id

        return msg

    def add_local_task(self, route, data, priority=INBOX_TASK1_MSG, route_meta=None):
        msg = create_local_task_message(route, data, route_meta)
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

    def bd_die(self, data, route_meta):
        #send death message data['msg'] to master (if possible)
        #self.KILL_BOT = True
        print '\n\n\nBot is dieing...(actually not come to bot.py:def bd_die)'
        keys = data.keys()
        dmsg = 'Bot Killed'
        if dmsg in keys:
            dmsg = data['dmsg'] 

        print '\n\n' + dmsg
        self.heartbeat.KILLALL()
        sys.exit(0)


    def bd_error(self, data, route_meta):
        keys = data.keys()

        error_msg = 'Error type: {} -> {}'.format(data['type'], data['msg'])

        if 'die' in keys and data['die']:
            self.bd_die({'dmsg': error_msg}, route_meta)

        print "\n\n\nERROR\n--------\n{}\n\n".format(error_msg)
        
    def bd_comms_launch(self, data, route_meta):
        self.comms()

    def bd_instance_free(self, data, route_meta):
        self.running_instance = False 
        print 'Freeing instance... now'


    def bd_process_launch(self, data, route_meta):
        #DO STUFF HERE WITHIN DATA FOR PROCESSING OPTion
        daemon = False 
        if 'daemon' in data['config'].keys():
            daemon = data['config']['daemon']

        print "function bd_process_launch executed {} pid {}".format(data['rdata']['route'], os.getpid())
        process = self.process_launch(self.command_mappings[data['rdata']['route']], data=data['rdata']['data'], daemon=daemon)


    def bd_process_kill(self, data, route_meta):
        print "BotDriver Killing process {}".format(data['pid'])
        os.kill(data['pid'], signal.SIGTERM)
        killed, stat = os.waitpid(data['pid'], os.WNOHANG)
        print "Process was Killed: {}".format(killed)


    def bd_watchdog_launch(self, data, route_meta):
        self.heartbeat.watchdog()


    def taskrunner_inbox_add(self, tag, tdata):
        msg = {
            'tag': tag,
            'data': tdata
        }
        self.taskrunner_inbox.put(msg, 0)
        print 'Taskrunner adding tag "{}" data'.format(tag)
        
    def bd_taskrunner_inbox_add(self, data, route_meta):
        self.taskrunner_inbox_add(data['tag'], data['tdata'])
   
    def bd_heartbeat_pulse_send(self, data, route_meta):
        self.heartbeat.send_pulse(os.getpid())
        timer = datetime.utcnow() + timedelta(seconds=self.heartbeat_pulse_interval)
        self.add_local_task('@bd.heartbeat.pulse.send', {}, INBOX_SYS_CRITICAL_MSG, {'timer':timer})

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
        self.heartbeat_inbox = MPPriorityQueue(1, manager=manager)
        self.taskrunner_inbox = MPPriorityQueue(1, manager=manager)

        self.init_outbox_callback()


    def check_outbox(self):
        msg = self.outbox.get()
        if (msg):
            print "___\n<BotDriver Outbox {}>\n{}\n___".format(os.getpid(), msg)
            if self.factory:
                self.factory.send_it(msg)
            else:
                self.add_local_task('@bd.die', {}, INBOX_SYS_CRITICAL_MSG)
                self.report_error('bd.comms', 'BotDriver Comms factory is null: {}'.format(msg['route']), kill=True)


    def router(self, route, data, route_meta):
        error = True
        msg = 'Uncaught Error'
        try:
            resp = self.command_mappings[route](data, route_meta)

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
            if not self.running_instance:
                print "Creating instance (DOESNT REALLY MANAGE INSTANCES ACCURATELY)"
                self.running_instance = True
                func = self.command_mappings[msg['route']]
                self.router_process_launch(func, msg['data'],route_meta)
                return

            else:
                self.inbox.put(msg, INBOX_BLOCKING_MSG)
                #print 'Not enough resources to create instance... waiting'
                return
       
        if 'timer' in route_meta.keys():
            self.msg_timers.append(msg)
            return

        elif route_meta['type'] == 'process':
            func = self.command_mappings[msg['route']]
            daemon = False
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
            now = datetime.utcnow().strftime("%m/%d/%y %H:%M:%S.%f")
            print "_________________________________________\n<BotDriver Inbox {} msgid {} {}>\n{}\n____________________________".format(os.getpid(), self.msg_id, now, msg)


            if (type(msg) != type({})):
                print "ABOVE MSG IS CORRUPTED OR NOT FORMATTED\n-------------------------\n"
                return

            self.router_msg_process(msg)

    def check_msg_timers(self):
        deleted = False
        for i in xrange(0, len(self.msg_timers)):
            if deleted:
                i = i-1
                deleted = False
            msg = None
            try:
                msg = self.msg_timers[i]
            except IndexError:
                pass
            else:
                if datetime.utcnow() > msg['route_meta']['timer']:
                    print "Timer RINGGGED: {}".format(msg['route'])
                    del msg['route_meta']['timer'] 
                    p = INBOX_SYS_MSG
                    if msg['route'] == '@bd.heartbeat.pulse.send':
                        p = INBOX_SYS_CRITICAL_MSG

                    self.inbox.put(msg, p)
                    del self.msg_timers[i]
                    deleted = True

                else:
                    deleted = False


    def taskrunner_send_data_to_tag(self, tag, data, uuid=None):
        if not uuid:
            uuid = self.taskrunner_address_tag_get_uuid(tag)


        if uuid and uuid != self.uuid:
            msg = create_local_task_message('@bd.taskrunner.inbox.add', {'tag': tag, 'tdata': data})
            self.send_message_to(uuid, msg)
            return

        self.taskrunner_inbox_add(tag, data)

    def taskrunner_inbox_get_tag(self, tag, poll=1, delay=.25, get_all=False):
        for i in xrange(0, poll):
            if i: #delay after first poll
                sleep(delay)

            f_msg = None #first msg
            msg = 1
            msgs = [] 

            while (msg != None and msg != f_msg):
                msg = self.taskrunner_inbox.get()
                if (msg):
                    if msg['tag'] == tag:
                        msgs.append(msg['data'])
                        if not get_all:
                            return msgs[0]
                    else:
                        if not f_msg: #sets as first message looked at
                           f_msg = msg 
                        self.taskrunner_inbox.put(msg, 0) #adds back un needed tag data back to queue
            if msgs:
                return msgs
        return msgs

    @staticmethod
    def taskrunner_address_tag_get_uuid(tag):
        try:
            if len(tag) > 3:
                if tag[0:3] == '###':
                    return tag.split('@')[1]
        except:
            pass

    def taskrunner_create_address_tag(self):
        #in the future when task id is assigned to current self, this func can be created by local tasks
        #using random generater based on time rather
        self.taskrunner_idc +=1
        return '###{}.{}@{}'.format(self.task_id, self.taskrunner_idc, self.uuid) 

    def query_WCV1(self, query, tag, job_id=None, name='Querying warehouse db'):
        self.add_global_task('bd.sd.@WCV1.query', {'query': query, 'tag': tag}, name, job_id=job_id)

    def insert_WCV1(self, table_name, arg, job_id=None):
        args = None
        if arg:
            if type(arg) == list:
                args = arg

            else:
                args = [arg]

            name = "Inserting {} rows into warehouse db".format(len(args))
            payload = {'table_name': table_name, 'args':args}
            self.add_global_task('bd.sd.@WCV1.query', payload, name, job_id=job_id)

    def start(self):
        with multiprocessing.Manager() as manager:
            self.manager = manager
            pid = os.getpid()
            print "Main BotDriver Pid: {}".format(pid)
            self.init_mailbox(manager)
            

            self.heartbeat = Heartbeat(self.heartbeat_inbox, self.inbox, grace=25, dead_cb=dead_process_cb, parent_pid=os.getpid())
            
            self.heartbeat.__track_process__(pid, name='BotDriver Main')


            self.init_start()

            while True and not self.KILL_BOT:
                self.loop()

            #RIGHT HERE IS WHERE WATCHDOG RUNS THROUGH BOTS RUNNING PROCESSES AND PURGES

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
        self.heartbeat.__track_process__(pid, name='SlaveDriver Comms', route='@bd.comms.launch')
        self.comms_pulse_cb = task.LoopingCall(self.heartbeat.send_pulse, pid=pid)

        cp = self.comms_pulse_cb.start(5)
        cp.addErrback(prnt)

        self.factory = BotClientFactory()
        self.factory.set_driver(self)

        reactor.connectTCP(self.master_server_ip, self.master_server_port, self.factory)
        reactor.run()


    def add_global_task(self, route, data, name, parent_task_id=None, task_group_id=None, job_id=None, job_ok_on_error=False, route_meta=None):
        if self.RUNNING_GLOBAL_TASK:
            if not job_id:
                job_id = self.job_id

        msg = BotDriver.create_global_task_message(route, data, name, parent_task_id, 
                                                   task_group_id, job_id, job_ok_on_error, 
                                                   route_meta=route_meta)
        self.outbox.put(msg, OUTBOX_SYS_MSG)
    
    def add_global_task_group(self, route, data, name, job_id, job_ok_on_error=False, route_meta=None):
        if self.RUNNING_GLOBAL_TASK:
            if not job_id:
                job_id = self.job_id

        msg = self.create_global_task_group_message(route, data, name, job_id, 
                                                    job_ok_on_error, route_meta=route_meta)
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
        self.check_msg_timers()


class MasterDriver(BotDriver):


    def __init__(self, config):
        ### SET MASTERDRIVER SETTINGS FROM CONFIG HERE
        self.port = config['server_port'] 
        self.host = config['server_host']
        super(MasterDriver, self).__init__(config)

        self.bot_route_header = self.bot_route_header + ".md.@" + self.model_id

        print 'Init MasterDriver'
        maps = {
            'bd.@md.bot.message.send': self.bd_md_bot_message_send,
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

            'bd.@md.slave.list': None,

            'bd.@md.Slave.APV1.forwarded': self.bd_md_Slave_APV1_forwarded,
            'bd.@md.Slave.CPV1.forwarded': self.bd_md_Slave_CPV1_forwarded,
            'bd.@md.Slave.CPV1.ping': self.bd_md_Slave_CPV1_ping,
            'bd.@md.Slave.CPV1.sendall': self.bd_md_Slave_CPV1_sendall

        }
        self.add_command_mappings(maps)
        self.slave_tasks = MPPriorityQueue(2)
        
        self.CPV1_grace_minutes = 120 #change this later
        self.CPV1_pinged_times = {} 

        args = []
        db_fn = os.path.join(self.__exc_dir__, 'master.db')
        try:
            os.remove(db_fn) #added for deebugging
        except:
            pass

        if not os.path.exists(db_fn):
            args = [
                {'name': 'AccessPoint', 'model_id': 'APV1'},
                {'name': 'Command Portal', 'model_id': 'CPV1'},
                {'name': 'Scraper', 'model_id': 'SCV1'},
                {'name': 'Warehouse Clerk', 'model_id': 'WCV1'},
                {'name': 'Binance Trader', 'model_id': 'BTV1'}
            ]
        self.master_db = db.DB(db_fn, masterdb.Base, 'sqlite', create=True)
        for a in args:
            self.master_db.add(masterdb.SlaveType, a)
        



    def send_message_to(self, uuid, msg, priority=OUTBOX_TASK_MSG):
        payload = {'uuid': uuid, 'data': msg}
        self.outbox.put(payload, priority)


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
        route_meta.update({'task_id':task_id, 'job_id':job_id})
        msg = {
            'route_meta': route_meta,
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

    def check_slave_task_groups(self):
        task_groups = self.master_db.session.query(masterdb.SlaveTaskGroup)\
                .filter(
                    masterdb.SlaveTaskGroup.grouped==False
                ).all()

        if len(task_groups):
            for task_group in task_groups:
                total_cnt = task_group.error_cnt + task_group.completed_cnt
                if total_cnt == task_group.total_cnt:
                    task_args = {}
                    task_args['name'] = 'Task Group Handler'
                    task_args['route'] = task_group.cb_route 
                    task_args['data'] = {
                        'total_cnt': total_cnt,
                        'error_cnt': task_group.error_cnt,
                        'completed_cnt': task_group.completed_cnt,
                        'job_id': task_group.job_id
                    }
                    task = self.__slave_task_add(task_args)
                    if task:
                        task_group.grouped = True 

             
            self.master_db.session.commit()

    def check_slave_tasks(self):
        # sort by priority
        tasks = self.master_db.session.query(masterdb.SlaveTask)\
                 .filter(
                     masterdb.SlaveTask.active==True,
                     masterdb.SlaveTask.error==False,
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
                    .outerjoin(
                        masterdb.SlaveTask,
                        and_(
                            masterdb.Slave.id == masterdb.SlaveTask.assigned_slave_id,
                            masterdb.SlaveTask.completed == False,
                            masterdb.SlaveTask.error == False
                        )
                    )\
                    .group_by(masterdb.Slave)\
                    .order_by(desc('cnt'))\
                    .all()

            if not len(slaves):
                return

            slave_list = {}
            for i in xrange(0, len(slaves)):
                slave = slaves[i][0]
                ctasks = slaves[i][1] #current tasks

                key = str(slave.slave_type_id)
                if not key in slave_list.keys():
                    slave_list[key] = [] 

                if ctasks == 0: #Slaves can only have 1 task assigned at a time
                    
                    slave_list[key].append({'slave':slave, 'ctasks': ctasks, 'slave_id': slave.id, 'slave_type_id': slave.slave_type_id})

            for task in tasks:
                key = str(task.slave_type_id)
                if len(slave_list[key]):
                    slave_meta = slave_list[key].pop()
                    slave = slave_meta['slave']

                    task.assigned_slave_id = slave.id
                    task.msg = 'Assigned to slave {}'.format(slave.id)
                    self.master_db.session.commit()
                    
                    self.send_global_task(slave.uuid, task.route, task.data, task.id, job_id=task.job_id)

                    t = self.master_db.to_dict(task, masterdb.NOTIFY_TASK_COLS)
                    self.notify_CPV1('task.add', t)


    def bd_md_slave_task_status_check(self, data, route_meta):
        #updates task as it moves through the stages
        pass

    def bd_md_slave_task_cleanup(self, data, route_meta):
        #tasks after a certain time are removed from the database 
        pass

    def loop(self):
        self.check_inbox()
        self.check_msg_timers()

        self.check_slave_tasks()
        self.check_slave_task_groups()
        self.check_slave_chainers()
        self.check_schedule()

    def comms(self):
        self.comms_on = True
        pid = os.getpid()
        self.comms_pid = pid
        self.heartbeat.__track_process__(pid, name='MasterDriver Comms', route='@bd.comms.launch')

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
                

    def bd_md_bot_message_send(self, msg, route_meta):
        self.send_message_to(msg['uuid'], msg['data'])

    def bd_md_slave_job_add(self, data, route_meta):
        job = self.master_db.add(masterdb.SlaveJob, data['job_data'])

        obj = self.master_db.to_dict(job, masterdb.NOTIFY_JOB_COLS)
        print "\n\nCALLING NOTIFY {}".format(obj)
        self.notify_CPV1('job.add', obj)
        route_meta['job_id'] = job.id

        if 'tasks_data' in data.keys():
            tasks = []
            tasks_notify_data = []
            for task in data['tasks_data']:
                task['job_id'] = job.id
                task_obj = self.__slave_task_add(task)
                d = self.master_db.to_dict(task_obj, masterdb.NOTIFY_TASK_COLS, serialize=True)
                tasks_notify_data.append(d)
                self.notify_CPV1('task.add', d)
            #self.notify_CPV1('tasks.add', tasks_notify_data) set this up

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

            j = self.master_db.to_dict(job, masterdb.NOTIFY_JOB_COLS)
            self.notify_CPV1('job.add', j)

    def bd_md_slave_job_error(self, data, route_meta):
        job = self.master_db.session.query(masterdb.SlaveJob)\
                .filter(masterdb.SlaveTask.id==data['job_id'])\
                .first()
        if job:
            job.error = True
            job.completed = True
            job.msg = data['msg']
            self.master_db.session.commit()

            j = self.master_db.to_dict(job, masterdb.NOTIFY_JOB_COLS)
            self.notify_CPV1('job.add', j)


    def bd_md_slave_job_completed(self, data, route_meta):
        job = self.master_db.session.query(masterdb.SlaveJob)\
                .filter(masterdb.SlaveJob.id==data['job_id'])\
                .first()
        if job:
            job.completed = True
            job.stage = 'Done!'
            if 'msg' not in data.keys():
                data['msg'] = 'OK'
                job.msg = data['msg']
            self.master_db.session.commit()
            j = self.master_db.to_dict(job, masterdb.NOTIFY_JOB_COLS)
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
        cb_route = None
        if 'cb_route' in data.keys():
            cb_route = data['cb_route']

        task_group_args = {
            'total_cnt': cnt,
            'job_id': data['job_id'],
            'cb_route': cb_route
        }

        task_group = self.master_db.add(masterdb.SlaveTaskGroup, task_group_args)

        print "\n\n\nRD: {}".format(data)
        for task_data in data['tasks']:
            task_args = {}
            task_args['data'] = task_data

            task_args['job_id'] = data['job_id']
            if not 'name' in task_args.keys():
                task_args['name'] = data['name']
            task_args['route'] = data['route']
            task_args['task_group_id'] = task_group.id 
            task = self.__slave_task_add(task_args)

            t = self.master_db.to_dict(task, masterdb.NOTIFY_TASK_COLS, serialize=True)
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
   
    def slave_task_error(self, task_id, msg):
        task = self.master_db.session.query(masterdb.SlaveTask)\
                .filter(masterdb.SlaveTask.id==task_id)\
                .first()

        task.error = True
        task.msg = msg


        self.master_db.session.commit()
        t = self.master_db.to_dict(task, masterdb.NOTIFY_TASK_COLS, serialize=True)
        self.notify_CPV1('task.add', t)

        if task.job_id:
            if not task.job_ok_on_error and task.job_ok_on_error != None:
                job = self.master_db.session.query(masterdb.SlaveJob)\
                        .filter(masterdb.SlaveJob.id==task.job_id)\
                        .first()
                if job:
                    job.error = True
                    job.completed = True
                    err_msg = "Job was stopped because task '{}' failed".format(task.name)
                    job.msg = err_msg

                    self.master_db.session.commit()

                    job.stage = 'Failed'
                    j = self.master_db.to_dict(job, masterdb.NOTIFY_JOB_COLS)
                    self.notify_CPV1('job.add', j)

                    
                    for job_task in job.tasks:
                        if job_task.id != task.id:
                            if not job_task.error:
                                job_task.msg = err_msg 
                                job_task.error = True 
                                job_task.active = False

                                self.master_db.session.commit()
                                t = self.master_db.to_dict(job_task, masterdb.NOTIFY_TASK_COLS, serialize=True)
                                self.notify_CPV1('task.add', t)


            elif task.task_group_id:
                task_group = self.__slave_group_task_update_cnt(task.task_group_id, 0)
                if task_group:
                    tg = self.master_db.to_dict(task_group, masterdb.NOTIFY_TASK_GROUP_COLS)
                    self.notify_CPV1('taskgroup.add', tg)

                    #group_tasks = self.master_db.session.query(SlaveTask)\
                    #                .filter(masterdb.SlaveTask.task_group_id==task_group.id)\
                    #                .all()

                    #for gt in group_tasks:
                    #    gt.
                    #    pass

        t = self.master_db.to_dict(task, masterdb.NOTIFY_TASK_COLS, serialize=True)
        self.master_db.session.commit()
        self.notify_CPV1('task.add', t)

        pass

    def bd_md_slave_task_error(self, data, route_meta):
        msg = None
        if 'msg' in data.keys():
            msg = data['msg']
        self.slave_task_error(data['task_id'], msg)


    def __slave_group_task_update_cnt(self,task_group_id, completed):
        task_group = self.master_db.session.query(masterdb.SlaveTaskGroup)\
                .filter(masterdb.SlaveTaskGroup.id == task_group_id)\
                .first()
        if completed:
            task_group.completed_cnt = task_group.completed_cnt + 1
        else:
            task_group.error_cnt = task_group.error_cnt + 1
        self.master_db.session.commit()
        return task_group


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
                task_group = self.__slave_group_task_update_cnt(task.task_group_id,1)
                if task_group:
                    tg = self.master_db.to_dict(task_group, masterdb.NOTIFY_TASK_GROUP_COLS)
                    self.notify_CPV1('taskgroup.add', tg)

            if 'msg' in data.keys():
                task.msg = data['msg']
            self.master_db.session.commit()
            t = self.master_db.to_dict(task, masterdb.NOTIFY_TASK_COLS, serialize=True)
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
        slave = self.master_db.session.query(masterdb.Slave).filter(masterdb.Slave.uuid==data['uuid']).first()
        if not slave: #if slave is not registered, it asks for authentication
            msg = create_local_task_message(
                'bd.@sd.slave.auth',
                {}
            )
            self.send_message(data['uuid'], msg)
        else:
            print "Resuming previous session with Slave Type {}  {}".format(slave.slave_type_id, data['uuid'])
            s = self.master_db.to_dict(slave, masterdb.NOTIFY_SLAVE_COLS)
            self.notify_CPV1('slave.add', s)


    def bd_md_slave_register(self, data, route_meta):
        #this is where u do some auth stuff
        slave_type = self.master_db.session.query(masterdb.SlaveType)\
                .filter(masterdb.SlaveType.model_id==data['model_id'])\
                .first()
        if slave_type:
            data['slave_type_id'] = slave_type.id
        else:
            raise ValueError("MODEL ID {} NOT IN SLAVE TYPE TABLE".format(data['model_id']))

        data['is_ec2'] = False
        print "Registered Slave Type {} id {}".format(slave_type.name, slave_type.id)

        slave = self.master_db.add(masterdb.Slave, data)

        s = self.master_db.to_dict(slave, masterdb.NOTIFY_SLAVE_COLS)
        self.notify_CPV1('slave.add', s)

        print 'SLAVES AUTOMATICALLY SET AS NON EC2 instances'


    def bd_md_slave_lost(self, data, route_meta):
        slave = self.master_db.session.query(masterdb.Slave)\
                .filter(masterdb.Slave.uuid==data['uuid'])\
                .first()
        if slave:
            slave.active = False
            self.master_db.session.commit()
            s = self.master_db.to_dict(slave, masterdb.NOTIFY_SLAVE_COLS)
            self.notify_CPV1('slave.add', s)



            #this handles all tasks assigned
            tasks = self.master_db.session.query(masterdb.SlaveTask)\
                    .filter(masterdb.SlaveTask.assigned_slave_id==slave.id)\
                    .all()
            for task in tasks:
                if task.started:
                    self.slave_task_error(task.id, 'Slave {} was lost'.format(slave.id))
                else:
                    task.assigned_slave_id==None
                    self.master_db.session.commit()


    def bd_md_Slave_APV1_forwarded(self, data, route_meta):
        data = data['data'] #the original data has priority
        self.router_msg_process(data)

    def bd_md_Slave_CPV1_forwarded(self, data, route_meta):
        self.router_msg_process(data)

    def bd_md_Slave_CPV1_sendall(self, data, route_meta):
        jobs = self.master_db.session.query(masterdb.SlaveJob)\
                .filter(
                    or_(
                        masterdb.SlaveJob.completed==False,
                        masterdb.SlaveJob.created_time>=datetime.utcnow() - timedelta(hours=12)
                    )
                )\
                .all()

        for job in jobs:
            j = self.master_db.to_dict(job, masterdb.NOTIFY_JOB_COLS)
            self.notify_CPV1('job.add', j)
            tasks = self.master_db.session.query(masterdb.SlaveTask).filter(masterdb.SlaveTask.job_id==job.id).all()
            task_data = []
            for task in tasks:
                t = self.master_db.to_dict(task, masterdb.NOTIFY_TASK_COLS, serialize=True)
                task_data.append(t)
                #self.notify_CPV1('task.add', t)
            self.notify_CPV1('tasks.add', task_data)


        slave_types = self.master_db.session.query(masterdb.SlaveType).all()
        slave_type_data = []
        for stype in slave_types:
            st = self.master_db.to_dict(stype, masterdb.NOTIFY_SLAVE_TYPE_COLS)
            slave_type_data.append(st)

        self.notify_CPV1('slave.types.add', slave_type_data)

        slaves = self.master_db.session.query(masterdb.Slave)\
                .filter(masterdb.Slave.active==True)\
                .all()

        slave_data = []
        for slave in slaves:
            s = self.master_db.to_dict(slave, masterdb.NOTIFY_SLAVE_COLS)
            slave_data.append(s)
        self.notify_CPV1('slaves.add', slave_data)


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


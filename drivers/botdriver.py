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
from utils.mpinbox import create_local_task_message, INBOX_SYS_MSG, INBOX_TASK1_MSG, INBOX_TASK2_MSG, INBOX_BLOCKING_MSG, INBOX_SYS_CRITICAL_MSG, OUTBOX_SYS_MSG, OUTBOX_TASK_MSG

from utils.mpinbox import MPPriorityQueue, create_local_task_message 
from utils.heartbeat import Heartbeat, Pulse

from twisted.internet import reactor, task

import importlib
from operator import itemgetter
driver = None
def prnt(reason):
    #USE THIS FUNCTION FOR FILE LOGGING
    print 'ERROR!!!!'
    print reason
    print reason.printTraceback()
    print '~~~~~~~~~~~~~~~~~~~~~\n'
    print traceback.format_exc()
    print '====================='

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

    check_loop_timer = [] 

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
    uuid = None
    running_instance_pid = None
    running_instance = False
    running_instance_job_id = None
    running_instance_task_id = None

    def __init__(self, config):
        self.__exc_dir__ = config['exc_dir']
        self.check_funcs = [ 
            [self.check_inbox, 0],
            [self.check_msg_timers, 1000]
        ]


        print "Init BotDriver"
        self.__bd_command_mappings = {
            '@bd.echo': self.bd_echo,
            '@bd.comms.launch': self.bd_comms_launch,

            '@bd.process.kill': self.bd_process_kill,
            '@bd.watchdog.launch': self.bd_watchdog_launch,
            '@bd.heartbeat.pulse.send': self.bd_heartbeat_pulse_send,
            '@bd.outbox.add.front': self.bd_outbox_add_front,

            '@bd.die': self.bd_die,

            '@bd.error':self.bd_error,

            '@bd.instance.free': self.bd_instance_free,

            '@bd.taskrunner.inbox.add': self.bd_taskrunner_inbox_add
        }
        if not self.uuid:
            self.uuid = self.create_bot_uuid() 

        self.msg_timers = [] 

        self.heartbeat_pulse_interval = 10

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


    def create_bot_uuid(self):
        return str(uuid.uuid1())

    def send_msg(self, msg, priority):
        print "SENDING MSG!!"
        self.outbox.put(msg, priority)


    def send_message_to(self, uuid, msg, priority=OUTBOX_TASK_MSG):
        data = {'uuid': uuid, 'data': msg}
        out = create_local_task_message('bd.@md.bot.message.send', data, origin=uuid)
        print "OUT MSG: {}".format(out)
        print "outbox obj:{}".format(self.outbox)
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
    def create_global_task_group_message(route, data, name, job_id, job_ok_on_error=False, retry_cnt=None, route_meta=None):
        if not route_meta:
            route_meta = {
                'type': 'default',
                'job_id': job_id
            }
        msg = {
            'route': 'bd.@md.slave.task.group.add',
            'data': data, 
            'route_meta': route_meta
        }
        msg['data']['route'] = route
        msg['data']['name'] = name
        msg['data']['job_id'] = job_id
        if retry_cnt:
            msg['data']['retry_cnt'] = retry_cnt

        return msg

    @staticmethod
    def create_global_task_message(route, data, name, parent_task_id=None, task_group_id=None, job_id=None, job_ok_on_error=False, retry_cnt=None, route_meta=None):
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

        if retry_cnt:
            msg['data']['task_data']['retry_cnt'] = retry_cnt

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

    def bd_outbox_add_front(self, data, route_meta):
        pass

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

        if data.get('die'):
            self.bd_die({'dmsg': error_msg}, route_meta)

        print "\n\n\nbd.err\n--------\n{}\n\n".format(error_msg)
        
    def bd_comms_launch(self, data, route_meta):
        self.comms()

    def bd_instance_free(self, data, route_meta):
        if self.running_instance:
            print 'Freeing instance... instance: True'
            self.bd_process_kill({'pid': self.running_instance_pid}, route_meta)

            self.running_instance = False 
            self.running_instance_job_id = None 
            self.running_instance_task_id = None
            self.running_instance_pid = None
        else:
            print 'Freeing instance... instance: False'

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
        ob = self.outbox_cb.start(.01, now=False)

        def handle_error (reason):
            self.report_error('bd.outbox', str(reason))


        ob.addErrback(handle_error)

    def init_mailbox(self, manager):
        self.manager = manager
        self.inbox = MPPriorityQueue(self.INBOX_PRIORITIES, manager=manager)
        self.outbox = MPPriorityQueue(self.OUTBOX_PRIORITIES, manager=manager)
        self.heartbeat_inbox = MPPriorityQueue(1, manager=manager)
        self.taskrunner_inbox = MPPriorityQueue(1, manager=manager)

        #self.init_outbox_callback()


    def check_outbox(self):
        err_msg = None 
        try:
            msg = self.outbox.get()
            if (msg):
                if self.BOT_TYPE == "MASTER":
                    if not msg['data']['route'] == 'bd.sd.@CPV1.data.send':
                        print "___\n<BotDriver Outbox {}>\n{}\n___".format(os.getpid(), msg)
                    else: 
                        print "___\n<BotDriver Outbox action: {}>___".format(msg['data']['data']['action'])

                else:
                        print "___\n<BotDriver Outbox {}>\n{}\n___".format(os.getpid(), msg)


                if self.factory:
                    self.factory.send_it(msg)
                else:
                    err_msg = 'BotDriver Comms factory is null: {}'.format(msg['route'])
        except Exception as e:
            err_msg = str(e)
            print traceback.format_exc()


        finally:
            if err_msg:
                self.report_error('bd.comms', err_msg, kill=True)


    def router(self, route, data, route_meta):
        error = True
        msg = 'Uncaught Error!'
        try:
            resp = self.command_mappings[route](data, route_meta)
        
        except KeyError as e:
            if route == str(e):
                msg = 'Route "{}" does not exist'.format(route)
                self.report_error('bd.router.err', msg)
            else:
                msg = "KeyError issue in code: '{}'".format(e)
                print traceback.print_exc()
                self.report_error('bd.router.unk_err', msg)

        except Exception as e:
            msg = str(e)
            print traceback.print_exc()
            self.report_error('bd.router.unk_err', msg)

        else:
            error = False
            msg = None
        
        return error, msg

    def router_msg_process(self, msg, origin=None):
        if not origin:
            origin = '.'

        route_meta = {'type': 'default', 'origin': origin}

        if 'route_meta' in msg.keys():
            route_meta = msg['route_meta']

            cols = route_meta.keys()
            if not 'type' in cols:
                route_meta['type'] = 'default'

        route_meta['route'] = msg['route']


        ######MOVE EVERYTHING BELWO IF TO TASK_GLOBAL_START and launch process from main pid so global_start can still change variables
        ################3
        ##################
        ##################3
        ##################3

        '''
        if msg['route'] == 'bd.@sd.task.global.start': #checks to see if global task 
    #        if msg['data']['route'].find('#') != -1:
       '''
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

    def check_inbox(self):
        msg, priority = self.inbox.get(get_priority=True)
        if (msg):
            self.msg_id+=1
            now = datetime.utcnow().strftime("%m/%d/%y %H:%M:%S.%f")
            print "_________________________________________\n<BotDriver Inbox {} prior: {} msgid {} {}>\n{}\n____________________________".format(os.getpid(), priority, self.msg_id, now, msg)


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

    def taskrunner_inbox_get_tag(self, tag, poll=1, delay=.25, get_all=False, raise_error=False, err_msg=None):
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
        if not len(msgs) and raise_error:
            if not err_msg:
                err_msg = 'Tag "{}" was polled until timeout'.format(tag)
            raise Exception(err_msg)
        return msgs

    @staticmethod
    def taskrunner_address_tag_get_uuid(tag):
        try:
            if len(tag) > 3:
                if tag[0:3] == '###':
                    return tag.split('@')[1]
        except:
            raise ValueError("SOMETHING WRONG taskrunner_address_tag: {}".format(tag))

    def taskrunner_create_address_tag(self):
        #in the future when task id is assigned to current self, this func can be created by local tasks
        #using random generater based on time rather
        self.taskrunner_idc +=1
        return '###{}.{}@{}'.format(self.task_id, self.taskrunner_idc, self.uuid) 

    def query_WCV1(self, query, tag, job_id=None, name='Querying warehouse db'):
        if not job_id:
            job_id = self.job_id

        self.add_global_task('bd.sd.@WCV1.query', {'query': query, 'tag': tag}, name, job_id=job_id)


    def update_WCV1(self, table_name, arg, job_id=None):
        if not job_id:
            job_id = self.job_id

        args = [] 
        if arg:
            if type(arg) == list:
                args = arg

            else:
                args = [arg]


        name = "Modifying {} rows in warehouse db table '{}'".format(len(args), table_name)
        payload = {'table_name': table_name, 'args': args, 'overwrite': True}
        self.add_global_task('bd.sd.@WCV1.model.update', payload, name, job_id=job_id)

        
    def insert_WCV1(self, table_name, arg, job_id=None, tag=None):
        if not job_id:
            job_id = self.job_id

        args = None
        if arg:
            if type(arg) == list:
                args = arg

            else:
                args = [arg]

            name = "Inserting {} rows in warehouse db table '{}'".format(len(args), table_name)
            payload = {'table_name': table_name, 'args':args,}
            if tag:
                payload['tag'] = tag
            self.add_global_task('bd.sd.@WCV1.model.insert', payload, name, job_id=job_id)

    def loop(self):
        timer_len = len(self.check_loop_timer) - 1
        init_loop = False
        if timer_len == -1:
            init_loop = True

        for i, funcd in enumerate(self.check_funcs):
            def get_timer_ring():
                delay = timedelta(milliseconds=funcd[1])
                timer_ring = datetime.utcnow() + delay
                return timer_ring

            func = funcd[0]
            if init_loop:
                timer_ring = get_timer_ring()
                self.check_loop_timer.append(timer_ring)
            if i <= timer_len:
                if self.check_loop_timer[i] <= datetime.utcnow():
                    func()
                    self.check_loop_timer[i] = get_timer_ring()

    def start(self):
        with multiprocessing.Manager() as manager:
            self.manager = manager
            pid = os.getpid()
            print "Main BotDriver Pid: {}".format(pid)
            self.init_mailbox(manager)
            

            self.heartbeat = Heartbeat(self.heartbeat_inbox, self.inbox, grace=30, dead_cb=dead_process_cb, parent_pid=os.getpid())
            
            self.heartbeat.__track_process__(pid, name='BotDriver Main')


            self.init_start()

            while True and not self.KILL_BOT:
                try:
                    self.loop()
                except Exception as e:
                    if self.BOT_TYPE == "MASTER":
                        print "\n\n~~~~~~~~~~~~~~~~~~~~~~\nbd.check_inbox error: {}\n\n\n".format(e)
                        err_msg = 'Uncaught Master Error "{}": {}'.format(str(e), traceback.format_exc())

                        self.alert_CPV1('UNCAUGHT ERROR: >>{}<<'.format(err_msg), alert_color='red lighten-1', persist=True)

            #RIGHT HERE IS WHERE WATCHDOG RUNS THROUGH BOTS RUNNING PROCESSES AND PURGES





from botdriver import BotDriver, prnt, OUTBOX_SYS_MSG
from comms.client import BotClientFactory
import os
from twisted.internet import reactor, task
from mpinbox import create_local_task_message 
import json

from mpinbox import create_local_task_message, INBOX_SYS_MSG, INBOX_TASK1_MSG, INBOX_TASK2_MSG, INBOX_BLOCKING_MSG, INBOX_SYS_CRITICAL_MSG, OUTBOX_SYS_MSG, OUTBOX_TASK_MSG

from datetime import datetime, timedelta

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
            'bd.@sd.task.global.add': self.bd_sd_task_global_add,
            'bd.@sd.task.global.stop': self.bd_sd_task_global_stop
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
        #send global task to masterdriver
        if self.RUNNING_GLOBAL_TASK:
            if not job_id:
                job_id = self.job_id

        msg = self.create_global_task_message(route, data, name, parent_task_id, 
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


    def bd_sd_task_global_stop(self, data, route_meta):
        if data['task_id'] == self.running_instance_task_id:
            print "Stopping task id {}".format(self.running_instance_task_id)
            self.bd_instance_free({}, route_meta)


    def bd_sd_task_remove(self, data, route_meta):
        pass

    def loop(self):
        self.check_inbox()
        self.check_msg_timers()


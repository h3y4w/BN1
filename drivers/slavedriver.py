from botdriver import BotDriver, prnt, OUTBOX_SYS_MSG
from comms.client import BotClientFactory
import os
from twisted.internet import reactor, task
from utils.mpinbox import create_local_task_message 
import json

from utils.mpinbox import create_local_task_message, INBOX_SYS_MSG, INBOX_TASK1_MSG, INBOX_TASK2_MSG, INBOX_BLOCKING_MSG, INBOX_SYS_CRITICAL_MSG, OUTBOX_SYS_MSG, OUTBOX_TASK_MSG

from datetime import datetime, timedelta



class SlaveDriver (BotDriver):
    CONNECTED_TO_MASTER = False
    
    PULSE_MASTER_TIMER = None
    master_server_ip = None 
    master_server_port = None 
    is_ec2 = False

    BOT_TYPE = "SLAVE"
    global_task_waiting = False

    def __init__(self, config):
        super(SlaveDriver, self).__init__(config)

        self.check_funcs.extend([]) 

        slave_key_path = os.path.expanduser('~/.slave_key.json')
        slave_key_info = None 

        print "SLAVE KEY SHOULD BE :{}".format(slave_key_path)
        
        if os.path.exists(slave_key_path):
            print "SLAVE PATH EXISTS"
            with open(slave_key_path, 'r') as f:
                slave_key_info = json.loads(f.read())

            if slave_key_info: #this can pass more auth in future
                self.uuid = slave_key_info['uuid']
                self.is_ec2 = True
                print "FOUND UUID: {}".format(self.uuid)
                #self.key = slave_key_info['key']
                #self.secret = slave_key_info['secret']


        self.master_server_ip = config['master_server_ip']
        self.master_server_port = config['master_server_port']
        self.PULSE_MASTER_TIMER = datetime.utcnow()

        self.bot_route_header = self.bot_route_header + ".sd.@" + self.model_id


        maps = {
            'bd.@sd.master.connected': self.bd_sd_master_connected,
            'bd.@sd.master.disconnected': self.bd_sd_master_disconnected,
            'bd.@sd.slave.auth': self.bd_sd_slave_auth,

            'bd.@sd.task.global.start': self.bd_sd_task_global_start,
            'bd.@sd.task.global.stop': self.bd_sd_task_global_stop,

        }
        self.add_command_mappings(maps)

    def send_message_to_master(self, msg, priority=OUTBOX_TASK_MSG):
        self.outbox.put(msg, priority)

    def comms(self):
        self.comms_on = True
        pid = os.getpid()
        self.comms_pid = pid

        print 'Starting SlaveDriver Comms'
        self.heartbeat.__track_process__(pid, name='SlaveDriver Comms', route='@bd.comms.launch')
        self.comms_pulse_cb = task.LoopingCall(self.heartbeat.send_pulse, pid=pid, tmp_grace=10)

        cp = self.comms_pulse_cb.start(2)
        cp.addErrback(prnt)

        self.factory = BotClientFactory(self)
        self.init_outbox_callback()


        reactor.connectTCP(self.master_server_ip, self.master_server_port, self.factory)
        reactor.run()


    def bd_sd_master_disconnected(self, data, route_meta):
        self.CONNECTED_TO_MASTER = False
        print "BD_SD_MASTER_DISCONNECTED!!!\n\n"
        if hasattr(self, 'comms_pulse'):
            self.comms_pulse_cb.stop()

        if hasattr(self, 'disconnected_from_master'):
            self.disconnected_from_master()

    def bd_sd_master_connected(self, data, route_meta):
        self.CONNECTED_TO_MASTER = True
        print "BD_SD_MASTER_CONNECTED!!!\n\n"

        if hasattr(self, 'connected_to_master'):
            self.connected_to_master()

    def add_global_task(self, route, data, name, parent_task_id=None, task_group_id=None, job_id=None, job_ok_on_error=False, retry_cnt=None, route_meta=None):
        #send global task to masterdriver

        if self.RUNNING_GLOBAL_TASK:
            if not job_id:
                job_id = self.job_id
                print  "ADDING GLOBAL_TASK: ",

        print "SELF>JOB {} SELF>TASK{}\n\n\n\n".format(self.job_id, self.task_id)

        msg = self.create_global_task_message(route, data, name, 
                                              parent_task_id=parent_task_id, 
                                              task_group_id=task_group_id,
                                              job_id=job_id,
                                              job_ok_on_error=job_ok_on_error, 
                                              retry_cnt=retry_cnt,
                                              route_meta=route_meta)
        self.outbox.put(msg, OUTBOX_SYS_MSG)
    
    def add_global_task_group(self, route, data, name, job_ok_on_error=False, job_id=None, route_meta=None):
        raise ValueError("TASK_GROUP IS BROKEN")
        ######################################
        ######## routes have to be written to be able to handle it
        ######## master stores individual task group data with
        ######### the task groups sending data to master address tag
        ######## when task group cb func is ran, it pings master with master tag (which stores data) and sends along a new address_tag to itself so that the master forwards it 
        #######################################################
        if self.RUNNING_GLOBAL_TASK:
            if not job_id:
                job_id = self.job_id

        msg = self.create_global_task_group_message(route, data, name, job_id, 
                                                    job_ok_on_error, route_meta=route_meta)
        self.outbox.put(msg, OUTBOX_SYS_MSG)

    def bd_sd_slave_auth(self, data, route_meta):
        self.master_uuid = data['master_uuid']
        print "Sending Authentication info to Master"
        msg = create_local_task_message(
            'bd.@md.slave.register',
            {
                'uuid': self.uuid,
                'model_id': self.model_id
            }
        )
        self.send_msg(msg, OUTBOX_SYS_MSG)

    def starting_global_task(self, data, route_meta):
        task_id = route_meta['task_id']

        task_msg = create_local_task_message(
            'bd.@md.slave.task.started',
            {
                'task_id':task_id,
                'time_started': str(datetime.utcnow())
            }
        )
        self.send_message_to_master(task_msg)

        print 'Starting Global Task id {}'.format(task_id)
        route = data['route']
        route_data = data['data']
        

        err, msg = self.router(route, json.loads(route_data), route_meta)

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

        self.send_message_to_master(task_msg1)

        free_msg = create_local_task_message (
            '@bd.instance.free',
            {}
        )
        self.inbox.put(free_msg, INBOX_SYS_MSG)

    def router_task_process_launch(self, route, data, route_meta):
        process_data = {
            'data': data,
            'route': route
        }
        
        print "PROCESS_DATA: {}".format(process_data)
        process = self.router_process_launch(self.starting_global_task, process_data,route_meta)

        if process:
            if process.pid:
                self.running_instance = True

                self.running_instance_job_id = route_meta['job_id']
                self.running_instance_task_id = route_meta['task_id']
                self.running_instance_pid = process.pid
                return
            else:
                raise ValueError("Process id could not be got")

        else:
            raise ValueError("Process could not be created!")


    def bd_sd_task_global_start(self, data, route_meta): #THIS FUNCTION STARTS ALL GLOBALLY TRACKED TASKS

        if not self.running_instance:
            self.router_task_process_launch(data['route'], data['data'], route_meta)
        else:
            self.reject_global_task(route_meta['task_id'], route_meta['job_id'])

    def reject_global_task(self, task_id, job_id):
        msg = create_local_task_message(
            'bd.@md.slave.task.reject',
            {
                'task_id': task_id,
                'job_id': job_id
            }
        )
        self.send_message_to_master(msg)


    def bd_sd_task_global_add(self, data, route_meta):
        raise NotImplemented


    def bd_sd_task_global_stop(self, data, route_meta):
        if data['task_id'] == self.running_instance_task_id:
            print "Stopping task id {}".format(self.running_instance_task_id)
            self.bd_instance_free({}, route_meta)
            out = create_local_task_message(
                'bd.md.@slave.task.stopped',
                {}
            )
            self.send_message_to_master(out)

    def bd_sd_task_remove(self, data, route_meta):
        pass

    def send_pulse_to_master(self):
        if self.uuid:
            data = {'uuid': self.uuid}
            out = create_local_task_message('bd.@md.slave.pulse', data)
            self.send_message_to_master(out)

    def loop(self):
        self.check_inbox()
        self.check_msg_timers()
        if self.PULSE_MASTER_TIMER < datetime.utcnow():
            self.send_pulse_to_master()
            self.PULSE_MASTER_TIMER = datetime.utcnow() + timedelta(seconds=5)
        


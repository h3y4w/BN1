from botdriver import BotDriver, prnt
import os
from comms.server import BotServerFactory

from utils.mpinbox import  INBOX_SYS_MSG, INBOX_TASK1_MSG, INBOX_TASK2_MSG, INBOX_BLOCKING_MSG, INBOX_SYS_CRITICAL_MSG, OUTBOX_SYS_MSG, OUTBOX_TASK_MSG

from utils.mpinbox import MPPriorityQueue, create_local_task_message 

from db import db
from db import masterdb
from sqlalchemy import func, desc, and_, or_

from datetime import datetime, timedelta

from twisted.internet import reactor, protocol, task

import json


import boto3


class MasterDriver(BotDriver):
    CPV1_notify_msg_id = 0
    ec2_resource = None

    def __init__(self, config):
        ### SET MASTERDRIVER SETTINGS FROM CONFIG HERE
        self.port = config['server_port'] 
        self.host = config['server_host']
        super(MasterDriver, self).__init__(config)

        aws_key = config.get('aws_access_key')
        aws_secret = config.get('aws_secret_key')
        
        if aws_key and aws_secret:
            self.ec2_resource = boto3.resource(
                'ec2',
                region_name='us-west-2',
                aws_access_key_id=aws_key,
                aws_secret_access_key=aws_secret
            )

        self.needed_bots = {}

        self.bot_route_header = self.bot_route_header + ".md.@" + self.model_id

        print 'Init MasterDriver'
        maps = {
            'bd.@md.global.task.post': self.bd_md_global_task_post,

            'bd.@md.bot.message.send': self.bd_md_bot_message_send,
            'bd.@md.slave.job.add': self.bd_md_slave_job_add,
            'bd.@md.slave.job.remove': self.bd_md_slave_job_remove,
            'bd.@md.slave.job.stage': self.bd_md_slave_job_stage,
            'bd.@md.slave.job.completed': self.bd_md_slave_job_completed,
            'bd.@md.slave.job.stop': self.bd_md_slave_job_stop,
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
            'bd.@md.slave.pulse': self.bd_md_slave_pulse,
            'bd.@md.slave.launch': self.bd_md_slave_launch,
            'bd.@md.slave.terminate': self.bd_md_slave_terminate,
            'bd.@md.slave.free': None,
            'bd.@md.slave.list': None,
            'bd.@md.alert.add': self.bd_md_alert_add,
            'bd.@md.alert.viewed': self.bd_md_alert_viewed,
            'bd.@md.alert.remove': self.bd_md_alert_remove,

            'bd.@md.Slave.APV1.forwarded': self.bd_md_Slave_APV1_forwarded,
            'bd.@md.Slave.CPV1.get.objs': self.bd_md_Slave_CPV1_get_objs,
            'bd.@md.Slave.CPV1.get.last-update-id': self.bd_md_Slave_CPV1_get_lastupdateid,
            'bd.@md.Slave.CPV1.forwarded': self.bd_md_Slave_CPV1_forwarded,
            'bd.@md.Slave.CPV1.ping': self.bd_md_Slave_CPV1_ping,
            'bd.@md.Slave.CPV1.sendall': self.bd_md_Slave_CPV1_sendall,
            'bd.@md.Slave.CPV1.notify': self.bd_md_Slave_CPV1_notify,

        }
        self.add_command_mappings(maps)
        self.slave_tasks = MPPriorityQueue(2)
        
        self.CPV1_grace_minutes = 120 #change this later
        self.CPV1_pinged_times = {} 

        args = []
        db_fn = os.path.join(self.__exc_dir__, 'master.db')
        #remove in future
        try:
            pass
            #os.remove(db_fn) #added for deebugging
        except:
            pass

        db_exists = os.path.exists(db_fn)
        self.master_db = db.DB(db_fn, masterdb.Base, 'sqlite', create=not db_exists)

        if not db_exists:
            args = [
                {'name': 'AccessPoint', 'model_id': 'APV1'},
                {'name': 'Command Portal', 'model_id': 'CPV1'},
                {'name': 'Scraper', 'model_id': 'SCV1'},
                {'name': 'Warehouse Clerk', 'model_id': 'WCV1'},
                {'name': 'Binance Trader', 'model_id': 'BTV1'},
                {'name': 'Alcatraz Ticket Bot', 'model_id': 'ATBV1'}
            ]

            for a in args:
                self.master_db.add(masterdb.SlaveType, a)
       
        else:
            self.master_db.session.rollback()
            jobs = self.master_db.session.query(masterdb.SlaveJob)\
                    .filter(
                        masterdb.SlaveJob.error==False,
                        masterdb.SlaveJob.completed==False
                    )\
                    .all()

            for job in jobs:
                self.slave_job_error(job.id, 'Master closed abruptly before jobs completed', notify=False)

            slaves = self.master_db.session.query(masterdb.Slave)\
                    .filter(masterdb.Slave.active == True)\
                    .all()

            for slave in slaves:
                if not slave.is_ec2:
                    slave.active = False
            self.master_db.session.commit()

    def launch_ec2_instance(self):
        slave_uuid = self.create_bot_uuid()
        slave_key = json.dumps({'uuid': slave_uuid})

        user_data = '''#!/bin/bash
        apt-get update
        apt-get install python -y
        apt-get install python-pip -y

        cd /home/ubuntu 
        echo '{}' > .slave_key.json
        wget https://bn1-bucket.s3-us-west-2.amazonaws.com/lib/bn1_lib.tar.gz
        tar -zxvf bn1_lib.tar.gz
        wget https://bn1-bucket.s3-us-west-2.amazonaws.com/configs/WCV1_config.json -O BN1/run_deployed/config.json
        pip install -r BN1/reqs/WCV1_reqs.txt 
        cd BN1/run_deployed
        python WCV1_run.py
        '''.format(slave_key)
        ec2_slave = self.ec2_resource.create_instances(
            ImageId='ami-0b37e9efc396e4c38',
            UserData=user_data,
            MinCount=1, MaxCount=1,
            InstanceType='t2.micro',
            SecurityGroupIds=['sg-0fc9354b97fb643c7'],
            KeyName='heyaw_key',
            TagSpecifications=[{'ResourceType': 'instance', 'Tags':[{'Key':'launch-type', 'Value': 'master'}]}]
        )[0]

        #gives ec2 instances 1 1/2 min grace period#
        ########################################
        slave_info = {
            'is_ec2': True,
            'ec2_instance_id': ec2_slave.instance_id,
            'uuid': slave_uuid,
            'active': True,
            'init': True,
            'last_pulse': datetime.utcnow()+timedelta(seconds=90),
            "slave_type_id": 4 #warehouse slave_id
        }

        slave = self.master_db.add(masterdb.Slave, slave_info)
        if not slave:
            self.terminate_ec2_instance([ec2_slave.instance_id])
            msg = "AWS Ec2 instance launched but could not add to masterdb.  Terminating ec2 instance {}".format(ec2_slave.instance_id)
            self.send_alert({'msg': msg, 'go_to': '/alerts'})
            return

        self.ec2_resource.create_tags(
            Resources=[ec2_slave.instance_id], 
            Tags=[
                {'Key': 'slave.id', 'Value': str(slave.id)},
                {'Key': 'bot.type', 'Value': 'slave'},
                {'Key': 'Name', 'Value': 'sd-'+str(slave.id)}
            ]
        )
        self.notify_CPV1('add', slave)
        return slave


    def terminate_ec2_instance(self, instance_ids):
        print "\n\n\nTERMINATING : {}".format(instance_ids)
        print self.ec2_resource.instances.filter(InstanceIds=instance_ids).terminate()

    def send_message_to(self, uuid, msg, priority=OUTBOX_TASK_MSG):
        payload = {'uuid': uuid, 'data': msg}
        self.outbox.put(payload, priority)


    def send_message(self, uuid, msg, priority=OUTBOX_TASK_MSG):
        print "MASTER_DRIVER SENDING MSG TO: {} (priority is hardcoded)".format(uuid)
        print "msg: {}\n-----------------------------".format(msg)
        payload = {'uuid': uuid, 'data': msg}
        self.outbox.put(payload, priority) 


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

    def bd_md_slave_launch(self, data, route_meta): 
        slave = self.launch_ec2_instance() #already notified
        #alert_CPV1 msg= Ec2 instance launched successfully id {}

    def bd_md_slave_terminate(self, data, route_meta):
        cols = data.keys()
        ec2_instance_ids = []
        slave_ids = []
        if 'id' in cols:
            slave_ids.append(data['id'])
        if 'ids' in cols:
            slave_ids += ids
        '''
        if 'instance_id' in cols:
            ec2_instance_ids.append(data['instance_id'])
        if 'instance_ids' in cols:
            ec2_instance_ids += data['instance_ids']
        '''
        if slave_ids:
            slaves = self.master_db.session.query(Slave)\
                    .filter(Slave.id.in_(slave_ids))\
                    .all()

            for slave in slaves:
                if slave.ec2_instance_id:
                    ec2_instance_ids.append(slave.ec2_instance_id)

        if ec2_instance_ids:
            self.terminate_ec2_instance(ec2_instance_ids)
        
    def bd_md_slave_pulse(self, data, route_meta):
        slave = self.master_db.session.query(masterdb.Slave).filter(masterdb.Slave.uuid == data['uuid']).first()
        if slave:
            slave.last_pulse = datetime.utcnow()
            self.master_db.session.commit()
        #Here is where it updates last pulse 

    def check_slave_pulses(self):
        slaves = self.master_db.session.query(masterdb.Slave).filter(masterdb.Slave.active==True).all()
        
        for slave in slaves:
            if (slave.last_pulse + timedelta(seconds=10)) < datetime.utcnow():
                slave.active = False
                if slave.is_ec2:
                    self.terminate_ec2_instance([slave.ec2_instance_id])
                    self.send_alert({'msg': 'Slave {} has been marked inactive'.format(slave.id), 'go_to': '/alerts'})
                self.master_db.session.commit()
                self.notify_CPV1('add', slave)

    def check_slave_schedules(self):
        groups = self.master_db.session.query(masterdb.SchedulerGroup).all()
        if groups:
            for group in groups:
                schedulers = self.master_db.session.query(masterdb.Scheduler)\
                        .filter(
                            masterdb.Scheduler.scheduler_group_id==group.id
                        ).all()
                if schedulers:
                    for scheduler in schedulers:
                        if datetime.utcnow() > scheduler.run_time and not scheduler.executed:
                            data = {
                                "job_data": {'name': group.name},
                                'tasks_data': [
                                    {'name': 'Launcher', 'route': scheduler.route, 'data': scheduler.data}
                                ]
                            }
                            job, tasks = self.__slave_job_add(data)

                            msg = "Job ({}) '{}' added by Scheduler ({}) '{}'".format(job.id, job.name, group.id, group.name)
                            self.send_alert({'msg': msg, 'go_to': '/job/{}'.format(job.id)})

                            if scheduler.frequency_min:
                                new_run_time = datetime.utcnow() + timedelta(minutes=scheduler.frequency_min)
                                scheduler.run_time = new_run_time
                                sc = self.master_db.as_json(scheduler, cols=masterdb.NOTIFY_SCHEDULER_COLS)
                                ###################3
                                #################notify_cpv1 with new updated times
                            else:
                                scheduler.executed = True
                                #maybe delete them or do something else

                            self.notify_CPV1('add', scheduler)
                            self.master_db.session.commit()



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
                    task_args['name'] = task_group.cb_name 
                    task_args['route'] = task_group.cb_route 
                    task_args['job_id'] = task_group.job_id

                    cb_data = self.taskrunner_inbox_get_tag(task_group.cb_data_tag, get_all=True)

                    task_args['data'] = {
                        'cb_data': cb_data,
                        '__task_group__': {
                            'total_cnt': total_cnt,
                            'error_cnt': task_group.error_cnt,
                            'completed_cnt': task_group.completed_cnt,
                        }
                    }

                    task = self.__slave_task_add(task_args)
                    if task:
                        task_group.grouped = True 
                        self.notify_CPV1('add', task_group)
                        

             
            self.master_db.session.commit()

    def check_slave_tasks(self):
        # sort by priority
        tasks = self.master_db.session.query(masterdb.SlaveTask)\
                 .filter(
                     masterdb.SlaveTask.error==False,
                     masterdb.SlaveTask.assigned_slave_id.is_(None)
                 ).order_by(masterdb.SlaveTask.time_created.asc()).all()

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

            if not len(slaves): #no slaves with needed type
                pass
                '''
                alert = self.master_db.add(masterdb.Alert, {'msg': 'No Slaves here', 'go_to': '/slaves/'})
                if alert:
                    self.notify_CPV1('add', alert)
                return
                '''

            slave_list = {}
            for i in xrange(0, len(slaves)):
                slave = slaves[i][0]
                ctasks = slaves[i][1] #current tasks

                key = str(slave.slave_type_id)
                if not key in slave_list.keys():
                    slave_list[key] = [] 

                slave_list[key].append({'slave':slave, 'ctasks': ctasks, 'slave_id': slave.id, 'slave_type_id': slave.slave_type_id})

            #add a check to see if active slaves for tasks 

            keys = slave_list.keys()
            for task in tasks:
                key = str(task.slave_type_id)

                if not key in keys:
                    #launch more bots
                    if task.active:
                        task.active=False
                        task.msg = 'Launch slave type {} to start'.format(key)
                        alert = self.master_db.add(masterdb.Alert, {'msg': 'Slave type '+key+' needed to run task '+str(task.id), 'go_to': '/task/{}'.format(task.id)})
                        if alert:
                            self.notify_CPV1('add', alert)
                        self.master_db.session.commit()
                    else:
                        #already marked as inactive
                        pass


                elif len(slave_list[key]):
                    if slave_list[key][0]['ctasks'] == 0: #slaves are only assigned one task at a time
                        slave_meta = slave_list[key].pop(0)
                        keys = slave_list.keys() #updates keys
                        slave = slave_meta['slave']

                        task.assigned_slave_id = slave.id
                        task.msg = 'Assigned to slave {}'.format(slave.id)
                        task.active=True

                        if task.job_id:
                            job = self.master_db.session.query(masterdb.SlaveJob).filter(masterdb.SlaveJob.id == task.job_id).first()
                            if job:
                                if job.stage == 'Queued':
                                    job.stage = 'Running'
                                    
                                    j = self.master_db.as_json(job, cols=masterdb.NOTIFY_JOB_COLS)
                                    self.notify_CPV1('add', job)


                        self.master_db.session.commit()
                        
                        self.send_global_task(slave.uuid, task.route, task.data, task.id, job_id=task.job_id)

                        self.notify_CPV1('add', task)

    def bd_md_global_task_post(self, data, route_meta):
        pass


    def bd_md_slave_launch_ec2(self, data, route_meta):
        #https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.ServiceResource.create_instances
        pass

    def bd_md_slave_kill(self, data, route_meta):
        #terminate instances
        pass


    def bd_md_slave_task_status_check(self, data, route_meta):
        #updates task as it moves through the stages
        pass

    def bd_md_slave_task_cleanup(self, data, route_meta):
        #tasks after a certain time are removed from the database 
        pass

    def loop(self):
        self.check_slave_pulses()
        self.check_inbox()
        self.check_msg_timers()
        
        #add a time statement here to run every x seconds
        #ex run check_inbox more often than check_slave_task
        self.check_slave_tasks()
        self.check_slave_task_groups()
        self.check_slave_chainers()
        self.check_slave_schedules()

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

    def notify_CPV1(self, action, data, session_id=None, slave_uuid=None, update=True, model_cols=[]):
        path_defined = True
        obj_type = None
        if not '.' in action:
            path_defined = False 

        if not data:
            return False

        payload = [] 

        PATH_NOT_DEFINED_ERROR_MSG = "Dicts need action path defined. Include action path or pass obj as model class" 
        payload_type_list = False 
        if type(data) == dict:
            if not path_defined:
                raise Exception(PATH_NOT_DEFINED_ERROR_MSG)
            payload = data

        elif type(data) == list:
            payload_type_list = True
            for obj in data:
                if type(obj) == dict:
                    if not path_defined:
                        raise Exception(PATH_NOT_DEFINED_ERROR_MSG)
                else:
                    obj_type = type(obj)
                    obj = self.master_db.as_json(obj, cols=model_cols)
                payload.append(obj)
        else:
            if type(data) in masterdb.accessible_models:
                obj = self.master_db.as_json(data, cols=model_cols)
                obj_type = type(data)
                payload = obj
            else:
                raise KeyError("Type '{}' is not an accessible_model".format(type(data)))

        if update:
            self.CPV1_notify_msg_id+=1
        expire_time = datetime.utcnow() + timedelta(minutes=self.CPV1_grace_minutes)
        uuid_query= self.master_db.session.query(masterdb.SlaveType.id, masterdb.Slave.uuid)

        if slave_uuid:
            uuid_query = uuid_query.filter(
                masterdb.Slave.uuid==slave_uuid,
                masterdb.Slave.active==True
            )
        else:
                uuid_query = uuid_query.filter(
                    masterdb.SlaveType.model_id=='CPV1'
                ).join(
                        masterdb.Slave,
                        masterdb.Slave.slave_type_id==masterdb.SlaveType.id
                ).filter( masterdb.Slave.active==True)

        uuids = uuid_query.all()

        obj_type_paths = {
            masterdb.SchedulerGroup: 'scheduler.group',
            masterdb.Scheduler: 'scheduler',
            masterdb.Slave: 'slave',
            masterdb.SlaveType: 'slave.type',
            masterdb.SlaveTask: 'task',
            masterdb.SlaveTaskGroup: 'task.group',
            masterdb.SlaveJob: 'job',
            masterdb.SlaveTask: 'task',
            masterdb.Alert: 'alert'
        }

        if not path_defined:
            action_path = None
            try:
                path = obj_type_paths[obj_type]
                if payload_type_list:
                    path += 's' #ex task + 's' implies its a list of objects
                action_path =  path + '.' + action

            except KeyError:
                raise Exception("Obj type: {} does not have defined paths".format(str(obj_type)))
            action = action_path

        for uuid in uuids:
            uuid = uuid[1] #second col is uuid, first col is slavetype id 
            chunk_size = 30
            if type(payload) == list:
                cnt = len(payload)/chunk_size
                if len(payload) % chunk_size:
                    cnt+=1
                for i in xrange(0, cnt):
                    print 'sent chunk'
                    chunk = payload[i*chunk_size:((i+1)*chunk_size-1)]
                
                    rdata = {
                        'action_data': chunk,
                        'action': action,
                        'last_update': self.CPV1_notify_msg_id,
                        'sid': session_id
                    }
                    
                    msg = create_local_task_message('bd.sd.@CPV1.data.send', rdata)
                    self.send_message_to(uuid, msg, OUTBOX_SYS_MSG)
            else:
                rdata = {
                    'action_data': payload,
                    'action': action,
                    'last_update': self.CPV1_notify_msg_id,
                    'sid': session_id
                }
                
                msg = create_local_task_message('bd.sd.@CPV1.data.send', rdata)
                self.send_message_to(uuid, msg, OUTBOX_SYS_MSG)
        return True

    def __slave_task_add(self, data):
        model_id = self.get_model_id_from_route(data['route'])

        if not model_id in ['bd', 'md', 'sd']: #to prevent adding driver sys tasks 
            slave_type_id = self.master_db.session.query(masterdb.SlaveType.id)\
                    .filter(masterdb.SlaveType.model_id==model_id)\
                    .first()

            if slave_type_id:
                slave_type_id = slave_type_id[0]

                data["slave_type_id"] = slave_type_id

        task =  self.master_db.add(masterdb.SlaveTask, data) 
        self.notify_CPV1('add', task)
        return task

    def bd_md_bot_message_send(self, msg, route_meta):
        self.send_message_to(msg['uuid'], msg['data'])


    def send_alert(self, data):
        alert = self.master_db.add(masterdb.Alert, data)
        self.notify_CPV1('add', alert)

    def bd_md_alert_add(self, data, route_meta):
        self.send_alert(data)

    def bd_md_alert_viewed(self, data, route_meta):
        def __view_alert(_id=None, alert=None):
            if _id:
                alert = self.master_db.session.query(masterdb.Alert)\
                        .filter(masterdb.Alert.id==data['alert_id'])\
                        .first()
            if alert:
                alert.viewed = True
                self.master_db.session.commit()
                return alert

        if 'alert_id' in data.keys():
            alert = __view_alert(data['alert_id'])
            if alert:
                #a = self.master_db.as_json(alert, cols=masterdb.NOTIFY_ALERT_COLS)
                self.notify_CPV1('delete', alert)

        if 'alert_ids' in data.keys():
            alerts = self.master_db.session.query(masterdb.Alert)\
                    .filter(masterdb.Alert.id.in_(data['alert_ids']))\
                    .all()

            alert_data = []
            for alert in alerts:
                viewed = __view_alert(alert=alert)
                if viewed:
                    alert_data.append(viewed)
                    #alert_data.append(self.master_db.as_json(viewed, cols=masterdb.NOTIFY_ALERT_COLS))

            self.notify_CPV1('delete', alert_data, )


    def bd_md_alert_remove(self, data, route_meta):
        alert = self.master_db.session.query(masterdb.Alert)\
                .filter(masterdb.Alert.id==data['alert_id'])\
                .first()

        if alert:
            self.master_db.delete(alert)
            self.notify_CPV1('delete', alert, model_cols=['id']) 

    def bd_md_slave_job_add(self, data, route_meta):
        job = self.__slave_job_add(data)

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

            self.notify_CPV1('add', job)

    def slave_job_error(self, job_id, msg='Job had an unexpected error', task_msg=None, stage='Failed', notify=True):
        if not task_msg:
            task_msg = msg

        job = self.master_db.session.query(masterdb.SlaveJob)\
                .filter(masterdb.SlaveJob.id==job_id)\
                .first()
        if job:
            job.error = True
            job.completed = False 
            job.msg = msg
            job.stage = stage

            self.master_db.session.commit()

            for job_task in job.tasks:
                if not job_task.error:
                    self.slave_task_error(job_task.id, task_msg, check_job=False, notify=notify)

            if notify:
                self.notify_CPV1('add', job)

    def bd_md_slave_job_error(self, data, route_meta):
        msg = data.get('msg')
        self.slave_job_error(data['job_id'], msg)

    def bd_md_slave_job_stop(self, data, route_meta):
        job = self.master_db.session.query(masterdb.SlaveJob)\
                .filter(masterdb.SlaveJob.id==data['job_id'])\
                .first()

        if job:
            if not job.completed and not job.error:
                if 'msg' not in data.keys():
                    data['msg'] = 'Job stopped'

                self.slave_job_error(job.id, msg=data['msg'], task_msg='Stopped!', stage='Stopped!')



    def __slave_job_add(self, data):
        job = self.master_db.add(masterdb.SlaveJob, data['job_data'])
        tasks = []


        if 'tasks_data' in data.keys():
            for task in data['tasks_data']:
                task['job_id'] = job.id
                task_obj = self.__slave_task_add(task)
                tasks.append(task_obj)
            self.notify_CPV1('add', job)

        return job, tasks

    def __slave_job_completed(self, job, msg='OK', notify=True):
        if not type(job) == masterdb.SlaveJob:
            job = self.master_db.session.query(masterdb.SlaveJob)\
                    .filter(masterdb.SlaveJob.id==job)\
                    .first()

        if job:
            job.completed = True
            job.stage = 'Done!'
            job.msg = msg
            self.master_db.session.commit()

            if notify:
                self.notify_CPV1('add', job)

            if job.send_alert:
                self.send_alert({'msg': 'Job {} is finished!'.format(job.id), 'go_to':'job/'+str(job.id)})
            return job

    def bd_md_slave_job_completed(self, data, route_meta):
        job = self.__slve_job_completed(data['job_id'])

        
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
                 
                if task: 
                    t1 = self.master_db.as_json(task, cols=masterdb.NOTIFY_TASK_COLS)
                    self.notify_CPV1('add', task)

                    if chaining:
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
        cb_name = None
        cb_tag = None
        if 'cb_route' in data.keys():
            cb_route = data['cb_route']
            cb_name = data['cb_name'] 
            cb_tag = self.taskrunner_create_address_tag
    
        task_group_args = {
            'total_cnt': cnt,
            'job_id': data['job_id'],
            'cb_route': cb_route,
            'cb_name': cb_name,
            'cb_data_tag': cb_tag
        }

        task_group = self.master_db.add(masterdb.SlaveTaskGroup, task_group_args)

        self.notify_CPV1('add', task_group)

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
   
    def slave_task_error(self, task_id, msg, check_job=True, notify=True):
        task = self.master_db.session.query(masterdb.SlaveTask)\
                .filter(masterdb.SlaveTask.id==task_id)\
                .first()


        if task.started and (not task.completed or not task.error):
            stop_msg = create_local_task_message(
                'bd.@sd.task.global.stop',
                {'task_id': task.id}
            )

            slave = self.master_db.session.query(masterdb.Slave)\
                    .filter(
                        masterdb.Slave.id==task.assigned_slave_id,
                        masterdb.Slave.active==True
                    )\
                    .first()

            if slave:
                    self.send_message_to(slave.uuid, msg)

        task.error = True
        task.msg = msg
        self.master_db.session.commit()

        if notify:
            self.notify_CPV1('add', task)

        if task.job_id and check_job:
            if not task.job_ok_on_error and task.job_ok_on_error != None:
                err_msg = "Job was stopped because task '{}' failed".format(task.id)
                self.slave_job_error(task.job_id, msg=err_msg, stage='Failed', notify=notify)

            elif task.task_group_id:
                task_group = self.__slave_group_task_update_cnt(task.task_group_id, 0)
                if notify:
                    self.notify_CPV1('add', task)
                    #group_tasks = self.master_db.session.query(SlaveTask)\
                    #                .filter(masterdb.SlaveTask.task_group_id==task_group.id)\
                    #                .all()

                    #for gt in group_tasks:
                    #    gt.
                    #    pass

        self.master_db.session.commit()

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

        self.notify_CPV1('add', task_group)
        self.master_db.session.commit()
        return task_group


    def bd_md_slave_task_completed(self, data, route_meta):
        task = self.master_db.session.query(masterdb.SlaveTask)\
                .filter(masterdb.SlaveTask.id==data['task_id'])\
                .first()

        if not task:
            raise ValueError("Task id doesn't exist")

        task.completed = True
        task.time_completed = datetime.strptime(data['time_completed'], "%Y-%m-%d %H:%M:%S.%f")


        if task.job_id:
            job_tasks = self.master_db.session.query(masterdb.SlaveTask)\
                    .filter(masterdb.SlaveTask.job_id == task.job_id).all()

            if job_tasks:
                job_done = True 
                for jt in job_tasks:
                    if not jt.completed:
                        job_done = False
                        break

                if job_done:
                    job = self.__slave_job_completed(task.job_id)


        if task.task_group_id:
            task_group = self.__slave_group_task_update_cnt(task.task_group_id,1)

        if 'msg' in data.keys():
            task.msg = data['msg']
        self.master_db.session.commit()
        self.notify_CPV1('add', task)

    def bd_md_slave_task_schedule_add(self, data, route_meta):
        #print 'Adding schedule for {}'.format(data['scheduler']['route'])
        ######
        #### DOESNT ACCOUNT FOR DIFFERENT WEEKDAY IN DIFFERENT TIMEZONES
        ######
        weekdays = {'monday':0, 'tuesday':1, 'wednesday':2, 'thursday':3, 'friday':4, 'saturday':5, 'sunday':6}

        name = data['name']
        print "ADDING NAME: {}\n\n\n\n\n".format(name)

        scheduler_group = self.master_db.add(masterdb.SchedulerGroup, {'name': name})

        schedulers = []

        if not scheduler_group:
            raise Exception("Could not create scheduler_group")

        if data['type'] == 'scheduler':
            scheduler_payloads = []
            date = datetime.utcnow()
            print  "masterdriver bd_md_slave_task_schedule_add-->\n {}...{}\n\n\n\n".format(weekdays[data['days'][0]], date.weekday())
            for day in data['days']:
                d = datetime.utcnow() 

                payload = {}
                target_day_idx = weekdays[day]
                while (target_day_idx!=d.weekday()-1):
                    d+=timedelta(days=1)

                d = d.replace(hour=int(data['clock'].split(':')[0]), minute=int(data['clock'].split(':')[1]))
                payload['run_time'] = d 
                payload['frequency_min'] =  60*24*7 
                payload['route'] = data['route']
                payload['scheduler_group_id'] = scheduler_group.id
                scheduler = self.master_db.add(masterdb.Scheduler, payload)
                if not scheduler:
                    db.session.rollback()
                    raise ValueError("SOME ISSUE SCHEDULER RETURNED NONE")

                schedulers.append(scheduler)
            

        elif data['type'] == 'timer':
            payload = {}

            payload['route']  = data['route'] 
            multiplier = 1
            if data['frequency_unit'] == 'minutes':
                pass
            elif data['frequency_unit'] == 'hours':
                multiplier = 60
            elif data['frequency_unit'] == 'days':
                multiplier = 60 * 24
            else:
                raise ValueError(" Freq unit is not in system: {}".format(data['frequency_unit']))

            freq_min = multiplier * data['frequency']
            run_time = datetime.utcnow() + timedelta(minutes=freq_min)

            if 'repeat' in data.keys():
                if data['repeat']:
                    payload['frequency_min'] = freq_min 
                    payload['run_time'] = run_time 
                else:
                    payload['run_time'] = run_time 
            else:
                payload['run_time'] = run_time 

            payload['scheduler_group_id'] = scheduler_group.id

            schedulers.append(self.master_db.add(masterdb.Scheduler, payload))

        self.notify_CPV1('add', scheduler_group)
        self.notify_CPV1('add', schedulers)

    def bd_md_slave_task_schedule_remove(self, data, route_meta):
        scheduler_group_id = None
        schedulers = []

        if 'scheduler_group_id' in data.keys():
            scheduler_group_id = data['scheduler_group_id']

        if 'scheduler_id' in data.keys():
            scheduler = self.master_db.session.query(masterdb.Scheduler)\
                    .filter(masterdb.Scheduler.id==data['scheduler_id'])\
                    .first()

            scheduler_group_id = scheduler.scheduler_group_id

        schedulers = self.master_db.session.query(masterdb.Scheduler)\
                .filter(masterdb.Scheduler.scheduler_group_id==scheduler_group_id)\
                .all()

        scheduler_group = self.master_db.session.query(masterdb.SchedulerGroup)\
                    .filter(masterdb.SchedulerGroup.id==scheduler_group_id)\
                    .first()

        if scheduler_group:
            for scheduler in schedulers:
                self.master_db.delete(scheduler)
            self.master_db.delete(scheduler_group)
            self.notify_CPV1('delete', schedulers)
            self.notify_CPV1('delete', scheduler_group)


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
            slave.active = True
            slave.last_pulse = datetime.utcnow()
            self.notify_CPV1('add', slave)

            slave_type = str(slave.slave_type_id)
            if slave_type in self.needed_slaves.keys():
                if len(self.needed_slaves[slave_type]>0):
                    tasks = self.master_db.session.query(masterdb.SlaveTask).filter(masterdb.SlaveTask.id._in(self.needed_slaves[slave_type])).all()
                    for task in tasks:
                        task.active = True
                        t = self.master_db.as_json(task, cols=masterdb.NOTIFY_SLAVE_TASK_COLS)
                    self.master_db.session.commit()
                else:
                    del self.needed_slaves[slave_type]

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
        slave.last_pulse = datetime.utcnow()

        self.notify_CPV1('add', slave)

        print 'SLAVES AUTOMATICALLY SET AS NON EC2 instances'


    def bd_md_slave_lost(self, data, route_meta):
        slave = self.master_db.session.query(masterdb.Slave)\
                .filter(masterdb.Slave.uuid==data['uuid'])\
                .first()
        if slave:
            slave.active = False
            self.master_db.session.commit()
            s = self.master_db.as_json(slave, cols=masterdb.NOTIFY_SLAVE_COLS)
            self.notify_CPV1('add', slave)

            #this handles all tasks assigned
            tasks = self.master_db.session.query(masterdb.SlaveTask)\
                    .filter(and_(masterdb.SlaveTask.assigned_slave_id==slave.id, masterdb.SlaveTask.completed==False))\
                    .all()
            for task in tasks:
                if task.started and not (task.completed or task.error):
                    self.slave_task_error(task.id, 'Slave {} was lost'.format(slave.id))
                else:
                    task.assigned_slave_id==None
                    self.master_db.session.commit()

    def bd_md_Slave_CPV1_notify(self, data, route_meta):
        self.notify_CPV1(data['action'], data['action_data'], session_id=data.get('session_id'), slave_uuid=data.get('slave_uuid'), update=False)

    def bd_md_Slave_APV1_forwarded(self, data, route_meta):
        #Remove
        raise NotImplemented

    def bd_md_Slave_CPV1_forwarded(self, data, route_meta):
        self.router_msg_process(data, origin=route_meta['origin'])

    def bd_md_Slave_CPV1_get_lastupdateid(self, data, route_meta):
        uuid = route_meta['origin']
        data = {'last_update': self.CPV1_notify_msg_id}

        msg = create_local_task_message('bd.sd.@CPV1.set.last-update-id', data)
        self.send_message_to(uuid, msg)

    def bd_md_Slave_CPV1_get_objs(self, data, route_meta):
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
    
        obj_class = getattr(masterdb, obj_type)

        args = [getattr(obj_class, key)==value for key, value in obj_args.items()]

        objs = self.master_db.session.query(obj_class)\
                .filter(*args)\
                .offset(cnt)\
                .limit(per)\
                .all()

        self.notify_CPV1('add', objs, session_id=sid, slave_uuid=uuid, update=False)


    def bd_md_Slave_CPV1_sendall(self, data, route_meta):
        print 'Sending all data to CPV1: {}'.format(data['uuid'])
        sid = data['sid']
        uuid = data['uuid']

        jobs = self.master_db.session.query(masterdb.SlaveJob)\
                .order_by(masterdb.SlaveJob.id.desc())\
                .limit(100)\
                .all()

        job_ids = []

        self.notify_CPV1('add', jobs, session_id=sid, slave_uuid=uuid, update=False)
        
        scheduler_groups = self.master_db.session.query(masterdb.SchedulerGroup).all()
        scheduler_data = []

        for group in scheduler_groups:
            schedulers = self.master_db.session.query(masterdb.Scheduler)\
                    .filter(
                        masterdb.Scheduler.scheduler_group_id == group.id
                    )\
                    .all()
            scheduler_data+=schedulers

        self.notify_CPV1('add', scheduler_groups, session_id=sid, slave_uuid=uuid, update=False)
        self.notify_CPV1('add', scheduler_data, session_id=sid, slave_uuid=uuid, update=False) 

        slave_types = self.master_db.session.query(masterdb.SlaveType).all()

        self.notify_CPV1('add', slave_types, session_id=sid, slave_uuid=uuid, update=False)

        slaves = self.master_db.session.query(masterdb.Slave)\
                .filter(masterdb.Slave.active==True)\
                .all()

        self.notify_CPV1('add', slaves, session_id=sid, slave_uuid=uuid, update=False)

        alerts = self.master_db.session.query(masterdb.Alert)\
                .filter(masterdb.Alert.viewed==False)\
                .all()
        self.notify_CPV1('add', alerts, session_id=sid, slave_uuid=uuid, update=False)

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


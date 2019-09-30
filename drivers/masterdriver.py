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
    CPV1_alert_npersist_id = 0
    ec2_resource = None

    USE_AWS_RESOURCES = 0 
    AWS_SLAVES_ONDEMAND = 0 

    highest_job_task_priority = 0
    BOT_TYPE = "MASTER"

    def setup_aws(self):
        if self.aws_key and self.aws_secret:
            self.ec2_resource = boto3.resource(
                'ec2',
                region_name='us-west-2',
                aws_access_key_id=self.aws_key,
                aws_secret_access_key=self.aws_secret
            )
            self.USE_AWS_RESOURCES = 1 

    def __init__(self, config):
        ### SET MASTERDRIVER SETTINGS FROM CONFIG HERE
        super(MasterDriver, self).__init__(config)

        self.port = config['server_port'] 
        self.host = config['server_host']

        self.check_funcs.extend([ 
            [self.check_slave_pulses, 500],
            [self.check_slave_tasks, 100],
            [self.check_slave_task_groups, 3000],
            [self.check_slave_chainers, 3000],
            [self.check_slave_schedules, 1000],
            [self.check_CPV1_packet_queue, 1000],
            [self.check_aws_slaves, 5000]
        ])


        self.aws_key = config.get('aws_access_key')
        self.aws_secret = config.get('aws_secret_key')
        self.AWS_SLAVES_ONDEMAND = config.get('aws_slaves_ondemand')
        self.setup_aws()
       
        
        self.needed_bots = {}
        self.needed_slaves = []

        self.launch_slave_types = []

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
            'bd.@md.slave.job.pause': self.bd_md_slave_job_pause,
            'bd.@md.slave.job.error': self.bd_md_slave_job_error,

            'bd.@md.slave.task.add': self.bd_md_slave_task_add,
            'bd.@md.slave.task.reject': self.bd_md_slave_task_reject,
            'bd.@md.slave.task.remove': self.bd_md_slave_task_remove,
            'bd.@md.slave.task.error': self.bd_md_slave_task_error,
            'bd.@md.slave.task.started': self.bd_md_slave_task_started,
            'bd.@md.slave.task.stopped': self.bd_md_slave_task_stopped,

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
            'bd.@md.Slave.CPV1.user.auth': self.bd_md_Slave_CPV1_user_auth,
            'bd.@md.Slave.CPV1.get.objs': self.bd_md_Slave_CPV1_get_objs,
            'bd.@md.Slave.CPV1.alert': self.bd_md_Slave_CPV1_alert,
            'bd.@md.Slave.CPV1.get.last-update-id': self.bd_md_Slave_CPV1_get_lastupdateid,
            'bd.@md.Slave.CPV1.forwarded': self.bd_md_Slave_CPV1_forwarded,
            'bd.@md.Slave.CPV1.ping': self.bd_md_Slave_CPV1_ping,
            'bd.@md.Slave.CPV1.sendall': self.bd_md_Slave_CPV1_sendall,
            'bd.@md.Slave.CPV1.notify': self.bd_md_Slave_CPV1_notify,
            'bd.@md.Slave.CPV1.master-configs.get': self.bd_md_Slave_CPV1_master_configs_get,
            'bd.@md.Slave.CPV1.master-configs.set': self.bd_md_Slave_CPV1_master_configs_set


        }
        self.add_command_mappings(maps)
        self.slave_tasks = MPPriorityQueue(2)
        
        self.CPV1_grace_minutes = 120 #change this later
        self.CPV1_pinged_times = {} 

        self.CPV1_packet_queue = [] 

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
                {'name': 'Amazon Price Tracker', 'model_id': 'APTV1'},
                {'name': 'Warehouse Clerk', 'model_id': 'WCV1'},
                {'name': 'Binance Trader', 'model_id': 'BTV1'},
                {'name': 'Alcatraz Ticket Bot', 'model_id': 'ATBV1'}
            ]

            for a in args:
                self.master_db.add(masterdb.SlaveType, a)

            root_perm_group = { 

                'name': 'Root Group',
                'job_read': True, 'job_write': True,
                'job_execute': True, 'master_read': True,
                'master_write': True, 'master_execute': True,
                'slave_read': True, 'slave_write': True,
                'slave_execute':True, 'warehouse_read': True,
                'warehouse_write': True, 'warehouse_execute': True
            }
            self.master_db.add(masterdb.PermissionGroup, root_perm_group)

            root_arg = { 'username': 'root', 'password': 'root', 'permission_group_id': 1}
            self.master_db.add(masterdb.User, root_arg)
       
        else:
            self.master_db.session.rollback()
            jobs = self.master_db.session.query(masterdb.SlaveJob)\
                    .filter(
                        masterdb.SlaveJob.error==False,
                        masterdb.SlaveJob.completed==False
                    )\
                    .all()


            slaves = self.master_db.session.query(masterdb.Slave)\
                    .filter(masterdb.Slave.active == True)\
                    .all()

            for slave in slaves:
                if slave.is_ec2:
                        self.terminate_ec2_instance([slave.ec2_instance_id])

                slave.active = False

            self.master_db.session.commit()

            for job in jobs:
                self.slave_job_error(job.id, 'Master closed abruptly before jobs completed', notify=False)


    def get_configs(self):
        return {
            "aws_key": self.aws_key,
            "aws_secret":'*****************',
            "USE_AWS": self.USE_AWS_RESOURCES,
            "AWS_SLAVES_ONDEMAND": self.AWS_SLAVES_ONDEMAND
        }

    def bd_md_Slave_CPV1_user_auth(self, data, route_meta):
        u = self.master_db.session.query(masterdb.User)\
                .filter(masterdb.User.username == data['username'])\
                .first()
        rdata = {'worked':False}
        if u:
            rdata['worked']=True
            rdata['key'] = 'secret_key'

        self.taskrunner_send_data_to_tag(data['tag'], rdata)

    def bd_md_Slave_CPV1_master_configs_get(self, data, route_meta):
        sid = data['sid']
        uuid = data['uuid']

        self.notify_CPV1('master.config.add', self.get_configs(), session_id=sid, slave_uuid=uuid, update=False)

    def bd_md_Slave_CPV1_master_configs_set(self, data, route_meta):
        sid = data['sid']
        uuid = route_meta['origin']
        cols = data.keys()

        print "n\n\n\n\nCOLS: {}".format(cols)
        self.USE_AWS_RESOURCES = int(data['USE_AWS'])
        self.AWS_SLAVES_ONDEMAND = int(data['AWS_SLAVES_ONDEMAND'])
        
        if data['aws_key'] != self.aws_key:
            if '*' in data['aws_secret']:
                self.alert_CPV1("Must set aws_secret", session_id=sid, slave_uuid=uuid)
            else:
                self.aws_key = data['aws_key']
                self.aws_secret = data['aws_secret']
                self.setup_aws()
        self.notify_CPV1('master.config.add', self.get_configs())
        self.alert_CPV1('Successfully set master configurations')


        if self.USE_AWS_RESOURCES and self.AWS_SLAVES_ONDEMAND:
            self.launch_slave_types = [] 

    def launch_ec2_instance(self, slave_type_id):
        if not self.USE_AWS_RESOURCES:
            self.alert_CPV1("Add AWS Api key info in configurations tab")
            return


        if not self.AWS_SLAVES_ONDEMAND:
            self.alert_CPV1("Please enable AWS_SLAVES_ONDEMAND to allow tasks to be completed automatically", go_to='/bots?tab=master')
            return


        slave_uuid = self.create_bot_uuid()
        slave_key = json.dumps({'uuid': slave_uuid})

        slave_type = self.master_db.session.query(masterdb.SlaveType)\
                .filter(masterdb.SlaveType.id==slave_type_id)\
                .first()
        
        if not slave_type:
            raise KeyError("Slave type '{}' does not exist".format(slave_type.id))

        model_id = slave_type.model_id
        user_data = '''#!/bin/bash
        apt-get update
        apt-get install python -y
        apt-get install python-pip -y

        cd /home/ubuntu 
        echo '{}' > .slave_key.json
        wget https://bn1-bucket.s3-us-west-2.amazonaws.com/lib/bn1_lib.tar.gz
        tar -zxvf bn1_lib.tar.gz
        wget https://bn1-bucket.s3-us-west-2.amazonaws.com/configs/{}_config.json -O BN1/run_deployed/config.json
        pip install -r BN1/reqs/{}_reqs.txt 
        bash BN1/slaves/{}/setup.sh
        cd BN1/run_deployed
        su -c "python {}_run.py" ubuntu
        '''.format(slave_key, model_id, model_id, model_id, model_id)

        ec2_slave = None
        msg = ''
        try:
            ec2_slave = self.ec2_resource.create_instances(
                ImageId='ami-0b37e9efc396e4c38',
                UserData=user_data,
                MinCount=1, MaxCount=1,
                InstanceType='t2.micro',
                SecurityGroupIds=['sg-0fc9354b97fb643c7'],
                KeyName='heyaw_key',
                TagSpecifications=[{'ResourceType': 'instance', 'Tags':[{'Key':'launch-type', 'Value': 'master'}]}]
            )[0]
        except Exception as e:
            self.USE_AWS_SERVICES = 0 
            msg = str(e)

        #gives ec2 instances 2 min grace period to init#
        ###############################################
        slave_info = {
            'is_ec2': True,
            'uuid': slave_uuid,
            'ec2_instance_id': None,
            'active': True,
            'init': True,
            'last_pulse': datetime.utcnow()+timedelta(seconds=120),
            "slave_type_id": slave_type_id 
        }

        if not ec2_slave:
            slave_info['last_pulse'] = None
            slave_info['init'] = False
            slave_info['active'] = False

        else:
           slave_info['ec2_instance_id']=ec2_slave.instance_id

        slave = self.master_db.add(masterdb.Slave, slave_info)
        if not slave:
            self.terminate_ec2_instance([ec2_slave.instance_id])
            msg = "AWS EC2 instance launched but could not add to masterdb.  Terminating ec2 instance {}".format(ec2_slave.instance_id)
            self.alert_CPV1(msg, go_to='/alerts', persist=True)
            return
        if ec2_slave:
            self.ec2_resource.create_tags(
                Resources=[ec2_slave.instance_id], 
                Tags=[
                    {'Key': 'slave.id', 'Value': str(slave.id)},
                    {'Key': 'bot.type', 'Value': 'slave'},
                    {'Key': 'slave.type', 'Value': slave_type.name},
                    {'Key': 'Name', 'Value': 'sd-'+str(slave.id)}
                ]
            )

            msg = 'Slave [{}] "{}" launched on AWS EC2'.format(slave.id, slave_type.name)
        else:
            msg = 'Slave [{}] "{}" could not be launched on EC2. Error: "{}"'.format(slave.id, slave_type.name, msg)

        self.notify_CPV1('add', slave)
        go_to = '/bots/slave/{}'.format(slave.id)
        self.alert_CPV1(msg, go_to=go_to, persist=True)

        return slave


    def terminate_ec2_instance(self, instance_ids):
        print "\n\n\nTERMINATING : {}".format(instance_ids)
        print self.ec2_resource.instances.filter(InstanceIds=instance_ids).terminate()

    def send_message_to(self, uuid, msg, priority=OUTBOX_TASK_MSG):
        payload = {'uuid': uuid, 'data': msg}
        try:
            self.outbox.put(payload, priority)
        except Exception as e:
            self.report_error("bd.md.send_message_to", str(e)) 

    def send_message(self, uuid, msg, priority=OUTBOX_TASK_MSG):
        payload = {'uuid': uuid, 'data': msg}
        self.outbox.put(payload, priority) 

    def send_global_task(self, uuid, route, data, task_id, job_id=None, route_meta={}):
        route_meta.update({'task_id':task_id, 'job_id':job_id})
        msg = {
            'route_meta': route_meta,
            'route': 'bd.@sd.task.global.start',
            'data': {
                'route': route,
                'data': data
            }
        } 
        self.send_message(uuid, msg)

    def bd_md_slave_launch(self, data, route_meta): 
        if not 'slave_type_id' in data.keys():
            st = self.master_db.session.query(masterdb.SlaveType)\
                .filter(masterdb.SlaveType.model_id==data['model_id'])\
                .first()
            data['slave_type_id'] = st.id
        slave = self.launch_ec2_instance(data['slave_type_id']) #already notified
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
            slaves = self.master_db.session.query(masterdb.Slave)\
                    .filter(masterdb.Slave.id.in_(slave_ids))\
                    .all()

            for slave in slaves:
                if slave.ec2_instance_id:
                    ec2_instance_ids.append(slave.ec2_instance_id)

        if ec2_instance_ids:
            for instance_id in ec2_instance_ids:
                slave = self.master_db.session.query(masterdb.Slave).filter(masterdb.Slave.ec2_instance_id==instance_id).first()
                if slave:
                    slave.active = False

            self.master_db.session.commit()
            self.terminate_ec2_instance(ec2_instance_ids)

        
    def bd_md_slave_pulse(self, data, route_meta):
        slave = self.master_db.session.query(masterdb.Slave).filter(masterdb.Slave.uuid == data['uuid']).first()
        if slave:
            slave.last_pulse = datetime.utcnow()
            if slave.active == False:
                msg = 'Slave [{}] recovered, back online'.format(slave.id)
                go_to = '/bots/slave/{}/'.format(slave.id)
                self.alert_CPV1(msg,go_to)
                slave.active = True

            self.master_db.session.commit()
        #Here is where it updates last pulse 

    def check_aws_slaves(self):
        slaves = self.master_db.session.query(masterdb.Slave)\
                .filter(
                    masterdb.Slave.active==True,
                    masterdb.Slave.is_ec2==True,
                    masterdb.Slave.init==False,
                    masterdb.Slave.working==False
                ).all()
        
        for slave in slaves:
            task = self.master_db.session.query(masterdb.SlaveTask)\
                    .filter(
                        masterdb.SlaveTask.assigned_slave_id==slave.id
                    ).order_by(masterdb.SlaveTask.time_completed).first()
            time_ref = slave.first_pulse

            if task:
                time_ref = task.time_completed 
                if not task.time_completed:
                    time_ref = task.time_created

            if not time_ref:
                raise ValueError("SLAVE {} HAS NOT FIRST PULSE: {}".format(slave.id, slave.first_pulse))

            if time_ref + timedelta(seconds=150) < datetime.utcnow():
                self.terminate_ec2_instance([slave.ec2_instance_id])
                self.alert_CPV1('Terminating AWS Slave [{}] due to job inactivity'.format(slave.id), persist=True)


    def check_slave_pulses(self):
        slaves = self.master_db.session.query(masterdb.Slave).filter(masterdb.Slave.active==True).all()
        
        for slave in slaves:
            if slave.working: #checks if slave is still actually working
                task = self.master_db.session.query(masterdb.SlaveTask)\
                        .filter(masterdb.SlaveTask.assigned_slave_id==slave.id)\
                        .order_by(masterdb.SlaveTask.time_started.desc())\
                        .first()

                if not task:
                    slave.working = False
                    self.master_db.session.commit()

                else:

                    if task.completed or task.error:
                        slave.working=False
                        self.master_db.session.commit()


            if (slave.last_pulse + timedelta(seconds=15)) < datetime.utcnow():
                slave.active = False
                if slave.is_ec2:
                    self.terminate_ec2_instance([slave.ec2_instance_id])
                    amsg = 'Slave [{}] is unresponsive and ec2 instance has been terminated'.format(slave.id)
                    self.alert_CPV1(amsg, go_to='bots/slave/'+str(slave.id))
                    key = str(slave.slave_type_id)
                    if  key in self.launch_slave_types:
                        self.launch_slave_types.remove(key)

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

                            msg = "Job [{}] '{}' added by Scheduler [{}] '{}'".format(job.id, job.name, group.id, group.name)
                            self.alert_CPV1(msg, go_to='/job/{}'.format(job.id))


                            if scheduler.frequency_min:
                                new_run_time = datetime.utcnow() + timedelta(minutes=scheduler.frequency_min)
                                scheduler.run_time = new_run_time
                                ###################3
                                #################notify_cpv1 with new updated times
                            else:
                                scheduler.executed = True
                                #maybe delete them or do something else

                            self.master_db.session.commit()
                            self.notify_CPV1('add', scheduler)

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

                    #cb_data = self.taskrunner_inbox_get_tag(task_group.cb_data_tag, get_all=True)

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
        #queue order is important
        tasks = self.master_db.session.query(masterdb.SlaveTask)\
                 .filter(
                     masterdb.SlaveTask.error==False,
                     masterdb.SlaveTask.assigned_slave_id.is_(None)
                 ).order_by(masterdb.SlaveTask.time_created.asc()).all()


        jobs = self.master_db.session.query(masterdb.SlaveJob)\
                .filter(
                    masterdb.SlaveJob.completed==False,
                    masterdb.SlaveJob.error==False
                ).order_by(masterdb.SlaveJob.priority.desc(), masterdb.SlaveJob.created_time).all()
        

        def live_slaves (slave_type_needed, is_working):
            not_assigned = masterdb.Slave.assigned_task_id==None
            assigned = masterdb.Slave.assigned_task_id!=None

            slaves = self.master_db.session.query(masterdb.Slave)\
                    .filter(
                        masterdb.Slave.active==True,
                        assigned if is_working else not_assigned,
                        masterdb.Slave.working==is_working,
                        masterdb.Slave.init==False,
                        masterdb.Slave.slave_type_id.in_(slave_type_needed))\
                    .all()

            slave_list = {}
            for slave in slaves:
                key = str(slave.slave_type_id)

                if slave.working==is_working: #adds non working slaves
                    if not key in slave_list.keys():
                        slave_list[key] = [] 

                    slave_list[key].append(slave) 

            return slaves, slave_list

        if len(jobs):
            self.highest_slave_job_priority = jobs[0].priority

        if len(tasks):
            slave_type_needed = []
            job_tasks_map = {}
            for task in tasks:
                if not str(task.job_id) in job_tasks_map.keys():
                    job_tasks_map[str(task.job_id)] = []

                job_tasks_map[str(task.job_id)].append(task)

                if not task.slave_type_id in slave_type_needed:
                    slave_type_needed.append(task.slave_type_id)

            slaves, free_slaves = live_slaves(slave_type_needed, False)
            slaves, working_slaves = live_slaves(slave_type_needed, True)
            if not len(slaves): #no slaves with needed type
            #handled below
                pass
            
            ASSIGNED_TASK = False

            #add a check to see if active slaves for tasks 
            keys = free_slaves.keys()
            for job in jobs:
                if ASSIGNED_TASK:
                    break

                j_tasks = None 
                try:
                    j_tasks = job_tasks_map[str(job.id)]
                except KeyError:
                    j_tasks = [] 

                for task in j_tasks:
                    key = str(task.slave_type_id)

                    if not key in keys:
                        if key in working_slaves.keys() and not self.AWS_SLAVES_ONDEMAND:
                            #waiting for next slave
                            continue
                        else:
                            #launch more bots
                            if task.active:
                                if not key in self.launch_slave_types:
                                    self.launch_slave_types.append(key)

                                    if not self.launch_ec2_instance(task.slave_type_id):
                                        st = self.master_db.session.query(masterdb.SlaveType)\
                                                .filter(masterdb.SlaveType.id==task.slave_type_id)\
                                                .first()


                                        task.msg = 'Launch slave type "{}" to start'.format(st.name)
                                        msg = 'Slave type [{}] "{}" needed to run queued tasks'.format(st.id, st.name)
                                        go_to = '/bots'
                                        self.alert_CPV1(msg, go_to, alert_color='yellow darken-2', persist=True)

                                self.master_db.session.commit()

                    elif len(free_slaves[key]):
                        l = [s.id for s in free_slaves[key]]
                        print "FREE SLAVES IN KEY: {}".format(l)
                        slave = free_slaves[key].pop(0)
                        i = 0
                        keys = free_slaves.keys() #updates keys

                        task.assigned_slave_id = slave.id
                        task.msg = 'Assigned to slave [{}]'.format(slave.id)
                        slave.working = True
                        slave.assigned_task_id = task.id

                        self.master_db.session.commit()


                        if task.job_id:
                            job = self.master_db.session.query(masterdb.SlaveJob).filter(masterdb.SlaveJob.id == task.job_id).first()
                            if job:
                                if job.stage == 'Queued':
                                    job.stage = 'Running'
                                    self.master_db.session.commit()
                                    self.notify_CPV1('add', job)



                        print "\n\n\n=================\nASSIGNING TASK {} TO SLAVE {}".format(task.id, slave.id)
                        self.send_global_task(slave.uuid, task.route, task.data, task.id, job_id=task.job_id)

                        self.notify_CPV1('add', task)
                        self.notify_CPV1('add', slave)

                        ASSIGNED_TASK = False
#                            slaves, slave_list, working_slaves = live_slave_list(slave_type_needed)


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




    ''' 
    def old_loop(self)
        self.check_slave_pulses()
        self.check_inbox()
        self.check_msg_timers()
        
        #add a time statement here to run every x seconds
        #ex run check_inbox more often than check_slave_task
        self.check_slave_tasks()
        self.check_slave_task_groups()
        self.check_slave_chainers()
        self.check_slave_schedules()
        self.check_aws_slaves()
    '''

    def comms(self):
        self.comms_on = True
        pid = os.getpid()
        self.comms_pid = pid
        self.heartbeat.__track_process__(pid, name='MasterDriver Comms', route='@bd.comms.launch')

        print 'Starting Master Comms'

        self.factory = BotServerFactory(self)

        reactor.listenTCP(self.port, self.factory)

        self.init_outbox_callback()

        self.comms_pulse_cb = task.LoopingCall(self.heartbeat.send_pulse, pid=pid)
        cp = self.comms_pulse_cb.start(5)
        cp.addErrback(prnt)
        reactor.run()

    def check_CPV1_packet_queue(self):
        packet = True
        while (len(self.CPV1_packet_queue)):
            packet = self.CPV1_packet_queue.pop(0)
            if packet:
                data, target_uuids = packet
                for uuid in target_uuids:
                    self.send_to_CPV1(data, uuid)

    def send_to_CPV1(self, data, uuid):
        msg = create_local_task_message('bd.sd.@CPV1.data.send', data)
        self.send_message_to(uuid, msg, OUTBOX_SYS_MSG)

    def send_CPV1(self, data, target_uuids=[]):
        self.CPV1_packet_queue.append([data, target_uuids])

    def alert_CPV1(self, msg,  go_to=None, persist=False, alert_color=None, session_id=None, slave_uuid=None):
        id_ = 'np-'+str(self.CPV1_alert_npersist_id)
        self.CPV1_alert_npersist_id+=1

        data = {'msg': msg, 'go_to':go_to, 'time':str(datetime.utcnow()), 'viewed': False, 'id':id_}
        if alert_color:
            data['color'] = alert_color

        alert = data

        if persist:
            alert = self.master_db.add(masterdb.Alert, data)

        self.notify_CPV1('alert.add', alert, session_id=session_id, slave_uuid=slave_uuid, update=persist)

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
            for acc_model, cols in masterdb.accessible_models:
                obj_type = type(data)
                if obj_type == acc_model:
                    if not model_cols:
                        if cols:
                            model_cols = cols
                    payload = self.master_db.as_json(data, cols=model_cols)
                    break

            if not payload:
                raise KeyError("Type '{}' is not an accessible_model".format(obj_type))


        msg_id = -1 

        if update:
            self.CPV1_notify_msg_id+=1
            msg_id = self.CPV1_notify_msg_id

        expire_time = datetime.utcnow() + timedelta(minutes=self.CPV1_grace_minutes)
        uuid_query= self.master_db.session.query(masterdb.Slave.uuid)

        if slave_uuid:
            uuid_query = uuid_query.filter(
                masterdb.Slave.uuid==slave_uuid,
                masterdb.Slave.active==True
            )
        else:
                uuid_query = uuid_query.join(
                    masterdb.SlaveType,
                    masterdb.Slave.slave_type_id==masterdb.SlaveType.id
                ).filter(
                    masterdb.Slave.active==True,
                    masterdb.SlaveType.model_id=='CPV1'
                )

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

        real_uuids = []
        for uuid in uuids:
            real_uuids.append(uuid[0])
        uuids = real_uuids 

        chunk_size = 30 
        if type(payload) == list:
            cnt = len(payload)/chunk_size
            if len(payload) % chunk_size:
                cnt+=1

            for i in xrange(0, cnt):
                chunk = payload[i*chunk_size:((i+1)*chunk_size-1)]
            
                rdata = {
                    'action_data': chunk,
                    'action': action,
                    'last_update': msg_id,
                    'sid': session_id
                }
                self.send_CPV1(rdata, uuids) 
        else:
            rdata = {
                'action_data': payload,
                'action': action,
                'last_update': msg_id,
                'sid': session_id
            }
           
            self.send_CPV1(rdata, uuids)


        '''
        for uuid in uuids:
            uuid = uuid[0]  
            chunk_size = 30 
            if type(payload) == list:
                cnt = len(payload)/chunk_size
                if len(payload) % chunk_size:
                    cnt+=1

                for i in xrange(0, cnt):
                    chunk = payload[i*chunk_size:((i+1)*chunk_size-1)]
                
                    rdata = {
                        'action_data': chunk,
                        'action': action,
                        'last_update': msg_id,
                        'sid': session_id
                    }
                    self.send_CPV1(rdata) 
            else:
                rdata = {
                    'action_data': payload,
                    'action': action,
                    'last_update': msg_id,
                    'sid': session_id
                }
               
                self.send_CPV1(rdata)
        '''

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

        if not 'job_id' in data.keys():
            job_data = {"job_data":{"name": "Anon Job (hidden will be true)", 'hidden': False}, "tasks_data":[data]} 
            job, tasks = self.__slave_job_add(job_data)
            task = tasks[0]

        else:
            task = self.master_db.add(masterdb.SlaveTask, data) 
            if task.error:
                self.slave_task_error(task.id, task.msg, check_job=True)
            job = self.master_db.session.query(masterdb.SlaveJob)\
                    .filter(masterdb.SlaveJob.id==data['job_id'])\
                    .first()
            job.task_cnt+=1
            self.master_db.session.commit()
            self.notify_CPV1('add', task)
            self.notify_CPV1('add', job)

        return task

    def bd_md_bot_message_send(self, msg, route_meta):
        self.send_message_to(msg['uuid'], msg['data'])

    def bd_md_alert_add(self, data, route_meta):
        raise NotImplemented
        #self.alert_CPV1(data)

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

            self.notify_CPV1('delete', alert_data)

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

    def slave_is_working(self, slave_id, working):
        slave = self.master_db.session.query(masterdb.Slave)\
                .filter(masterdb.Slave.id==slave_id)\
                .first()
        if slave:
            slave.working = working 
            if not working:
                slave.assigned_task_id = None 

            self.master_db.session.commit()
            self.notify_CPV1('add', slave)


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

    def bd_md_slave_job_pause(self, data, route_meta):
        job = self.master_db.session.query(masterdb.SlaveJob)\
                .filter(masterdb.SlaveJob.id==data['job_id'])\
                .first()
        if job:
            if not job.completed and not job.error:
                if 'msg' not in data.keys():
                    data['msg'] = 'Job paused'


                job.stage = 'Paused'
                tasks = self.master_db.session.query(masterdb.SlaveTask)\
                        .filter(
                            masterdb.SlaveTask.job_id==job.id,
                            masterdb.SlaveTask.started==False,
                            masterdb.SlaveTask.assigned_slave_id==None
                        )\
                        .all()
                for task in tasks:
                    task.active = False
                    task.msg = 'Job paused'


                self.master_db.session.commit()
                self.notify_CPV1('add', job)
                self.notify_CPV1('add', tasks)

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
                msg = 'Job [{}] "{}" is finished!'.format(job.id, job.name)
                self.alert_CPV1(msg, go_to='job/'+str(job.id), persist=job.send_alert)
            return job

    def bd_md_slave_job_completed(self, data, route_meta):
        job = self.__slave_job_completed(data['job_id'])

    
    def bd_md_slave_task_reject(self, data, route_meta):
        print "\n\n\n\n"
        print "REJECT DATA: {}\nREJECT ROUTE_META: {}".format(data, route_meta)

        slave = self.master_db.session.query(masterdb.Slave)\
                .filter(masterdb.Slave.uuid==route_meta['origin'])\
                .first()
        if slave:
            task = self.master_db.session.query(masterdb.SlaveTask)\
                    .filter(and_(masterdb.SlaveTask.id==data['task_id'], masterdb.SlaveTask.assigned_slave_id==slave.id))\
                    .first()

            if not (task.started or task.error):
                task.assigned_slave_id = None
                task.active = True
                task.message = 'Requeued'
                self.master_db.session.commit()
                self.notify_CPV1('add', task)


    def bd_md_slave_task_add(self, data, route_meta):
        model_id = self.get_model_id_from_route(data['task_data']['route'])

        if not model_id in ['bd', 'md', 'sd']: #to prevent adding driver sys tasks 
            slave_type_id = self.master_db.session.query(masterdb.SlaveType.id)\
                    .filter(masterdb.SlaveType.model_id==model_id)\
                    .first()

            if slave_type_id:
                slave_type_id = slave_type_id[0]

                data['task_data']['slave_type_id'] = slave_type_id
                task_args = data['task_data']

                #if 'job_id' in cols:
                #    task_args['job_id'] = data['task_data']['job_id']
                self.__slave_task_add(task_args)

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

    def bd_md_slave_task_stopped(self, data, route_meta):
        print "\n\nTASK STOP ORIGIN: {}".format(route_meta['origin'])
        task = self.master_db.session.query(masterdb.SlaveTask)\
                .filter(masterdb.SlaveTask.id==data['task_id'])\
                .first()


        if task:
            slave = self.master_db.session.query(masterdb.Slave)\
                    .filter(masterdb.Slave.uuid==route_meta['origin'])\
                    .first()

            if not slave:
                raise ValueError("Sender Slave {} not found".format(route_meta['origin']))

            if not slave.id == task.assigned_slave_id:
                raise ValueError("Sender Slave {} is not working on task being stopped. Slave {} is working on it. ".format(slave.id, task.assigned_slave_id))

            if not task.completed:
                self.master_db.session.commit()
                self.slave_task_error(task.id, 'Task Stopped', check_job=True, notify=True)

    def bd_md_slave_task_started(self, data, route_meta):
        task = self.master_db.session.query(masterdb.SlaveTask)\
                .filter(masterdb.SlaveTask.id==data['task_id'])\
                .first()

        if not task: 
            raise ValueError("Task id doesnt exist")
        
        if task.started:
            sent_msg = self.stop_slave_task(task.id, slave_uuid=route_meta['origin'])

            raise ValueError("TASK {} HAS ALREADY STARTED, CANT START ONCE STARTED. Notified Slave: {} ".format(task.id), sent_msg)

        task.started = True
        task.time_started = datetime.strptime(data['time_started'], "%Y-%m-%d %H:%M:%S.%f")
        self.master_db.session.commit()

        self.notify_CPV1('add', task)

        self.slave_is_working(task.assigned_slave_id, True)

    def stop_slave_task(self, task_id, slave_uuid=None, slave_id=None, slave=None):
            stop_msg = create_local_task_message(
                'bd.@sd.task.global.stop',
                {'task_id': task_id}
            )
            slave_query = self.master_db.session.query(masterdb.Slave).filter(masterdb.Slave.active==True)
            if slave_id:
                slave_query = slave_query.filter(masterdb.Slave.id==slave_id)
            elif slave_uuid:
                slave_query = slave_query.filter(masterdb.Slave.uuid==slave_uuid)
            if not slave:
                slave = slave_query.first()

            if slave:
                if slave.assigned_task_id == task_id:
                    self.send_message_to(slave.uuid, stop_msg)
                    return True
            return False

    def slave_task_error(self, task_id, msg, check_job=True, notify=True):
        task = self.master_db.session.query(masterdb.SlaveTask)\
                .filter(masterdb.SlaveTask.id==task_id)\
                .first()


        if task.started and (not task.completed or not task.error):
            self.stop_slave_task(task.id, slave_id=task.assigned_slave_id)

        task.error = True
        task.msg = msg
        self.master_db.session.commit()

        if notify:
            self.notify_CPV1('add', task)
        
        RETRY_OK = True
        if task.job_id:
            job = self.master_db.session.query(masterdb.SlaveJob)\
                    .filter(masterdb.SlaveJob.id==task.job_id)\
                    .first()

            RETRY_OK = not job.error

        
        if RETRY_OK:
            if task.retry_cnt >=1:
                task_retry_payload = {}
                max_retry = 0
                name = task.name
                if task.retried_task_id:
                    task_retry_payload['retried_task_id'] = task.retried_task_id
                    parent_task = self.master_db.session.query(masterdb.SlaveTask)\
                            .filter(masterdb.SlaveTask.id==task.retried_task_id)\
                            .first()
                    name = parent_task.name
                    max_retry = parent_task.retry_cnt
                else:
                    task_retry_payload['retried_task_id'] = task.id
                    max_retry = task.retry_cnt

                cnt = task.retry_cnt - 1
                display_cnt = (max_retry-cnt)
                task_retry_payload['retry_cnt'] =cnt 
                task_retry_payload['job_id'] = task.job_id
                task_retry_payload['slave_type_id'] = task.slave_type_id
                task_retry_payload['job_ok_on_error'] = task.retry_cnt>0
                task_retry_payload['data'] = task.data
                task_retry_payload['route'] = task.route
                task_retry_payload['name'] = "{} (Retry {}/{})".format(name, display_cnt, max_retry)
                retrying_task = self.master_db.add(masterdb.SlaveTask, task_retry_payload)

                self.master_db.session.commit()
                if notify:
                    self.notify_CPV1('add', retrying_task)
                #returns before causing error with other job and tasks because there are retries left
                return

            elif task.retry_cnt==0:
                #no retries left so task continues with func
                check_job = True
                task.job_ok_on_error = False
                pass

        if task.job_id and check_job:
            if not task.job_ok_on_error: 
                err_msg = "Job was stopped because task [{}] failed".format(task.id)
                self.slave_job_error(task.job_id, msg=err_msg, stage='Failed', notify=notify)

            elif task.task_group_id:
                task_group = self.__slave_group_task_update_cnt(task.task_group_id, 0)
                if notify:
                    self.notify_CPV1('add', task)
                    #group_tasks = self.master_db.session.query(masterdb.SlaveTask)\
                    #                .filter(masterdb.SlaveTask.task_group_id==task_group.id)\
                    #                .all()

                    #for gt in group_tasks:
                    #    gt.
                    #    pass

        self.slave_is_working(task.assigned_slave_id, False)

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
                    if not (jt.completed or jt.error):
                        job_done = False
                        break

                if job_done:
                    job = self.__slave_job_completed(task.job_id)


        if task.task_group_id:
            task_group = self.__slave_group_task_update_cnt(task.task_group_id,1)

        if 'msg' in data.keys():
            task.msg = data['msg']

        self.slave_is_working(task.assigned_slave_id, False)
        self.master_db.session.commit()
        self.notify_CPV1('add', task)

    def bd_md_slave_task_schedule_add(self, data, route_meta):
        #print 'Adding schedule for {}'.format(data['scheduler']['route'])
        ######
        #### DOESNT ACCOUNT FOR DIFFERENT WEEKDAY IN DIFFERENT TIMEZONES
        ######
        weekdays = {'monday':0, 'tuesday':1, 'wednesday':2, 'thursday':3, 'friday':4, 'saturday':5, 'sunday':6}

        name = data['name']

        scheduler_group = self.master_db.add(masterdb.SchedulerGroup, {'name': name})

        schedulers = []

        if not scheduler_group:
            raise Exception("Could not create scheduler_group")

        if data['type'] == 'scheduler':
            scheduler_payloads = []
            date = datetime.utcnow()
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
        print "\n\nSLAVE {} IS TRYING TO CONNECT".format(data['uuid'])
        slave = self.master_db.session.query(masterdb.Slave).filter(masterdb.Slave.uuid==data['uuid']).first()
        if not slave: #if slave is not registered, it asks for authentication
            print "REQUESTING AUTHENTICATION FROM SLAVE"
            msg = create_local_task_message(
                'bd.@sd.slave.auth',
                {
                    'master_uuid':self.uuid,
                     'pk': 'private_key'
                }
            )
            self.send_message(data['uuid'], msg)
        else:
            print "Resuming previous session with Slave Type {}  {}".format(slave.slave_type_id, data['uuid'])


            slave.active = True
            if not slave.first_pulse:
                slave.first_pulse = datetime.utcnow()
            slave.last_pulse = datetime.utcnow()
            if slave.is_ec2 and slave.init:
                slave.init = False

            if slave.working:
                slave.working = False
                tasks = self.master_db.session.query(masterdb.SlaveTask)\
                        .filter(
                            masterdb.SlaveTask.assigned_slave_id==slave.id,
                            masterdb.SlaveTask.completed==False,
                            masterdb.SlaveTask.error==False
                        ).all()
                for task in tasks:
                    self.slave_task_error(task.id, "Slave [{}] lost connection during task".format(slave.id), check_job=True, notify=True)

            self.master_db.session.commit()
            self.notify_CPV1('add', slave)

            st = self.master_db.session.query(masterdb.SlaveType)\
                    .filter(masterdb.SlaveType.id==slave.slave_type_id)\
                    .first()

            if st.model_id=='CPV1':
                msg = create_local_task_message(
                    'bd.sd.@CPV1.set.last-update-id',
                    {'last_update':self.CPV1_notify_msg_id}
                )

                self.send_message_to(slave.uuid, msg)

            slave_type = str(slave.slave_type_id)

            if slave_type in self.launch_slave_types:
                tasks = self.master_db.session.query(masterdb.SlaveTask)\
                        .filter(
                            masterdb.SlaveTask.id==slave.slave_type_id,
                            masterdb.SlaveTask.active == False,
                            masterdb.SlaveTask.completed == False,
                            masterdb.SlaveTask.error == False
                        ).all()

                for task in tasks:
                    task.active = True

                self.master_db.session.commit()
                self.launch_slave_types.remove(slave_type) #Removes the need for this slave_type

                self.notify_CPV1('add', tasks)

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
        data['first_pulse'] = datetime.utcnow()
        data['last_pulse'] = datetime.utcnow() + timedelta(seconds=15)
        slave = self.master_db.add(masterdb.Slave, data)

        if slave:
            print "Registered Slave Type {} id {}".format(slave_type.name, slave_type.id)
            self.notify_CPV1('add', slave)

            print 'SLAVES AUTOMATICALLY SET AS NON EC2 instances'
        else:
            raise ValueError("Could not register slave")


    def bd_md_slave_lost(self, data, route_meta):
        slave = self.master_db.session.query(masterdb.Slave)\
                .filter(masterdb.Slave.uuid==data['uuid'])\
                .first()

        if slave:
            slave.active = False
            if slave.is_ec2:
                self.terminate_ec2_instance([slave.ec2_instance_id])
            #this handles all tasks assigned
            if slave.working:
                slave.working = False
                if slave.assigned_task_id:
                    task = self.master_db.session.query(masterdb.SlaveTask)\
                            .filter(masterdb.SlaveTask.id==slave.assigned_task_id)\
                            .first()
                    slave.assigned_task_id = None
                    if task: 
                        if task.started:
                            self.slave_task_error(task.id, 'Slave [{}] was lost'.format(slave.id))

                        if not task.time_started:
                            task.assigned_slave_id==None
                            self.notify_CPV1('add', task)

            self.master_db.session.commit()
            self.notify_CPV1('add', slave)

    def bd_md_Slave_CPV1_notify(self, data, route_meta):
        self.notify_CPV1(data['action'], data['action_data'], session_id=data.get('session_id'), slave_uuid=data.get('slave_uuid'), update=False)

    def bd_md_Slave_CPV1_alert(self, data, route_meta):
        keys = data.keys()
        go_to = None
        persist = False
        session_id = None
        uuid = None
        msg = data['msg']
        if 'go_to' in keys:
            go_to = data['go_to']

        if 'persist' in keys:
            persist = data['persist']

        if 'session_id' in keys:
            session_id = data['session_id']

        if 'uuid' in keys:
            uuid = data['uuid']

        self.alert_CPV1(msg, go_to=go_to, persist=persist, session_id=session_id, slave_uuid=uuid)

    def bd_md_Slave_APV1_forwarded(self, data, route_meta):
        #Remove
        raise NotImplemented

    def bd_md_Slave_CPV1_forwarded(self, data, route_meta):
        cpv1_cmd_id = None
        sid = data['sid']
        sid_info =  data['sid'].split('#')
        if len(sid_info)>1:
            cpv1_cmd_id = sid_info[1]
            sid = sid_info[0]
            if 'cmd_msg' in data.keys():
                #msg = data['cmd_msg']
                #cmd_msg = "Processing: {}".format(msg)
                cmd_msg = data['cmd_msg']
                self.alert_CPV1('[Received] ' + cmd_msg, alert_color='green lighten-1', slave_uuid=data['uuid'], session_id=sid)

        self.router_msg_process(data, origin=route_meta['origin'])

    def bd_md_Slave_CPV1_get_lastupdateid(self, data, route_meta):
        print "SENDING CPV1 LASTUPDATEID: {}\n\n".format(self.CPV1_notify_msg_id)
        uuid = route_meta['origin']
        data = {'last_update': self.CPV1_notify_msg_id}

        msg = create_local_task_message('bd.sd.@CPV1.set.last-update-id', data)
        self.send_message_to(uuid, msg)

    def bd_md_Slave_CPV1_get_objs(self, data, route_meta):
        sid = data['sid']
        uuid = data['uuid']
        cnt = data.get('cnt')
        per = data.get('per')

        if not cnt:
            cnt = 0
        if not per or per > 30:
            per = 10

        offset = cnt * per
        obj_type = data['obj_type']
        obj_args = data.get('obj_args')

        if obj_args:
            pass
            #obj_id = obj_args.get('id')
    
        obj_class = getattr(masterdb, obj_type)

        args = None
        if obj_args:
            args = [getattr(obj_class, key)==value for key, value in obj_args.items()]

        objs_query = self.master_db.session.query(obj_class)

        if args:
            objs_query = objs_query.filter(*args)

        objs = objs_query\
            .offset(offset)\
            .limit(per)\
            .all()


        print "\n\n\nObjS: {}".format(objs)

        self.notify_CPV1('add', objs, session_id=sid, slave_uuid=uuid, update=False)


    def bd_md_Slave_CPV1_sendall(self, data, route_meta):
        print 'Sending all data to CPV1: {}'.format(data['uuid'])
        sid = data.get('sid')
        uuid = data['uuid']

        date = datetime.utcnow() - timedelta(hours=24)
        jobs = self.master_db.session.query(masterdb.SlaveJob)\
                .filter(masterdb.SlaveJob.hidden==False)\
                .filter(
                    or_(
                        masterdb.SlaveJob.created_time>=date,
                        and_(masterdb.SlaveJob.completed, not masterdb.SlaveJob.error)
                    )
                )\
                .order_by(masterdb.SlaveJob.id.desc())\
                .all()
        '''
        jobs = self.master_db.session.query(masterdb.SlaveJob)\
                .filter(masterdb.SlaveJob.hidden==False)\
                .order_by(masterdb.SlaveJob.id.desc())\
                .all()
            #.limit(10)\
        '''
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

        offline_slave_show_grace = datetime.utcnow() - timedelta(minutes=10)
        slaves = self.master_db.session.query(masterdb.Slave)\
                .filter(
                    or_(
                        masterdb.Slave.active==True,
                        masterdb.Slave.last_pulse>offline_slave_show_grace
                    )
                )\
                .all()

        self.notify_CPV1('add', slaves, session_id=sid, slave_uuid=uuid, update=False)

        alerts = self.master_db.session.query(masterdb.Alert)\
                .filter(masterdb.Alert.viewed==False)\
                .order_by(masterdb.Alert.id.desc())\
                .limit(50)\
                .all()

        self.notify_CPV1('add', alerts, session_id=sid, slave_uuid=uuid, update=False)
        self.notify_CPV1('master.config.add', self.get_configs(), session_id=sid, slave_uuid=uuid, update=False)

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


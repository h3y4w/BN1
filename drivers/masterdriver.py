from botdriver import BotDriver, prnt
import os
from comms.server import BotServerFactory

from mpinbox import  INBOX_SYS_MSG, INBOX_TASK1_MSG, INBOX_TASK2_MSG, INBOX_BLOCKING_MSG, INBOX_SYS_CRITICAL_MSG, OUTBOX_SYS_MSG, OUTBOX_TASK_MSG

from mpinbox import MPPriorityQueue, create_local_task_message 

from db import db
from db import masterdb
from sqlalchemy import func, desc, and_, or_

from datetime import datetime, timedelta

from twisted.internet import reactor, protocol, task

import json


import boto3


class MasterDriver(BotDriver):
    CPV1_notify_msg_id = 0

    def __init__(self, config):
        ### SET MASTERDRIVER SETTINGS FROM CONFIG HERE
        self.port = config['server_port'] 
        self.host = config['server_host']
        super(MasterDriver, self).__init__(config)


        #self.ec2 = boto3.resource('ec2')

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
            'bd.@md.slave.launch': None,
            'bd.@md.slave.free': None,
            'bd.@md.slave.list': None,

            'bd.@md.alert.add': self.bd_md_alert_add,
            'bd.@md.alert.viewed': self.bd_md_alert_viewed,
            'bd.@md.alert.remove': self.bd_md_alert_remove,

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
                                "job_data": {'name': 'Scheduled Job'},
                                'tasks_data': [
                                    {'name': 'Launcher', 'route': scheduler.route, 'data': scheduler.data}
                                ]
                            }
                            job, tasks = self.__slave_job_add(data)

                            msg = 'Scheduled Job {} added'.format(job.id)
                            self.send_alert({'msg': msg, 'go_to': '/job/{}'.format(job.id)})

                            if scheduler.frequency_min:
                                new_run_time = datetime.utcnow() + timedelta(minutes=scheduler.frequency_min)
                                scheduler.run_time = new_run_time
                                sc = self.master_db.as_json(scheduler, cols=masterdb.NOTIFY_SCHEDULER_COLS)
                                self.notify_CPV1('scheduler.add', sc)
                                ###################3
                                #################notify_cpv1 with new updated times
                            else:
                                scheduler.executed = True
                                #maybe delete them or do something else

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

                slave_list[key].append({'slave':slave, 'ctasks': ctasks, 'slave_id': slave.id, 'slave_type_id': slave.slave_type_id})


            #add a check to see if active slaves for tasks 

            keys = slave_list.keys()
            for task in tasks:
                key = str(task.slave_type_id)


                if key not in keys:
                    #launch more bots
                    task.active=False
                    self.master_db.session.commit()
                    alert = self.master_db.add(masterdb.Alert, {'msg': 'Slave type '+key+' needed to run task '+str(task.id), 'go_to': '/task/{}'.format(task.id)})
                    if alert:
                        print "SHIIIIT\n\n"
                        a = self.master_db.as_json(alert, cols=masterdb.NOTIFY_ALERT_COLS)
                        self.notify_CPV1('alert.add', a)


                elif len(slave_list[key]):
                    if slave_list[key][0]['ctasks'] == 0: #slaves are only assigned one task at a time
                        slave_meta = slave_list[key].pop(0)
                        keys = slave_list.keys() #updates keys
                        slave = slave_meta['slave']

                        task.assigned_slave_id = slave.id
                        task.msg = 'Assigned to slave {}'.format(slave.id)

                        if task.job_id:
                            job = self.master_db.session.query(masterdb.SlaveJob).filter(masterdb.SlaveJob.id == task.job_id).first()
                            if job:
                                if job.stage == 'Queued':
                                    job.stage = 'Running'
                                    
                                    j = self.master_db.as_json(job, cols=masterdb.NOTIFY_JOB_COLS)
                                    self.notify_CPV1('job.add', j)


                        self.master_db.session.commit()
                        
                        self.send_global_task(slave.uuid, task.route, task.data, task.id, job_id=task.job_id)

                        t = self.master_db.as_json(task, cols=masterdb.NOTIFY_TASK_COLS)
                        self.notify_CPV1('task.add', t)

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
        self.check_inbox()
        self.check_msg_timers()

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
        if not data:
            return False
        if not type(data) in [dict, list]:
            data = self.master_db.as_json(data, cols=model_cols) 

        if update:
            self.CPV1_notify_msg_id+=1
        expire_time = datetime.utcnow() + timedelta(minutes=self.CPV1_grace_minutes)
        uuid_query= self.master_db.session.query(masterdb.SlaveType.id, masterdb.Slave.uuid)

        if slave_uuid:
            uuid_query = uuid_query.filter(masterdb.Slave.uuid==slave_uuid)

        else:
                uuid_query = uuid_query.filter(
                    masterdb.SlaveType.model_id=='CPV1'
                ).join(
                        masterdb.Slave,
                        masterdb.Slave.slave_type_id==masterdb.SlaveType.id
                ).filter( masterdb.Slave.active==True)\

        uuids = uuid_query.all()

        #stored = self.CPV1_pinged_times.keys()
        
        for uuid in uuids:
            uuid = uuid[1] #second col is uuid, first col is slavetype id 
            #if not uuid in stored:
            #    self.CPV1_pinged_times[uuid] = datetime.utcnow()

            #if self.CPV1_pinged_times[uuid] < expire_time: #less than means expired 
            chunk_size = 30
            if type(data) == list and len(data) > chunk_size:
                cnt = len(data)/chunk_size
                if len(data) % chunk_size:
                    cnt+=1
                for i in xrange(0, cnt):
                    print 'sent chunk'
                    chunk = data[i*chunk_size:((i+1)*chunk_size-1)]
                

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
                        'action_data': data,
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

        return self.master_db.add(masterdb.SlaveTask, data) 
                

    def bd_md_bot_message_send(self, msg, route_meta):
        self.send_message_to(msg['uuid'], msg['data'])


    def send_alert(self, data):
        alert = self.master_db.add(masterdb.Alert, data)
        a = self.master_db.as_json(alert, cols=masterdb.NOTIFY_ALERT_COLS)
        self.notify_CPV1('alert.add', a)

    def bd_md_alert_add(self, data, route_meta):
        self.send_alert(data)

    def bd_md_alert_viewed(self, data, route_meta):
        alert = self.master_db.session.query(masterdb.Alert)\
                .filter(masterdb.Alert.id==data['alert_id'])\
                .first()
        if alert:
            alert.viewed = True
            self.master_db.session.commit()
            a = self.master_db.as_json(alert, cols=masterdb.NOTIFY_ALERT_COLS)
            self.notify_CPV1('alert.add', a)

    def bd_md_alert_remove(self, data, route_meta):
        alert = self.master_db.session.query(masterdb.Alert)\
                .filter(masterdb.Alert.id==data['alert_id'])\
                .first()
        if alert:
            self.master_db.delete(alert)
            self.notify_CPV1('alert.delete', data['alert_id']) 

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

            j = self.master_db.as_json(job, cols=masterdb.NOTIFY_JOB_COLS)
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

            j = self.master_db.as_json(job, cols=masterdb.NOTIFY_JOB_COLS)
            self.notify_CPV1('job.add', j)


    def bd_md_slave_job_stop(self, data, route_meta):
        job = self.master_db.session.query(masterdb.SlaveJob)\
                .filter(masterdb.SlaveJob.id==data['job_id'])\
                .first()
        if job:
            if not job.completed:
                job.completed = False
                job.error = True
                job.stage = 'Stopped!'
                if 'msg' not in data.keys():
                    data['msg'] = 'Job stopped'

                tasks = self.master_db.session.query(masterdb.SlaveTask)\
                        .filter(masterdb.SlaveTask.job_id==data['job_id'])\
                        .all()
                
                ts = []
                for task in tasks:
                    if task.started and (not task.completed or not task.error):
                        stop_msg = create_local_task_message(
                            'bd.@sd.task.global.stop',
                            {'task_id': task.id}
                        )
                        slave = self.master_db.session.query(masterdb.Slave)\
                                .filter(masterdb.Slave.id==task.assigned_slave_id)\
                                .first()
                        if slave:
                            self.send_message_to(slave.uuid, stop_msg)


                    if not task.error:
                        task.msg = 'Stopped!'
                        task.error = True
                        ts.append(self.master_db.as_json(task, cols=masterdb.NOTIFY_TASK_COLS))

                job.msg = data['msg'] 
                self.master_db.session.commit()


                j = self.master_db.as_json(job, cols=masterdb.NOTIFY_JOB_COLS)
                self.notify_CPV1('job.add', j)
                self.notify_CPV1('tasks.add', ts)


    def __slave_job_add(self, data):
        job = self.master_db.add(masterdb.SlaveJob, data['job_data'])
        tasks = []


        j = self.master_db.as_json(job, cols=masterdb.NOTIFY_JOB_COLS)

        if 'tasks_data' in data.keys():
            tasks_notify_data = []
            for task in data['tasks_data']:
                task['job_id'] = job.id
                task_obj = self.__slave_task_add(task)
                tasks.append(task_obj)
                d = self.master_db.as_json(task_obj, cols=masterdb.NOTIFY_TASK_COLS)
                tasks_notify_data.append(d)
                self.notify_CPV1('task.add', d)
                if task_obj.error:
                    job.error = True
                    job.msg = "Task {} failed".format(task_obj.id)

                    j = self.master_db.as_json(job, cols=masterdb.NOTIFY_JOB_COLS)
                    self.master_db.session.commit()
                    break

            self.notify_CPV1('job.add', j)

            self.notify_CPV1('tasks.add', tasks_notify_data)
        return job, tasks

    def __slave_job_completed(self, job, msg='OK'):
        if not type(job) == masterdb.SlaveJob:
            job = self.master_db.session.query(masterdb.SlaveJob)\
                    .filter(masterdb.SlaveJob.id==job)\
                    .first()

        if job:
            job.completed = True
            job.stage = 'Done!'
            job.msg = msg
            self.master_db.session.commit()

            j = self.master_db.as_json(job, cols=masterdb.NOTIFY_JOB_COLS)
            self.notify_CPV1('job.add', j)

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
                    self.notify_CPV1('task.add', t1)

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
        if 'cb_route' in data.keys():
            cb_route = data['cb_route']

        task_group_args = {
            'total_cnt': cnt,
            'job_id': data['job_id'],
            'cb_route': cb_route
        }

        task_group = self.master_db.add(masterdb.SlaveTaskGroup, task_group_args)

        tg = self.master_db.as_json(task_group, cols=masterdb.NOTIFY_TASK_GROUP_COLS)
        self.notify_CPV1('taskgroup.add', tg)

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

            t = self.master_db.as_json(task, cols=masterdb.NOTIFY_TASK_COLS)
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
        t = self.master_db.as_json(task, cols=masterdb.NOTIFY_TASK_COLS)
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
                    j = self.master_db.as_json(job, cols=masterdb.NOTIFY_JOB_COLS)
                    self.notify_CPV1('job.add', j)

                    
                    for job_task in job.tasks:
                        if job_task.id != task.id:
                            if not job_task.error:
                                job_task.msg = err_msg 
                                job_task.error = True 
                                job_task.active = False

                                self.master_db.session.commit()
                                t = self.master_db.as_json(job_task, cols=masterdb.NOTIFY_TASK_COLS)
                                self.notify_CPV1('task.add', t)


            elif task.task_group_id:
                task_group = self.__slave_group_task_update_cnt(task.task_group_id, 0)
                if task_group:
                    tg = self.master_db.as_json(task_group, cols=masterdb.NOTIFY_TASK_GROUP_COLS)
                    self.notify_CPV1('taskgroup.add', tg)

                    #group_tasks = self.master_db.session.query(SlaveTask)\
                    #                .filter(masterdb.SlaveTask.task_group_id==task_group.id)\
                    #                .all()

                    #for gt in group_tasks:
                    #    gt.
                    #    pass

        t = self.master_db.as_json(task, cols=masterdb.NOTIFY_TASK_COLS)
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
            if task_group:
                tg = self.master_db.as_json(task_group, cols=masterdb.NOTIFY_TASK_GROUP_COLS)
                self.notify_CPV1('taskgroup.add', tg)

        if 'msg' in data.keys():
            task.msg = data['msg']
        self.master_db.session.commit()
        t = self.master_db.as_json(task, cols=masterdb.NOTIFY_TASK_COLS)
        self.notify_CPV1('task.add', t)

    def bd_md_slave_task_schedule_add(self, data, route_meta):
        #print 'Adding schedule for {}'.format(data['scheduler']['route'])
        ######
        #### ISSUE HERE BECAUSE IT WILL ONLY WORK DURING DAY IN PST
        #### DOESNT ACCOUNT FOR DIFFERENT WEEKDAY IN DIFFERENT TIMEZONES
        ######
        weekdays = {'monday':0, 'tuesday':1, 'wednesday':2, 'thursday':3, 'friday':4, 'saturday':5, 'sunday':6}
        if data['type'] == 'scheduler':
            scheduler_payloads = []
            date = datetime.utcnow()
            print  "\n\n\n\n\n {}...{}\n\n\n\n".format(weekdays[data['days'][0]], date.weekday())

            for day in data['days']:
                d = datetime.utcnow() 

                payload = {}
                target_day_idx = weekdays[day]
                while (target_day_idx!=d.weekday()-1):
                    d+=timedelta(days=1)


                d = d.replace(hour=int(data['clock'].split(':')[0]), minute=int(data['clock'].split(':')[1]))
                payload['run_time'] = d 
                payload['frequency_min'] =  60*24*7 
                payload['route']  = 'bd.sd.@BTV1.echo.job'

                scheduler_payloads.append(payload)
            
            scheduler_group = self.master_db.add(masterdb.SchedulerGroup, {'name': 'Scheduler Day test'})
            
            schedulers = []
            for payload in scheduler_payloads:
                payload['scheduler_group_id'] = scheduler_group.id
                scheduler = self.master_db.add(masterdb.Scheduler, payload)
                if not scheduler:
                    raise ValueError("SOME ISSUE SCHEDULER RETURNED NONE")

                schedulers.append(scheduler)
            
            sg = self.master_db.as_json(scheduler_group, cols=masterdb.NOTIFY_SCHEDULER_GROUP_COLS)
            self.notify_CPV1('scheduler.group.add', sg)

            for scheduler in schedulers:
                sc = self.master_db.as_json(scheduler, cols=masterdb.NOTIFY_SCHEDULER_COLS)
                self.notify_CPV1('scheduler.add', sc)

        elif data['type'] == 'timer':
            payload = {}

            payload['route']  = 'bd.sd.@BTV1.echo.job'
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

            scheduler_group = self.master_db.add(masterdb.SchedulerGroup, {'name': 'Scheduler Timer test'})

            payload['scheduler_group_id'] = scheduler_group.id

            scheduler = self.master_db.add(masterdb.Scheduler, payload)

            sg = self.master_db.as_json(scheduler_group, cols=masterdb.NOTIFY_SCHEDULER_GROUP_COLS)
            sc = self.master_db.as_json(scheduler, cols=masterdb.NOTIFY_SCHEDULER_COLS)

            self.notify_CPV1('scheduler.group.add', sg)
            self.notify_CPV1('scheduler.add', sc)

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
            s = self.master_db.as_json(slave, cols=masterdb.NOTIFY_SLAVE_COLS)
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

        s = self.master_db.as_json(slave, cols=masterdb.NOTIFY_SLAVE_COLS)
        self.notify_CPV1('slave.add', s)

        print 'SLAVES AUTOMATICALLY SET AS NON EC2 instances'


    def bd_md_slave_lost(self, data, route_meta):
        slave = self.master_db.session.query(masterdb.Slave)\
                .filter(masterdb.Slave.uuid==data['uuid'])\
                .first()
        if slave:
            slave.active = False
            self.master_db.session.commit()
            s = self.master_db.as_json(slave, cols=masterdb.NOTIFY_SLAVE_COLS)
            self.notify_CPV1('slave.add', s)



            #this handles all tasks assigned
            tasks = self.master_db.session.query(masterdb.SlaveTask)\
                    .filter(and_(masterdb.SlaveTask.assigned_slave_id==slave.id, masterdb.SlaveTask.completed==False))\
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
        sid = data['sid']
        uuid = data['uuid']

        jobs = self.master_db.session.query(masterdb.SlaveJob)\
                .filter(
                    or_(
                        masterdb.SlaveJob.completed==False,
                        masterdb.SlaveJob.created_time>=datetime.utcnow() - timedelta(hours=12)
                    )
                )\
                .all()


        job_data = []
        job_ids = []
        task_data = []
        task_group_data = []

        for job in jobs:
            j = self.master_db.as_json(job, cols=masterdb.NOTIFY_JOB_COLS)
            job_ids.append(job.id)
            job_data.append(j)
            tasks = self.master_db.session.query(masterdb.SlaveTask).filter(masterdb.SlaveTask.job_id==job.id).all()
            for task in tasks:
                t = self.master_db.as_json(task, cols=masterdb.NOTIFY_TASK_COLS)
                task_data.append(t)

        task_groups = self.master_db.session.query(masterdb.SlaveTaskGroup).filter(masterdb.SlaveTaskGroup.job_id.in_(job_ids)).all()

        if task_groups:
            for group in task_groups:
                tg = self.master_db.as_json(group, cols=masterdb.NOTIFY_TASK_GROUP_COLS)
                task_group_data.append(tg)

        self.notify_CPV1('jobs.add', job_data, session_id=sid, slave_uuid=uuid, update=False)
        self.notify_CPV1('taskgroups.add', task_group_data, session_id=sid, slave_uuid=uuid, update=False)
        self.notify_CPV1('tasks.add', task_data, session_id=sid, slave_uuid=uuid, update=False)
        
        scheduler_groups = self.master_db.session.query(masterdb.SchedulerGroup).all()

        for group in scheduler_groups:
           
            schedulers = self.master_db.session.query(masterdb.Scheduler)\
                    .filter(
                        masterdb.Scheduler.scheduler_group_id == group.id
                    )\
                    .all()

            if (schedulers):
                sg = self.master_db.as_json(group, cols=masterdb.NOTIFY_SCHEDULER_GROUP_COLS)
                self.notify_CPV1('scheduler.group.add', sg, session_id=sid, slave_uuid=uuid, update=False)

                schedulers_data = []
                for scheduler in schedulers:
                    s = self.master_db.as_json(scheduler, cols=masterdb.NOTIFY_SCHEDULER_COLS)
                    schedulers_data.append(s)

                if schedulers_data:
                    self.notify_CPV1('schedulers.add', schedulers_data, session_id=sid, slave_uuid=uuid, update=False) 


        slave_types = self.master_db.session.query(masterdb.SlaveType).all()
        slave_type_data = []
        for stype in slave_types:
            st = self.master_db.as_json(stype, cols=masterdb.NOTIFY_SLAVE_TYPE_COLS)
            slave_type_data.append(st)

        if slave_type_data:
            self.notify_CPV1('slave.types.add', slave_type_data, session_id=sid, slave_uuid=uuid, update=False)

        slaves = self.master_db.session.query(masterdb.Slave)\
                .filter(masterdb.Slave.active==True)\
                .all()

        slave_data = []
        for slave in slaves:
            s = self.master_db.as_json(slave, cols=masterdb.NOTIFY_SLAVE_COLS)
            slave_data.append(s)

        if slave_data:
            self.notify_CPV1('slaves.add', slave_data, session_id=sid, slave_uuid=uuid, update=False)


        alerts = self.master_db.session.query(masterdb.Alert)\
                .filter(masterdb.Alert.viewed==False)\
                .all()

        alert_data = [] 
        for alert in alerts:
            a = self.master_db.as_json(alert, cols=masterdb.NOTIFY_ALERT_COLS)
            alert_data.append(a)

        if alert_data:
            self.notify_CPV1('alerts.add', alert_data, session_id=sid, slave_uuid=uuid, update=False)


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


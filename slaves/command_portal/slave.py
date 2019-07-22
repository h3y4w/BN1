from drivers.slavedriver import SlaveDriver
from utils.mpinbox import MPPriorityQueue, create_local_task_message, INBOX_SYS_MSG, INBOX_TASK1_MSG, OUTBOX_SYS_MSG, OUTBOX_TASK_MSG

from flask import Flask, render_template, request, send_from_directory 
from flask_socketio import SocketIO, join_room, emit

import requests
import json
import os
import sys

import threading
import time
from datetime import datetime
driver = None

sym_dist = os.path.join(os.getcwd(), "dist_sym")

app = Flask(__name__,
           template_folder=sym_dist,
            static_folder=sym_dist
           )
socketio = SocketIO(app)
last_update = 0 
uuid = None

clients = {}

#add authentication from master
@app.route('/data/send', methods=['POST'])
def data_send ():
    global last_update
    data = request.get_json(force=True)

    session_id = None
    if 'sid' in data.keys():
        session_id = data['sid']

        if data.get('last_update'):
            if data['last_update'] > last_update:
                last_update = data['last_update'] #datetime.utcnow()
    r = {'success': False}
    if data:
        print "EMITTING update{} @ {}\n".format(last_update, data['channel'])
        if session_id:
            print "EMMITING TO :{}".format(session_id)
            emit('data_in', data, namespace='/', room=session_id)
        else:
            print "EMMITING TO ALL"
            emit('data_in', data, namespace='/', broadcast=True)

        r['success'] = True
    else:
        print "EMITTING NOT: {}\n".format(data)
    return str(r)

@app.route('/dist/css/<path:filename>')
def return_css(filename):
    path = os.path.join(sym_dist, 'css/')
    return send_from_directory(path, filename)

@app.route('/dist/js/<path:filename>')
def return_js(filename):
    path = os.path.join(sym_dist, 'js/')#, filename)
    return send_from_directory(path, filename)

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def catch_all(path):
    return render_template("index.html")

@socketio.on('forward')
def on_forward(data):
    msg = create_local_task_message('bd.@md.Slave.CPV1.forwarded', data)
    driver.send_msg(msg, 0)
    print "\n*******\nFORWARD DATA: {}".format(data)

@socketio.on('pinger')
def on_pinger(data):
    global last_update
    print "\n*******************PINGED LAST_UPDATE: {}".format(last_update)
    emit('ponger', {'last_update': last_update}, namespace='/', broadcast=True)

@socketio.on('get/warehouse')
def on_get_warehouse(data):
    global uuid
    data['uuid'] = uuid
    data['sid'] = request.sid

    driver.add_global_task('bd.sd.@WCV1.models.get.CPV1', data, 'Getting rows')

@socketio.on('get')
def on_get(data):
    global uuid
    data['uuid'] = uuid
    data['sid'] = request.sid
    msg = create_local_task_message('bd.@md.Slave.CPV1.get.objs', data)
    driver.send_msg(msg, 0)

@socketio.on('getall')
def on_getall(data):
    emit('set_last_update', {'last_update':last_update}, namespace='/', room=request.sid)

    global uuid
    d = { 'uuid': uuid, 'sid': request.sid} 
    msg = create_local_task_message('bd.@md.Slave.CPV1.sendall', d)
    driver.send_msg(msg, 0)
    print "\n\n**********************GETALL!!"

class CommandPortal(SlaveDriver):
    model_id = 'CPV1' 

    def __init__(self, config):
        super(CommandPortal, self).__init__(config)
        
        #creates symbol directory to template
        if os.path.exists(sym_dist):
            os.remove(sym_dist)
        os.symlink(config['dist_directory'], sym_dist) 

        self.config = config

        self.add_command_mappings({
            'bd.sd.@CPV1.grace': self.grace,
            'bd.sd.@CPV1.server.run': self.bd_sd_CPV1_server_run,
            'bd.sd.@CPV1.data.request.cb': self.bd_sd_CPV1_data_request_cb,
            'bd.sd.@CPV1.data.send': self.bd_sd_CPV1_data_send,
            'bd.sd.@CPV1.set.last-update-id': self.bd_sd_CPV1_set_lastupdateid
        })

        self.server_host = '0.0.0.0'
        self.server_port = 80

        server_args = {
            'host': self.server_host,
            'port': self.server_port 
        }
        run_server = lambda: self.add_local_task('bd.sd.@CPV1.server.run', server_args, INBOX_SYS_MSG, {'type': 'process'})

        self.init_start_task_funcs.append(run_server)

        global driver
        driver = self

    def bd_sd_CPV1_set_lastupdateid(self, data, route_meta):
        global last_update
        last_update = data['last_update']


    def bd_sd_CPV1_data_send(self, data, route_meta):
        action = data['action'] #triggered route
        session_id = data['sid']
        m = {
            'alert.add': 'SET_ALERT',
            'alert.delete': 'DELETE_ALERT',
            'alerts.add': 'SET_ALERTS',
            'alerts.delete': 'DELETE_ALERTS',
            'scheduler.add': 'SET_SCHEDULER',
            'schedulers.add': 'SET_SCHEDULERS',
            'scheduler.delete': 'DELETE_SCHEDULER',
            'schedulers.delete': 'DELETE_SCHEDULERS',
            'scheduler.group.add': 'SET_SCHEDULER_GROUP',
            'scheduler.groups.add': 'SET_SCHEDULER_GROUPS',
            'scheduler.group.delete': 'DELETE_SCHEDULER_GROUP',
            'scheduler.groups.delete': 'DELETE_SCHEDULER_GROUPS',
            'slave.add': 'SET_SLAVE',
            'slaves.add': 'SET_SLAVES',
            'slave.type.add': 'SET_SLAVE_TYPE',
            'slave.types.add': 'SET_SLAVE_TYPES',
            'task.add': 'SET_TASK',
            'tasks.add': 'SET_TASKS',
            'task.delete': 'DELETE_TASK',
            'tasks.delete': 'DELETE_TASKS',
            'job.add': 'SET_JOB',
            'jobs.add': 'SET_JOBS',
            'job.delete': 'DELETE_JOB',
            'jobs.delete': 'DELETE_JOBS',
            'task.group.add': 'SET_TASK_GROUP',
            'task.groups.add': 'SET_TASK_GROUPS',
            'task.group.delete': 'DELETE_TASK_GROUP',
            'task.groups.delete': 'DELETE_TASK_GROUPS',
            'data.rows.set': 'SET_ROWS',
            'data.row.set': 'SET_ROW',
            'data.row.update': 'UPDATE_ROW',
            'data.rows.update': 'UPDATE_ROWS',
            'data.row.delete': 'DELETE_ROW',
            'data.rows.delete': 'DELETE_ROWS'
        }
        try:
            data['channel'] = m[action]
        except Exception as e:
            self.report_error("CPV1 Data send", 'Action key not in dict: {}'.format(e))

        r = requests.post('http://{}:{}{}'.format(self.server_host, self.server_port, '/data/send'), data=json.dumps(data))

    def grace(self):
        raise NotImplemented

    def bd_sd_CPV1_data_request_cb(self, data, route_meta):
        {
            'slaves': [{'type': 'Scraper'}],
            'jobs': [{'name': 'Scrape site', 'id':1}],
            'tasks': [{'name': 'pull data', 'id':2}],
        }
        tasks_map = {} 
        if 'tasks' in data.keys():
            for task in data['tasks']:
                tasks_map[task['id']] = task
            emit('update_tasks_map', {'task_maps': task_maps})

    def server_pulse_loop(self, pid):
        while 1:
            self.heartbeat.send_pulse(pid)
            time.sleep(8)

    def bd_sd_CPV1_server_run(self, data, route_meta):
        #Add to to watchdogs to be deleted
        msg = create_local_task_message('bd.@md.CPV1.get.last-update-id',{})
        self.send_message_to_master(msg)

        global uuid
        uuid = self.uuid
        pid = os.getpid()
        print "\n\n\nRunning webserver {}".format(pid)
        self.heartbeat.__track_process__(pid, name='CPV1 Webserver', route='bd.sd.@CPV1.server.run', data=data)
        thread = threading.Thread(target=self.server_pulse_loop, args=(pid,))
        thread.daemon = False 
        thread.start()
        
        debug = False
        if 'CPV1_debug' in self.config.keys():
            if self.config['CPV1_debug']:
                self.report_error('NotImplemented', "Config Key CPV1_debug is not setup correctly", kill=True)
                debug=True
   
        try:
            socketio.run(app, host=data['host'], port=data['port'])
        except Exception as e:
            print "\n\nWEBSERVER CPV1 broke"
            self.report_error('CPV1 Webserver fail', str(e), kill=True)  
            sys.exit(0)



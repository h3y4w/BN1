from drivers.slavedriver import SlaveDriver
from mpinbox import MPPriorityQueue, create_local_task_message, INBOX_SYS_MSG, INBOX_TASK1_MSG, OUTBOX_SYS_MSG, OUTBOX_TASK_MSG

from flask import Flask, render_template, request
from flask_socketio import SocketIO, join_room, emit

import requests
import json
import os
import sys

import threading
import time
from datetime import datetime
driver = None

folder = "/home/den0/Programs/MSystem/BotNetwork1/BN1/test/dist"

app = Flask(__name__,
            template_folder=folder,
            static_folder=folder
           )

socketio = SocketIO(app)
last_update =None#datetime.utcnow() 
uuid = None

clients = {}

@app.route('/data/send', methods=['POST'])
def data_send():
    global last_update
    data = request.get_json(force=True)

    session_id = None
    if 'sid' in data.keys():
        session_id = data['sid']

    last_update = data['last_update'] #datetime.utcnow()
    r = {'success': False}
    if data:
        print "EMITTING update{} @ {}\n".format(last_update, data['channel'])
        #emit('pong', {'last_update': last_update}, namespace='/', broadcast=True)
        if session_id:
            print "EMMITING TO :{}".format(session_id)
            emit('data_in', data, namespace='/', room=session_id)
        else:
            emit('data_in', data, namespace='/', broadcast=True)

        r['success'] = True
    else:
        print "EMITTING NOT: {}\n".format(data)

    return json.dumps(r)

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
    print "\n*******************\nPINGED: {}"#.format(last_update)
    emit('ponger', {'last_update': last_update}, namespace='/', broadcast=True)
    #dirver.send_msg(create_local_task_message('@bd.echo', {'ping':'pong'}), INBOX_SYS_MSG)

    #inbox.put(create_local_task_message('bd.sd.@CPV1.grace', data), INBOX_SYS_MSG)

'''



COME HERE AND FIX THIS

getall needs to get session id and pass it to sendall
notify_cpv1 needs to have a uuid and session id argument


In the future change session_id to a name that can either be session_id or room_id (connection_id?)

'''

@socketio.on('getall')
def on_getall(data):
    global uuid
    d = { 'uuid': uuid, 'sid': request.sid} 
    msg = create_local_task_message('bd.@md.Slave.CPV1.sendall', d)
    driver.send_msg(msg, 0)
    print "\n\n**********************GETALL!!"

class CommandPortal(SlaveDriver):
    model_id = 'CPV1' 

    def __init__(self, config):
        super(CommandPortal, self).__init__(config)
        print "\n\nDist folder is setup by hardcoded method line 10"
        #folder = "/home/den0/Programs/MSystem/BotNetwork1/BN1/test/dist"

        #static_path = os.path.join(self.__exc_dir__, "dist/static")
        #template_path = os.path.join(self.__exc_dir__, "dist")

        #static_path = folder
        #template_path = folder

        #app.static_folder = static_path
        #app.template_folder = template_path
        #app.static_url_path = folder

        self.config = config


        self.add_command_mappings({
            'bd.sd.@CPV1.grace': self.grace,
            'bd.sd.@CPV1.server.run': self.bd_sd_CPV1_server_run,
            'bd.sd.@CPV1.data.request.cb': self.bd_sd_CPV1_data_request_cb,
            'bd.sd.@CPV1.data.send': self.bd_sd_CPV1_data_send
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

    def bd_sd_CPV1_data_send(self, data, route_meta):
        action = data['action'] #triggered route
        session_id = data['sid']
        m = {
            'alert.add': 'SET_ALERT',
            'alert.delete': 'DELETE_ALERT',
            'alerts.add': 'SET_ALERTS',
            'scheduler.add': 'SET_SCHEDULER',
            'schedulers.add': 'SET_SCHEDULERS',
            'scheduler.group.add': 'SET_SCHEDULER_GROUP',
            'scheduler.groups.add': 'SET_SCHEDULER_GROUPS',
            'slave.add': 'SET_SLAVE',
            'slaves.add': 'SET_SLAVES',
            'slave.type.add': 'SET_SLAVE_TYPE',
            'slave.types.add': 'SET_SLAVE_TYPES',
            'task.add': 'SET_TASK',
            'tasks.add': 'SET_TASKS',
            'job.add': 'SET_JOB',
            'jobs.add': 'SET_JOBS',
            'taskgroup.add': 'SET_TASK_GROUP',
            'taskgroups.add': 'SET_TASK_GROUPS'

        }
        try:
            data['channel'] = m[action]
        except Exception as e:
            self.report_error("CPV1 Data send", 'Action key not in dict: {}'.format(e))

        r = requests.post('http://{}:{}{}'.format(self.server_host, self.server_port, '/data/send'), data=json.dumps(data))

        print "DATA SEND RESP: {}".format(r)

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



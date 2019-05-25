from bot import SlaveDriver
from mpinbox import MPPriorityQueue, create_local_task_message, INBOX_SYS_MSG, INBOX_TASK1_MSG, OUTBOX_SYS_MSG, OUTBOX_TASK_MSG

from flask import Flask, render_template, request
from flask_socketio import SocketIO, join_room, emit

import requests
import json
import os

import threading
import time
from datetime import datetime
driver = None

folder = "/home/den0/Programs/MSystem/BotNetwork1/BN1/test/dist"
app = Flask(__name__#,
            #template_folder=folder,
            #static_folder=folder
           )

socketio = SocketIO(app)
last_update =datetime.utcnow() 

@app.route('/data/send', methods=['POST'])
def data_send():
    global last_update


    data = request.get_json(force=True)
    last_update = datetime.utcnow()
    r = {'success': False}
    if data:
        print "EMITTING: {}\n".format(data)
        #emit('pong', {'last_update': str(last_update)}, namespace='/', broadcast=True)
        emit(data['channel'], data['action_data'], namespace='/', broadcast=True)
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
    emit('ponger', {'last_update': str(last_update)}, namespace='/', broadcast=True)
    #dirver.send_msg(create_local_task_message('@bd.echo', {'ping':'pong'}), INBOX_SYS_MSG)

    #inbox.put(create_local_task_message('bd.sd.@CPV1.grace', data), INBOX_SYS_MSG)

@socketio.on('getall')
def on_getall(data):
    msg = create_local_task_message('bd.@md.Slave.CPV1.sendall', {})
    driver.send_msg(msg, 0)
    print "\n\n**********************GETALL!!"

class CommandPortal(SlaveDriver):
    model_id = 'CPV1' 

    def __init__(self, config):
        super(CommandPortal, self).__init__(config)
        static_path = os.path.join(self.__exc_dir__, "dist/static")
        template_path = os.path.join(self.__exc_dir__, "dist")

        app.static_folder = static_path
        app.template_folder = template_path
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
        m = {
            'slave.add': 'SET_SLAVE',
            'slaves.add': 'SET_SLAVES',
            'slave.types.add': 'SET_SLAVE_TYPES',
            'task.add': 'SET_TASK',
            'tasks.add': 'SET_TASKS',
            'job.add': 'SET_JOB',
            'taskgroup.add': 'SET_TASKGROUP'
        }
        data['channel'] = m[action]

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

        pid = os.getpid()
        print "\n\n\nRunning webserver {}".format(pid)
        self.heartbeat.__track_process__(pid, name='CPV1 Webserver', route='bd.sd.@CPV1.server.run', data=data)
        thread = threading.Thread(target=self.server_pulse_loop, args=(pid,))
        thread.daemon = True 
        thread.start()
        
        debug = False
        if 'CPV1_debug' in self.config.keys():
            if self.config['CPV1_debug']:
                self.report_error('NotImplemented', "Config Key CPV1_debug is not setup correctly", kill=True)
                debug=True

        socketio.run(app, host=data['host'], port=data['port'])


from bot import SlaveDriver
from mpinbox import MPPriorityQueue, create_local_task_message, INBOX_SYS_MSG, INBOX_TASK1_MSG, OUTBOX_SYS_MSG, OUTBOX_TASK_MSG


from flask import Flask, render_template, request
from flask_socketio import SocketIO, join_room, emit

import requests
import json

app = Flask(__name__,
        static_folder = "./dist/static",
        template_folder = "./dist")

socketio = SocketIO(app)

@app.route('/data/send', methods=['POST'])
def data_send():
    data = request.get_json(force=True)
    r = {'success': False}
    if data:
        print "EMITTING: {}\n".format(data)
        emit(data['channel'], data['action_data'], namespace='/', broadcast=True)
        r['success'] = True
    else:
        print "EMITTING NOT: {}\n".format(data)

    return json.dumps(r)

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def catch_all():
    return render_template("index.html")

@socketio.on('/forward')
def on_forward(data):
    print "FORWARD DATA: {}".format(data)

@socketio.on('/grace')
def on_grace(data):
    inbox.put(create_local_task_message('bd.sd.@CPV1.grace', data), INBOX_SYS_MSG)

class CommandPortal(SlaveDriver):
    model_id = 'CPV1' 

    def __init__(self, config):
        super(CommandPortal, self).__init__(config)

        self.add_command_mappings({
            'bd.sd.@CPV1.grace': self.grace,
            'bd.sd.@CPV1.server.run': self.bd_sd_CPV1_server_run,
            'bd.sd.@CPV1.data.request.cb': self.bd_sd_CPV1_data_request_cb,
            'bd.sd.@CPV1.data.send': self.bd_sd_CPV1_data_send
        })

        self.server_host = 'localhost'
        self.server_port = 80

        server_args = {
            'host': 'localhost',
            'port': 80 
        }
        run_server = lambda: self.add_local_task('bd.sd.@CPV1.server.run', server_args, INBOX_SYS_MSG, {'type': 'process'})

        self.init_start_task_funcs.append(run_server)

    def bd_sd_CPV1_data_send(self, data, route_meta):
        action = data['action'] #triggered route
        m = {
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


    def bd_sd_CPV1_server_run(self, args, route_meta):
        print "\n\n\nRunning socketio server"
        socketio.run(app, port=80, debug=False)



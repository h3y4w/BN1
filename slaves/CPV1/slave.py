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
#####################MIGHT HAVE TO INCLUDE POST URI FOR SETTING LAST_UPDATE THAT THE SLAVEDRIVER CALLS
####################
sym_dist = os.path.join(os.getcwd(), "dist_sym")

app = Flask(__name__,
           template_folder=sym_dist,
            static_folder=sym_dist
           )
socketio = SocketIO(app, async_mode='threading')
last_update = -1 
master_is_connected = False
uuid = None

clients = {}


def set_sid_info(sid, cmd_id):
    if '#' in sid:
        sid = sid.split('#')[0]
    return "{}#{}".format(sid, cmd_id)

def get_sid_info(sid):
    if '#' in sid:
        return sid.split('#')
    return sid, None

@app.route('/login', methods=["POST"])
def user_login():
    data = request.get_json(force=True)
    sdata = {
        'username': data['username'],
        'password': data['password']
    }
    tag = driver.taskrunner_create_address_tag()
    sdata['tag'] = tag
    msg = create_local_task_message ('bd.@md.Slave.CPV1.user.auth', sdata) 
    driver.send_message_to_master(msg)

    d = driver.taskrunner_inbox_get_tag(tag, poll=5, delay=.5)
    print "GOT THIS SHIT FROM DATA: {}\n\n\n".format(d)
    return json.dumps(d)


def user_register():
    data = request.get_json(force=True)

@app.route('/driver/emit/data-in', methods=["POST"])
def emit_datain():
    data = request.get_json(force=True)
    print "EMITTING {} id: {}".format(data['channel'], data['last_update'])
    if data.get('sid'):
        print "TO:{}".format(data.get('sid'))
        socketio.emit('data_in', data, namespace='/', room=data.get('sid'))

    else:
        print "TO:ALL"
        socketio.emit('data_in', data, namespace='/', broadcast=True)

    return ""

@app.route('/driver-set/master-connected', methods=["POST"])
def set_masterconnected():
    global master_is_connected
    data = request.get_json(force=True)
    master_is_connected = data['is_connected']
    
    print "EMIT MASTER_IS_COONNECTED: {}\n\n".format(master_is_connected)
    socketio.emit('set_is_connected_to_master', {'is_connected':master_is_connected}, namespace='/', broadcast=True)

    return""

@app.route('/driver-set/last-update-id', methods=["POST"])
def set_lastupdateid():
    global last_update
    data = request.get_json(force=True)
    last_update = data['last_update']
    return ""

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

cpv1_cmd_id = 0

@socketio.on('forward')
def on_forward(data):
    global cpv1_cmd_id
    cpv1_cmd_id+=1

    if 'cmd_msg' in data.keys():
        #data['cmd_msg'] = data['cmd_msg']+' [{}]'.format(cpv1_cmd_id)
        pass
    data['sid'] = set_sid_info(request.sid, cpv1_cmd_id)
    global uuid
    data['uuid'] = uuid
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
    emit('SET_SID', {'sid': request.sid}, namespace='/', room=request.sid)
    emit('set_last_update', {'last_update':last_update}, namespace='/', room=request.sid)
    if not master_is_connected:
        msg = create_local_task_message('bd.sd.@CPV1.set.is-connected-to-master', {})
        driver.inbox.put(msg, INBOX_SYS_MSG)
    else:
        socketio.emit('set_is_connected_to_master',{'is_connected':master_is_connected}, namespace='/', room=request.sid)

    global uuid
    d = { 'uuid': uuid, 'sid': request.sid} 
    msg = create_local_task_message('bd.@md.Slave.CPV1.sendall', d)
    driver.send_msg(msg, 0)
    print "\n\n**********************GETALL!!, last_UPDATE: {}".format(last_update)

class CPV1(SlaveDriver):
    model_id = 'CPV1' 
    last_update = -1

    def __init__(self, config):
        super(CPV1, self).__init__(config)
        
        #creates symbol directory to template
        if os.path.exists(sym_dist):
            os.remove(sym_dist)
        os.symlink(config['dist_directory'], sym_dist) 

        self.config = config
        self.cmd_msgs = []

        self.add_command_mappings({
            'bd.sd.@CPV1.server.run': self.bd_sd_CPV1_server_run,
            'bd.sd.@CPV1.data.send': self.bd_sd_CPV1_data_send,
            'bd.sd.@CPV1.set.last-update-id': self.bd_sd_CPV1_set_lastupdateid,
            'bd.sd.@CPV1.set.is-connected-to-master': self.bd_sd_CPV1_set_isconnectedtomaster

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

    def connected_to_master(self):
        print "CONNECTED_TO_MASTER!!!!\n\n\n"
        msg = create_local_task_message('bd.@md.Slave.CPV1.get.last-update-id',{})
        self.send_message_to_master(msg, OUTBOX_SYS_MSG)
        try:
            self.__set_isconnectedtomaster(True)
        except Exception as e:
            print "CONNECTED_TO_MASTER ERROR: {}".format(e)

    def disconnected_from_master(self):
        try:
            self.__set_isconnectedtomaster(False)
        except Exception as e:
            print "DISCONNECTED_TO_MASTER ERROR: {}".format(e)

    def send_cmd_msg(self, cmd_id, resp, session_id=None, uuid=None):
        print "CPV1.slave.SEND_CMD_MSG"
        cmd_msg = self.get_cmd_msg(cmd_id)
        if cmd_msg:
            print "FOUND MSG"
            cmd_msg = "{}: {}".format(resp, cmd_msg)
            data['msg'] = cmd_msg
            if session_id:
                data['session_id'] = session_id
            if uuid:
                data['uuid'] = uuid

            msg = create_local_task_message('bd.@md.Slave.CPV1.alert',data)
            self.send_msg(msg)

    def get_cmd_msg(self, msg_id):
        l = len(self.cmd_msgs)
        for i in xrange(0, l):
            if self.cmd_msgs[i]['id'] == msg_id:
                msg = self.cmd_msgs[i]['id']
                del self.cmd_msgs[i]['id']
                return msg
            if self.cmd_msgs[i]['exp'] == datetime.utcnow():
                del self.cmd_msgs[i]

    def __set_isconnectedtomaster(self, is_connected):
        print "SETTING CONNECTED TO MASTER: {}\n\n\n".format(is_connected)

        data = {"is_connected": is_connected, 'uuid': self.master_uuid}
        r = requests.post(
            'http://{}:{}{}'.format(self.server_host, self.server_port, '/driver-set/master-connected'),
            data=json.dumps(data)
        )


    def __set_lastupdateid_server(self, update_id):


        return requests.post(
            'http://{}:{}{}'.format(self.server_host, self.server_port, '/driver-set/last-update-id'),
            data=json.dumps({'last_update':self.last_update})
        )

    def bd_sd_CPV1_set_lastupdateid(self, data, route_meta):
        self.last_update = data['last_update']
        self.__set_lastupdateid_server(self.last_update)

    def bd_sd_CPV1_set_isconnectedtomaster(self, data, route_meta):
        self.__set_isconnectedtomaster(self.CONNECTED_TO_MASTER)

    def bd_sd_CPV1_data_send(self, data, route_meta):
        action = data['action'] #triggered route
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
            'master.config.add': 'SET_MASTER_CONFIG',
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
            'data.rows.cnt.set': 'SET_ROWS_CNT',
            'data.rows.set': 'SET_ROWS',
            'data.row.set': 'SET_ROW',
            'data.row.update': 'UPDATE_ROW',
            'data.rows.update': 'UPDATE_ROWS',
            'data.row.delete': 'DELETE_ROW',
            'data.rows.delete': 'DELETE_ROWS',
            'sid.set': 'SET_SID'
        }

        try:
            data['channel'] = m[action]
        except Exception as e:
            self.report_error("CPV1 Data send", 'Action key not in dict: {}'.format(e))

        with app.app_context():

            session_id = data.get('sid')
            cmd_id = None
            if session_id:
                session_id, cmd_id = get_sid_info(session_id)
                if cmd_id:
                    print "SESSION_ID: {} CMD_ID: {}\n\n\n\n".format(session_id, cmd_id)
                    self.send_cmd_msg(cmd_id, "Successful", session_id=session_id, uuid=self.uuid)


            if data['last_update'] > self.last_update:
                self.last_update = data['last_update']
                self.__set_lastupdateid_server(self.last_update)

            r = requests.post(
                'http://{}:{}{}'.format(self.server_host, self.server_port, '/driver/emit/data-in'),
                data=json.dumps(data)
            )
            print "CPV1.R = {}".format(r)
            return

        #DOESNT WORK UNDERNEATH
        #####################################

            if data:
                print "EMITTING {} id: {} uuid: {}".format(data['channel'], self.last_update, self.uuid)
                if session_id:
                    print "TO:{}".format(session_id)
                    socketio.emit('data_in', data, namespace='/', room=session_id)


                else:
                    print "TO:ALL"
                    socketio.emit('data_in', data, namespace='/', broadcast=True)

    def server_pulse_loop(self, pid):
        while 1:
            self.heartbeat.send_pulse(pid)
            time.sleep(8)

    def bd_sd_CPV1_server_run(self, data, route_meta):
        #Add to to watchdogs to be deleted
        msg = create_local_task_message('bd.@md.Slave.CPV1.get.last-update-id',{})
        self.send_message_to_master(msg)



        global uuid
        uuid = self.uuid
        pid = os.getpid()
        print "\n\n\nRunning webserver {}".format(pid)
        self.heartbeat.__track_process__(pid, name='CPV1 Webserver', route='bd.sd.@CPV1.server.run', data=data)

        thread = threading.Thread(target=self.server_pulse_loop, args=(pid,))
        thread.daemon = True #False 
        thread.start()

        ###KEeps running even when server fails
        #socketio.start_background_task(self.server_pulse_loop, pid)
        
        debug = False
        if 'CPV1_debug' in self.config.keys():
            if self.config['CPV1_debug']:
                self.report_error('NotImplemented', "Config Key CPV1_debug is not setup correctly", kill=True)
                debug=True
        try:
            socketio.run(app, host=data['host'], port=data['port'], debug=debug)
        except Exception as e:
            print "\n\nWEBSERVER CPV1 broke"
            self.report_error('CPV1 Webserver fail', str(e), kill=True)  
            sys.exit(0)


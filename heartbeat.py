from multiprocessing import Queue
import os
from Queue import Empty
from datetime import datetime, timedelta
from mpinbox import create_local_task_message, INBOX_SYS_MSG, INBOX_SYS_CRITICAL_MSG


class Heartbeat (object):
    q = None
    pids = {} 
    def __init__(self, queue, outbox, grace=30, dead_cb=None, parent_pid=None):
        self.q = queue
        self.grace = grace
        self.dead_cb = dead_cb
        self.active = False
        self.parent_pid = parent_pid
        self.outbox = outbox

    def KILLALL(self):
        self.q.put('KILLALL',0)

    def kill_pid(self, pid):
        os.system("kill -9 {}".format(pid))
        os.system("sudo kill -9 {}".format(pid))
        os.system('sudo pkill -TERM -P {}'.format(pid))

    def watchdog(self):
        print "Starting Watchdog pid: {}".format(os.getpid())
        while 1:
            self.listen()
            pids = self.pids.keys()
            if len(pids):
                if not self.active: #activates heartbeat to begin tracking and exit if no more pids 
                    self.active = True

                for pid in pids:
                    pid_info = self.pids[pid]
                    if pid_info['pulse'].is_expired(datetime.utcnow()):
                        if pid_info['pulse'].flag:
                            name = pid_info['name']
                            route = pid_info['meta']['route']
                            data = pid_info['meta']['data']
                            print "pid {} {} is inactive - killing process".format(name, pid)
                            if self.parent_pid == pid:
                                print "Main pid died so killing everything"
                                self.kill_pid(pid)
                                break
                            else:
                                msg = create_local_task_message('@bd.process.kill', {'pid':pid})
                                self.outbox.put(msg,INBOX_SYS_CRITICAL_MSG)

                            del self.pids[pid]
                            
                            if self.dead_cb:
                                self.dead_cb(route, data)

                        pid_info['pulse'].flag = True
            else:
                if self.active:
                    print "No pids to track... Exiting"
                    break

    def listen(self):
        pulse = None
        pulse = self.q.get()

        if type(pulse) == type({}): #its a new pid to add
            print pulse
            pid = pulse['pulse'].pid
            print "\n\nTracking new Process '{}' Pid: {}".format(pulse['name'], pid)
            self.pids[pid] = {}
            self.pids[pid]['pulse'] = pulse['pulse'] 
            self.pids[pid]['name'] = pulse['name']
            self.pids[pid]['meta'] = {'route': pulse['route'], 'data': pulse['data']}
        
        elif type(pulse) == Pulse:
            print "Received pulse from pid {},{}".format(pulse.pid, self.pids[pulse.pid]['name'])
            self.pids[pulse.pid]['pulse'] = pulse
        elif type(pulse) == str:
            if pulse == 'KILLALL':
                print '\n\n\n'
                pids = self.pids.keys()
                print pids
                print '\n\n'
                main_pid = None
                for pid in pids:
                    if self.pids[pid]['name'] == 'BotDriver Main':
                        main_pid = pid
                        continue
                    self.kill_pid(pid)
                self.kill_pid(main_pid)
                    

    def send_pulse(self, pid, tmp_grace=None):
        grace = self.grace
        if tmp_grace:
            grace = tmp_grace
        self.q.put(Pulse(pid, grace=grace),0)

    def __track_process__(self, pid, name=None, route=None, data={}):
        if not name:
            name = "Process {}".format(pid)

        self.q.put({
            'name': name,
            'route': route,
            'data': data,
            'pulse': Pulse(pid, self.grace)
        },0)


class Pulse (object):
    def __init__(self, pid, grace=30):
        self.pid = pid
        self.time = datetime.utcnow()
        self.grace = grace
        self.flag = False

    def is_expired(self, dt):
        return dt > self.time + timedelta(seconds=self.grace)  
    

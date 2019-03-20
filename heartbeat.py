from multiprocessing import Queue
import os
from Queue import Empty
from datetime import datetime, timedelta

class Heartbeat (object):
    q = None
    pids = {} 
    def __init__(self, grace=30):
        self.q = Queue()
        self.grace = grace

    def watchdog(self, dead_cb):
        print "Starting Watchdog pid: {}".format(os.getpid())
        while 1:
            self.listen()
            for pid in self.pids.keys():
                pid_info = self.pids[pid]
                if pid_info['pulse'].is_expired(datetime.utcnow()):
                    if pid_info['pulse'].flag:
                        name = pid_info['name']
                        print "pid {} {} is inactive".format(name, pid)
                        print dead_cb
                        if dead_cb:
                            dead_cb(pid_info)
                        del self.pids[pid]

                    pid_info['pulse'].flag = True

    def listen(self):
        pulse = None
        try:
            pulse = self.q.get_nowait()
        except Empty:
            pass
        else:
            if type(pulse) == type({}): #its a new pid to add
                print "Adding new pid {} to heartbeat".format(pulse['pulse'].pid)
                self.pids[pulse['pulse'].pid] = pulse
                pulse = pulse['pulse']
            
            elif type(pulse) == Pulse:
                print "Received pulse from pid {},{},{}".format(pulse.pid, self.pids[pulse.pid]['type'], self.pids[pulse.pid]['name'])
                self.pids[pulse.pid]['pulse'] = pulse

            else:
                raise ValueError("Unknown object type sent to heartbeat")

        finally:
            return pulse

    def send_pulse(self, pid):
        self.q.put(Pulse(pid, grace=self.grace))

    def __track_process__(self, pid, type_=None, name=None, route=None):
        if name:
            print "Tracking Process '{}' Pid: {}".format(name, pid)
        if not name:
            name = "Process {}".format(pid)

        self.q.put({
            'type': type_,
            'name': name,
            'route': route,
            'pulse': Pulse(pid, self.grace)
        })


class Pulse (object):
    def __init__(self, pid, grace=30):
        self.pid = pid
        self.time = datetime.utcnow()
        self.grace = grace
        self.flag = False

    def is_expired(self, dt):
        return dt > self.time + timedelta(seconds=self.grace)  
    

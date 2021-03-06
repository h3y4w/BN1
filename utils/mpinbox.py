
import multiprocessing as multiprocess
import os
import Queue

INBOX_SYS_CRITICAL_MSG = 0
INBOX_SYS_CRIT_MSG = 0

INBOX_SYS_MSG = 1
INBOX_BLOCKING_MSG = 2
INBOX_TASK1_MSG = 3
INBOX_TASK2_MSG = 4

OUTBOX_SYS_MSG = 1
OUTBOX_TASK_MSG = 1

def create_local_task_message(route, data, route_meta=None, origin=None):
    msg = {'route': route, 'data':data}
    if not route_meta:
        route_meta={'type': 'default'}

    if origin:
        route_meta['origin'] = origin
    msg['route_meta'] = route_meta

    return msg 


class MPChannelQueue (object):
    queues = None
    def __init__(self, channels, manager=None):

       obj = multiprocess.Queue
       self.queues = {}
       if manager:
        obj = manager.Queue

        for channel in channels:
            self.queues[channel] = obj()

    def get(self, channel):
        return self.queues[channel].get_nowait()

    def put(self, item, channel):
        self.queues[channel].put(item)

class MPPriorityQueue(object):
    queues = None 
    q = None
    def __init__(self, total_priorities, manager=None):
        obj = multiprocess.Queue
        self.queues = []
        if manager:
            obj = manager.Queue
        self.total_priorities = total_priorities
        for i in range(0, total_priorities):
            self.queues.append(obj())

    def get(self, priority=None, remove=True, get_priority=False):
        item = None
        if (priority != None):
            try:
                if remove:
                    item = self.queues[priority].get_nowait()
                else:
                    item = self.queues[priority].queue[0]
            except Queue.Empty:
                pass
        else:
            priority = 0
            for priority in range(0, self.total_priorities): 
                item = self.get(priority, remove=remove)
                if item:
                    break
        if get_priority:
            return item, priority

        return item

    def put(self, item, priority):
        self.queues[priority].put(item)


from twisted.internet import reactor, protocol

from datetime import datetime
import json
from utils.mpinbox import create_local_task_message, INBOX_SYS_MSG, INBOX_TASK1_MSG, OUTBOX_SYS_MSG, OUTBOX_TASK_MSG

import traceback

class Payload (object):
    def __init__(self, data, code, error):
        self.data = data
        self.code = code
        self.error = error

    @staticmethod
    def load(json_data):
        if type(json_data) == str:
            json_data = json.loads(json_data)
        payload = Payload(
            json_data['data'],
            json_data['code'], 
            json_data['error']
        )
        return payload

    def dump(self):
        return json.dumps({
            'data': self.data,
            'code': self.code,
            'error': self.error
        })

driver = None

class Echo(protocol.Protocol):
    def __init__(self, factory):
        self.factory = factory
        self.uuid = None

    def connectionMade(self):
        self.connected = True

    def connectionLost(self, reason):
        print '</> (Lost {})'.format(self.uuid)
        if self.uuid in self.factory.connections.keys():
            #delete uuid from connections
            del self.factory.connections[self.uuid]

        driver.inbox.put(create_local_task_message('bd.@md.slave.lost', {'uuid':self.uuid}),0)

    def dataReceived(self, data):
        if not self.uuid:
            print "\n\nSLAVE IS NOT REGISTER comms.server\n\n"
            
        try:
            data = json.loads(str(data))
        except Exception as e:

            print '\n\ncomms.server.Excepion: {}\nERROR: {}\n\n'.format(e, data)
            print traceback.print_exc()
        else:
            if data['route'] == 'bd.@md.slave.connect': #create seperate func
                self.uuid = data['data']['uuid']
                self.factory.connections[data['data']['uuid']] = self
            
            data['route_meta']['origin'] = self.uuid
            msg = create_local_task_message(
                data['route'],
                data['data'],
                data['route_meta']
            )
            driver.inbox.put(msg, INBOX_SYS_MSG)

    def forward(self, data):
        if self.connected:
            self.transport.write(data)

class BotServerFactory(protocol.Factory):
    def __init__(self, drivert):
        self.connections = {} 
        self.driver = drivert
        global driver
        driver = drivert

    def buildProtocol(self, addr):
        print "connection by", addr
        e = Echo(self)
        return e

    def send_it(self, payload):
        self.connections[payload['uuid']].transport.write(json.dumps(payload['data']))

    def sendTime(self):
        print 'SENDING TIME'
        payload = Payload(str(datetime.utcnow()), 200, False)
        for con in self.connections:
            c = self.connections[con]
            is_alive = 1
            if (is_alive):
                print 'SENT TIME'
                c.forward(payload.dump())
            else:
                print 'removed disconnected client'

#reactor.listenTCP(5007, EchoFactory())
#reactor.run()

#s = S(payload_master)

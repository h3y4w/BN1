from twisted.internet import reactor, protocol

from datetime import datetime
import json
from utils.mpinbox import create_local_task_message, INBOX_SYS_MSG, INBOX_TASK1_MSG, OUTBOX_SYS_MSG, OUTBOX_TASK_MSG

import traceback

START_DEL = '\n\r'
END_DEL = '\r\n'

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
        self.dbuffer = ""

    def connectionMade(self):
        self.connected = True

    def connectionLost(self, reason):
        print '</> (Lost {}) bc: {}'.format(self.uuid, reason)
        if self.uuid in self.factory.connections.keys():
            #delete uuid from connections
            try:
                driver.inbox.put(create_local_task_message('bd.@md.slave.lost', {'uuid':self.uuid}),0)
            except:
                pass
            finally:
                del self.factory.connections[self.uuid]


    def dataReceived(self, data):
        received = False
        start = False
        end = False

            
        try:
            raw_data = str(data)

            data = raw_data
            if raw_data[:2] == START_DEL:
                start = True

            if raw_data[-2:] == END_DEL:
                end = True


            if start and end:
                try:
                    data = json.loads(raw_data)
                except ValueError:
                    idx = raw_data.find(END_DEL)
                    if idx != (len(raw_data)-1):
                        rds = raw_data.split(END_DEL)
                        for rd in rds:
                            d = json.loads(rd+END_DEL)
                            self.send_to_inbox(d)
                            return
                else:
                    received = True

            elif start and not end:
                self.dbuffer = raw_data

            elif not start and end:
                if self.dbuffer:
                    if self.dbuffer[:1] == START_DEL:
                        poss = self.dbuffer + raw_data
                        try:
                            data = json.loads(poss)
                        except Exception as e:
                            print "not START AND END ERROR: {}".format(e)
                        else:
                            received = True

        except Exception as e:
            print '\n\ncomms.server.Excepion: {}\nERROR: {}\n\n'.format(e, data)
            print traceback.print_exc()


        if received:
            self.send_to_inbox(data)
    
    def send_to_inbox(self, data):
            if data['route'] == 'bd.@md.slave.connect': #create seperate func
                self.uuid = data['data']['uuid']
                self.factory.connections[data['data']['uuid']] = self
            else:
                if not self.uuid:
                    self.transport.abortConnection()
                    print "\n\nSLAVE IS NOT REGISTER DISCONNECTING: {} comms.server\n\n".format(self.uuid)
                    return
            
            data['route_meta']['origin'] = self.uuid
            msg = create_local_task_message(
                data['route'],
                data['data'],
                data['route_meta']
            )
            driver.inbox.put(msg, INBOX_SYS_MSG)

    def send(self, payload):
        #if self.connected:
        self.transport.write(START_DEL+payload+END_DEL)

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
        uuid = payload['uuid']
        data = None
        if type(payload) == dict:
            data = json.dumps(payload['data'])
        try:
            self.connections[uuid].send(data)
        except KeyError:
            self.driver.inbox.put(create_local_task_message('bd.@md.slave.lost', {'uuid':uuid}),0)
            if uuid in self.connections.keys():
                del self.connections[uuid]

            print "\n\ncomms.server UUID: {} DOESNT EXIST AS CONNECTION".format(uuid)

#reactor.listenTCP(5007, EchoFactory())
#reactor.run()

#s = S(payload_master)

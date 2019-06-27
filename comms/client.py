from twisted.internet import reactor, stdio
import sys
from twisted.internet.protocol import Protocol, ClientFactory
from twisted.protocols import basic
import json
from server import Payload
import os
from mpinbox import create_local_task_message, INBOX_SYS_CRIT_MSG, INBOX_SYS_MSG, INBOX_TASK1_MSG, OUTBOX_SYS_MSG, OUTBOX_TASK_MSG

class BotClientProtocol(Protocol):
    factory = None
    def __init__(self, factory):
        self.factory = factory

    def dataReceived(self, data):
        data = json.loads(data)
        msg = create_local_task_message(
            data['route'],
            data['data'],
            data['route_meta']
        )
        if self.factory:
            self.factory.driver.inbox.put(msg, INBOX_SYS_MSG)

        #else:
        #    raise ValueError("client factory is null")

        #self.transport.loseConnection()

    def connectionMade(self):
        print "Connection made {}".format(self.transport.getPeer())

        msg = create_local_task_message(
            'bd.@md.slave.connect',
            { 'uuid': self.factory.driver.uuid }
        )

        #if self.factory.driver:
        #    data['data']['slave_type_id'] = self.factory.driver.model_id

        self.transport.write(json.dumps(msg))

    def connectionLost(self, s):
        print "connection lost"
        self.transport.write("Connection Lost!\r\n")
        #self.factory.protocols.remove(self)

    def write(self, payload):
        self.transport.write(payload)

class BotClientFactory(ClientFactory):
    protocol = BotClientProtocol
    p = None

    def __init__(self):
        self.client = None

    def set_driver(self, driver):
        self.driver = driver

    def send_it(self, payload):
        if self.p:
            print "SEND IT"
            if type(payload) == dict:
                payload = json.dumps(payload)
            self.p.write(payload)
        else:
            msg = create_local_task_message(
                '@bd.error',
                {'type': 'ValueError', 'msg': 'No Protocol', 'die':True},
            )
            self.factory.driver.inbox.put(msg, INBOX_SYS_CRIT_MSG)


    def buildProtocol(self, addr):
        p = BotClientProtocol(self)
        self.p = p
        return p

    def startedConnecting(self, connector):
        destination = connector.getDestination()
        print "<client> started Connecting destionation: {}".format(destination)

    def clientConnectionLost(self, connector, reason):
        print "LOST CONNECTION {}".format(reason)

        #add a grae period before disconnecting
        #connector.connect()
        connector.disconnect()

    def clientConnectionFailed(self, connector, reason):
        print "lost failed"
        self.driver.comms_pulse_cb.stop()
        msg = create_local_task_message(
            '@bd.error',
            {'type': 'ValueError', 'msg': 'Client Connection Failed', 'die':True},
        )
        if self.p:
            if self.p.factory:
                self.p.factory.driver.inbox.put(msg, INBOX_SYS_CRIT_MSG)
        

        connector.disconnect()

        sys.exit(1)
        #raise ValueError("Client Connection Lost")


from twisted.internet import reactor, stdio
import sys
import time
from twisted.internet.protocol import Protocol, ClientFactory
from twisted.protocols import basic
import json
from server import Payload
import os
from utils.mpinbox import create_local_task_message, INBOX_SYS_CRIT_MSG, INBOX_SYS_MSG, INBOX_TASK1_MSG, OUTBOX_SYS_MSG, OUTBOX_TASK_MSG
from datetime import datetime, timedelta


START_DEL = '\n\r'
END_DEL = '\r\n'


class BotClientProtocol(Protocol):
    factory = None
    connected = False
    def __init__(self, factory):
        self.factory = factory
        self.dbuffer = ""

    def dataReceived(self, data):
        self.connected = True
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
        #self.transport.loseConnection()

    def send_to_inbox(self, data):
            msg = None
            try:
                msg = create_local_task_message(
                    data['route'],
                    data['data'],
                    data['route_meta']
                )
            except TypeError as e:
                print "\n\n\n\TypeError comms.client: {}".format(e)
                print "error with {}:\n{}\n".format(type(data), data)
                raise e
            else:
                self.factory.driver.inbox.put(msg, INBOX_SYS_MSG)

    def connectionMade(self):
        self.connected = True
        print "Connection made {} my uuid: {}".format(self.transport.getPeer(), self.factory.driver.uuid)
        msg = create_local_task_message(
            'bd.@md.slave.connect',
            { 'uuid': self.factory.driver.uuid }
        )

        self.send(json.dumps(msg))

        msg1 = create_local_task_message(
            'bd.@sd.master.connected',
            {}
        )

        self.factory.driver.inbox.put(msg1, INBOX_SYS_CRIT_MSG)


    def connectionLost(self, s):
        self.connected = False
        print "connection lost"
        msg1 = create_local_task_message(
            'bd.@sd.master.disconnected',
            {}
        )

        self.factory.driver.inbox.put(msg1, INBOX_SYS_CRIT_MSG)

    def send(self, payload):
        self.transport.write(START_DEL+payload+END_DEL)


class BotClientFactory(ClientFactory):
    protocol = BotClientProtocol

    def __init__(self, driver):
        self.client = None
        self.driver = driver
        self.err_time = None

    def send_it(self, payload):
        if hasattr(self, 'p'):
            print "Sending to master..."
            if type(payload) == dict:
                payload = json.dumps(payload)

            self.p.send(payload)
        else:
            msg1 = create_local_task_message(
                'bd.@sd.master.disconnected',
                {}
            )

            self.driver.inbox.put(msg1, INBOX_SYS_CRIT_MSG)

            
    def buildProtocol(self, addr):
        p = BotClientProtocol(self)
        self.p = p
        return p

    def startedConnecting(self, connector):
        destination = connector.getDestination()
        print "<client> started Connecting destionation: {}".format(destination)


    def clientConnectionLost(self, connector, reason):
        if hasattr(self, 'p'):
            self.p.connected = False

        print "LOST CONNECTION {}".format(reason)
        try:
            connector.connect()
        except:
            time.sleep(1)
            pass

    def clientConnectionFailed(self, connector, reason):
        if hasattr(self, 'p'):
            self.p.connected = False

        print "CONNECTION FAILED {}".format(reason)
#        connector.disconnect()
        try:
            connector.connect()
        except:
            time.sleep(1)
            pass

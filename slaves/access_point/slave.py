import os
from bot import SlaveDriver, INBOX_SYS_MSG
import time
from db import masterdb
from db import db
from mpinbox import create_local_task_message

from mpinbox import create_local_task_message, INBOX_SYS_MSG, INBOX_TASK1_MSG, OUTBOX_SYS_MSG, OUTBOX_TASK_MSG

from flask import Flask, request
from flask_restful import Api, Resource

from time import sleep
#======================================================
#########CHANGE THIS TO AcessPoint##################
#######USE FLASK INSTEAD OF FLASK API#############
######SHOW SIMPLE CONTROL PAGE###############
#============================================######

driver_inbox = None
driver_outbox = None
class Forward (Resource):
    def post(self):
        data = {}
        message = ''
        error = False
        try:
            d1 = request.get_json(force=True)
            args = {"data": d1, "priority": 1}

            driver_outbox.put(create_local_task_message('bd.@md.Slave.APV1.forwarded', args), 1)
            data['worked'] = True
            
        except Exception as e:
            message = str(e)
            error = True
        return data 

    def get(self):
        data = {}
        error = str(e)
        message = 'Uncaught Error'
        try:
            pass
        except Exception as e:
            message = str(e)
            error = True
        else:
            error = False
            message = ''
        return data


class AccessPoint(SlaveDriver):
    model_id = "APV1"
    def __init__(self, config):
        payload = {}
        super(AccessPoint, self).__init__(config)
        self.add_command_mappings({
            'bd.sd.@APV1.echo': self.bd_sd_APV1_echo,
            'bd.sd.@APV1.#echos': self.bd_sd_APV1_echos,
            'bd.sd.@APV1.server.run': self.run
        })
    

        server_args = {
            'host': 'localhost',
            'port': 5555
        }

        server = lambda: self.add_local_task('bd.sd.@APV1.server.run', server_args, INBOX_SYS_MSG, {'type': 'process'})

        self.init_start_task_funcs.append(server)
        #self.driver = driver

    def bd_sd_APV1_echo(self, payload, route_meta):
        print 'AccessPoint.echo > payload: {}'.format(payload)

    def bd_sd_APV1_echos(self, data, route_meta):
        sleep(1)
        print "1"
        sleep(1)
        print "2"
        sleep(1)
        print "3"
        print 'AccessPoint.echos > payload: {}'.format(data)

    def run(self, args, route_meta):
        print "AccessPoint Slave api pid {}".format(os.getpid())
        print "Running Server\n\n\n"
        global driver_inbox
        global driver_outbox
        driver_inbox = self.inbox
        driver_outbox = self.outbox

        app = Flask(__name__)
        api = Api(app)

        api.add_resource(Forward, '/forward')
        app.run(args['host'], args['port'], debug=False)

if __name__ == "__main__":
    AccessPoint().start()

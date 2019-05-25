import os
import json
import sys
config = None 
exc_dir = os.path.dirname(os.path.abspath(__file__))
fn = os.path.dirname(os.path.abspath(__file__))+'/slave_config.json'
with open(fn, 'r') as f:
    config = json.loads(f.read())
os.chdir(config['lib_directory'])


print 'exec_dir {}'.format(exc_dir)
print '\n\n'

sys.path.append( config['lib_directory'])
config['exc_dir'] = exc_dir


import bot
from slaves.command_portal.slave import socketio, app 
os.chdir(exc_dir)

app

folder = "/home/den0/Programs/MSystem/BotNetwork1/BN1/test/dist"
folder = "/home/den0/Programs/MSystem/BotNetwork1/BN1/test/dist"
app.static_folder = folder
app.template_folder = folder
app.static_url_path = folder
app.debug=True

socketio.run(app, host='localhost', port=80, debug=True)

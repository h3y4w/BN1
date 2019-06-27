import json
import sys
import os

config = None 
exc_dir = os.path.dirname(os.path.abspath(__file__))
fn = os.path.dirname(os.path.abspath(__file__))+'/slave_config.json'
with open(fn, 'r') as f:
    config = json.loads(f.read())
os.chdir(config['lib_directory'])

sys.path.append( config['lib_directory'])
config['exc_dir'] = exc_dir
print exc_dir
print '\n\n'

from slaves.command_portal.slave import CommandPortal, app 
#config['CPV1_debug'] = True #option to run webserver in debug 

CommandPortal(config).start()


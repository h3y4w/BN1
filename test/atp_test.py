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

from slaves.APTV1.slave import APTV1 as Slave 
Slave(config).start()



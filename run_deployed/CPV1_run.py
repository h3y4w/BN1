import json
import sys
import os

config = None 
exc_dir = os.path.dirname(os.path.abspath(__file__))
fn = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.json')
with open(fn, 'r') as f:
    config = json.loads(f.read())
if not config:
    raise ValueError("Config has no value: '{}'".format(config))

os.chdir(config['lib_directory'])
sys.path.append( config['lib_directory'])
config['exc_dir'] = exc_dir
from slaves.CPV1.slave import CPV1
CPV1(config).start()


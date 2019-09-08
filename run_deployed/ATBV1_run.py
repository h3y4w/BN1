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
from slaves.ATBV1.slave import ATBV1 
ATBV1(config).start()


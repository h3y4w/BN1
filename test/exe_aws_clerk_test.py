import json
import sys
import os
config = None 
exc_dir = os.path.dirname(os.path.abspath(__file__))
home_path = os.path.expanduser("~")

fn = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.json')

with open(fn, 'r') as f:
    config = json.loads(f.read())
os.chdir(config['lib_directory'])

sys.path.append( config['lib_directory'])
config['exc_dir'] = exc_dir

from slaves.warehouse_clerk.slave import WarehouseClerk


WarehouseClerk(config).start()


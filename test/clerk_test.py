import json
import sys
import os
config = None 
exc_dir = os.path.dirname(os.path.abspath(__file__))
fn = os.path.dirname(os.path.abspath(__file__))+'/../../configs/clerk_config.json'
with open(fn, 'r') as f:
    s = f.read()
    print s
    config = json.loads(s)
os.chdir(config['lib_directory'])

sys.path.append( config['lib_directory'])
config['exc_dir'] = exc_dir

from slaves.WCV1.slave import WCV1 as WarehouseClerk


WarehouseClerk(config).start()


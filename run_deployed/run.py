import json
import sys
import os

from importlib import import_module

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
model_id = config['model_id'] 

slave_module_path = "slaves.{}.slave".format(model_id)
slave_module = import_module(slave_module_path)
slave = getattr(slave_module, model_id)
slave(config).start()


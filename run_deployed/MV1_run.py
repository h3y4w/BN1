import json
import sys
import os

config = None 
exc_dir = os.path.dirname(os.path.abspath(__file__))
#fn = os.path.dirname(os.path.abspath(__file__))+'config.json'
fn = '/home/ubuntu/config.json'
with open(fn, 'r') as f:
    config = json.loads(f.read())

if not config:
    raise ValueError("Config has no value: '{}'".format(config))

os.chdir(config['lib_directory'])

sys.path.append( config['lib_directory'])

config['exc_dir'] = exc_dir


#import bot
print os.getcwd()
print '---'
from masters.masterv1 import MasterV1


MasterV1(config).start()



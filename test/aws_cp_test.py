import json
import sys
import os

config = None 
exc_dir = os.path.dirname(os.path.abspath(__file__))
fn = '/home/den0/Programs/MSystem/BotNetwork1/configs/aws_cp_config.json'
with open(fn, 'r') as f:
    config = json.loads(f.read())
os.chdir(config['lib_directory'])

sys.path.append( config['lib_directory'])
config['exc_dir'] = exc_dir
print exc_dir
print '\n\n'

from slaves.CPV1.slave import CPV1, app 
#config['CPV1_debug'] = True #option to run webserver in debug 

CPV1(config).start()


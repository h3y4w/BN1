import json
import sys
import os
config = None 
exc_dir = os.path.dirname(os.path.abspath(__file__))
home_path = os.path.expanduser("~")

fn = '/home/den0/Programs/MSystem/BotNetwork1/configs/aws_clerk_config.json'

with open(fn, 'r') as f:
    config = json.loads(f.read())
os.chdir(config['lib_directory'])

sys.path.append( config['lib_directory'])
config['exc_dir'] = exc_dir

from slaves.WCV1.slave import WCV1


WCV1(config).start()


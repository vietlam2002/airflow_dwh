import pandas as pd
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(current_dir, '..', 'config.yaml')

utils_path = os.path.join(current_dir, '..', 'utils')
sys.path.append(utils_path)
from cache_utils import load_config, connect_to_mongodb

def export_data_to_postgres():
    config = load_config(config_path)
    create_config = config['create']
    print(create_config['POSTGRES_HOST'])

def export_to_postgres():
    pass

export_data_to_postgres()
import psycopg2
import yaml
import json
from pymongo import MongoClient
from datetime import datetime

def load_config(config_path):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)
    
def connect_to_mongodb(config):
    client = MongoClient(config['MONGODB_CONNECTION_STRING'])
    # print("connect ok!")
    return client[config['MONGODB_DATABASE']]

def get_postgres_connection():
    conn = psycopg2.connect(
        host="14.225.205.7",     
        port=5432,            
        database="go_chat",   
        user="root",      
        password="root"  
    )
    return conn
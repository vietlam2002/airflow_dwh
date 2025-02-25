from pymongo import MongoClient
import pandas as pd
import json
from bson import json_util, ObjectId
from datetime import datetime
import os
import sys
# Lấy đường dẫn thư mục hiện tại
current_dir = os.path.dirname(os.path.abspath(__file__))

# Đường dẫn tuyệt đối đến file config.yaml
config_path = os.path.join(current_dir, '..', 'config.yaml')
print(config_path)
# Thêm thư mục `utils` vào sys.path để có thể import `cache_utils`
utils_path = os.path.join(current_dir, '..', 'utils')
sys.path.append(utils_path)
from cache_utils import load_config, connect_to_mongodb

def convert_objectid_to_string(data):
    for item in data:
        for key, value in item.items():
            if isinstance(value, ObjectId):
                item[key] = str(value)
            elif isinstance(value, list):
                item[key] = ";".join([str(v) for v in value])
    return data

def load_data_mongo():
    config = load_config(config_path)
    create_config = config['create']

    db = connect_to_mongodb(create_config)

    collection = db['projects']
    query = {
        "updatedAt": {"$gt": '2024-11-12 09:03:00'},
        "createdAt": {"$lte": pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}
    }
    # print(query)
    documents = collection.find(query)
    data = list(documents)
    dt_converted = convert_objectid_to_string(data)
    df = pd.DataFrame(dt_converted)
    # print(df['createdAt'].head(1))
    print(f"{len(df)} rows retrieved")
    return df

# load_data_mongo()

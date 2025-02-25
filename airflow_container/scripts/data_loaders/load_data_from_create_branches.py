from pymongo import MongoClient
import pandas as pd
import json
from bson import json_util, ObjectId
from datetime import datetime
from os import path

client = MongoClient("mongodb://datawarehouse_tio:H#nB53k6rGuso6d6@150.95.109.100:27017/portal")
db = client['portal']

collection = db['projects']
documents = collection.find({})
print("hello")
def transform(data):
    if isinstance(data, pd.DataFrame):
        data.rename(columns={"_id": "id"}, inplace=True)
    return data

data = []
for doc in documents:
    serialized_doc = json.loads(json_util.dumps(doc))
    serialized_doc['_id'] = serialized_doc['_id']['$oid']
    if 'children' in serialized_doc:
        serialized_doc['children'] = [
            child['$oid'] for child in serialized_doc['children'] if '$oid' in child
        ]
    data.append(serialized_doc)

df = pd.DataFrame(data)
data_branches = transform(df)
print(df.info())
# print(data_branches.info())
# print("done_load")

import pandas as pd

def transform_data_create_branches(data):
    if isinstance(data, pd.DataFrame):
        data.rename(columns={"_id": "id"}, inplace=True)
    return data

print("Done")
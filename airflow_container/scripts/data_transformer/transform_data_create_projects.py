import pandas as pd

def transform_data_project(data):
    if isinstance(data, pd.DataFrame):
        data.rename(columns={"_id": "id"}, inplace=True)
    print("DONE")
    return data
import pandas as pd

def add_column_if_not_exists(pg_hook, df, schema_name, table_name):
    column_types = df.dtypes
    for column, dtype in column_types.items():
        postgres_dtype = ''
        if dtype == 'int64':
            postgres_dtype = 'BIGINT'
        elif dtype == 'float64':
            postgres_dtype = 'DOUBLE PRECISION'
        elif dtype == 'object':
            postgres_dtype = 'TEXT'
        elif dtype == 'bool':
            postgres_dtype = 'BOOLEAN'
        elif dtype == 'datetime64[ns]':
            postgres_dtype = 'TIMESTAMP'
        else:
            postgres_dtype = 'TEXT'
        query = f'''
                    ALTER TABLE {schema_name}.{table_name}
                    ADD COLUMN IF NOT EXISTS "{column}" {postgres_dtype}
                '''
        pg_hook.run(query)
def clean_dataframe(df):
    for column in df.columns:
        if df[column].dtype == 'object':
            df[column] = df[column].apply(lambda x : x.replace('\x00', '') if isinstance(x, str) else x)
    return df

def add_orther_columns(df, execution_date):  
    df['dwh_created_at'] = pd.to_datetime(execution_date)
    df['dwh_updated_at'] = pd.to_datetime(execution_date)
    return df
                                                                                                                                                                                               
def add_prrimary_key_and_constraint(pg_hook, schema_name, table_name, primary_key_column_name):
    query = f'''
        ALTER TABLE {schema_name}.{table_name}
        ADD PRIMARY KEY ("{primary_key_column_name}");
        ALTER TABLE {schema_name}.{table_name}
        ADD CONSTRAINT {table_name}_unique_{primary_key_column_name}
        UNIQUE ("{primary_key_column_name}");
    '''
    pg_hook.run(query)


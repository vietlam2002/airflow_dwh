import pandas as pd
import psycopg2


def add_column_if_not_exists(connection, df, schema_name, table_name):
    column_types = df.dtypes
    cur = connection.cursor()
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
        cur.execute(query)
        connection.commit()
    cur.close()

def clean_dataframe(df):
    for column in df.columns:
        if df[column].dtype == 'object':
            df[column] = df[column].apply(lambda x : x.replace('\x00', '') if isinstance(x, str) else x)
    return df

def add_other_columns(df, execution_date):  
    df['dwh_created_at'] = pd.to_datetime(execution_date)
    df['dwh_updated_at'] = pd.to_datetime(execution_date)
    return df
                                                                                                                                                                                               
def add_primary_key_and_constraint(connection, schema_name, table_name, primary_key_column_name):
    cur = connection.cursor()
    query = f'''
        ALTER TABLE {schema_name}.{table_name}
        ADD PRIMARY KEY ("{primary_key_column_name}");
        ALTER TABLE {schema_name}.{table_name}
        ADD CONSTRAINT {table_name}_unique_{primary_key_column_name}
        UNIQUE ("{primary_key_column_name}");
    '''
    cur.execute(query)
    connection.commit()
    cur.close()


from google.oauth2 import service_account
from airflow.models import Connection
import pandas as pd
import psycopg2
import json


# extract from file
def extract(path, format):
    if format == 'csv':
        df = pd.read_csv(path, sep=',')
    elif format == 'json':
        df = pd.read_json(path)

    return df

# extract from db
def extract_from_pg():
    conn = None
    try:
        connection = Connection.get_connection_from_secrets(conn_id='pg_conn')
        params = {
            'host': connection.host,
            'port': connection.port,
            'database': connection.schema,
            'user': connection.login,
            'password': connection.password
        }
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute('''SELECT * FROM course_enrollment''')
        query = cur.fetchall()

        cols = []
        for i in cur.description:
            cols.append(i[0])
        
        df = pd.DataFrame(data=query, columns=cols)
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        
    finally:
        if conn is not None:
            conn.close()
    
    return df

# load to BigQuery
def load_to_bq(df, table_id, schema):
    conn = Connection.get_connection_from_secrets(conn_id='to_bq')
    credentials = service_account.Credentials.from_service_account_info(json.loads(conn.extra_dejson['keyfile_dict']))
    credentials = credentials.with_scopes(["https://www.googleapis.com/auth/cloud-platform"])
    dataset = 'dwh'
    
    try:
        df.to_gbq(destination_table=f'{dataset}.{table_id}', if_exists='replace', table_schema=schema, credentials=credentials)
        print('Successfully created table {}.'.format(table_id))

    except Exception as e:
        print(f'An unexpected error occurred: {e}')

def main(path, format, table_id, schema):
    df = extract(path, format)
    load_to_bq(df, table_id, schema)

def main_2(table_id, schema):
    df = extract_from_pg()
    load_to_bq(df, table_id, schema)


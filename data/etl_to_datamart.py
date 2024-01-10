from google.oauth2 import service_account
from airflow.models import Connection
import pandas as pd
import json

# run query from BigQuery
def run_query(query, credentials):
    df = pd.read_gbq(query, credentials=credentials)

    return df

# define schema and create new table for datamart
def create_datamart_table(df, credentials, dataset, table_id, schema):   
    try:
        df.to_gbq(destination_table=f'{dataset}.{table_id}', if_exists='replace', table_schema=schema, credentials=credentials)
        print('Successfully created table {}.'.format(table_id))

    except Exception as e:
        print(f'An unexpected error occurred: {e}')

def main(query, dataset, table_id, schema):
    conn = Connection.get_connection_from_secrets(conn_id='to_bq')
    credentials = service_account.Credentials.from_service_account_info(json.loads(conn.extra_dejson['keyfile_dict']))
    credentials = credentials.with_scopes(["https://www.googleapis.com/auth/cloud-platform"])
    
    df = run_query(query, credentials)
    create_datamart_table(df, credentials, dataset, table_id, schema)


import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../data/'))
import yaml
import pathlib
import sqlparse
from etl_to_datamart import main
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor


def get_config_yaml(path_config_yaml):
    with open(path_config_yaml) as file_config_yaml:
        config_yaml = yaml.load_all(file_config_yaml, Loader=yaml.CLoader)
        for config in config_yaml:
            dict_config = {}

            dict_config['dataset'] = config.get('dataset')
            dict_config['table_id'] = config.get('table_id')
            dict_config['schema'] = config.get('schema')
            dict_config['external_task_dependencies'] = config.get('external_task_dependencies')

    return dict_config


def get_query_sql(path_query_sql):
    with open(path_query_sql) as file_query_sql:
        str_query = file_query_sql.read()
        str_query_parsed = sqlparse.format(str_query, reindent=True, keyword_case='upper')

    return str_query_parsed

with DAG (
    'dag_etl_to_datamart',
    description='Create datamart from dwh in BigQuery',
    schedule_interval='0 2 * * *',
    start_date=datetime(2024, 1, 12),
    # catchup=False
) as dag:

    current_path = pathlib.Path(__file__).absolute()
    config_dir_path = current_path.parent.parent.joinpath('data/datamart_config')
    config_dir = os.listdir(config_dir_path)
    for sub_config_dir in config_dir:
        path_config_yaml = config_dir_path.joinpath(sub_config_dir).joinpath('config.yaml')
        path_query_sql = config_dir_path.joinpath(sub_config_dir).joinpath('query.sql')
        dict_config = get_config_yaml(path_config_yaml)
        dict_config['query'] = get_query_sql(path_query_sql)

        create_datamart_table = PythonOperator(
            task_id=f'create_{sub_config_dir}_table',
            python_callable=main,
            op_kwargs={
                'query': dict_config['query'],
                'dataset': dict_config['dataset'],
                'table_id': dict_config['table_id'],
                'schema': dict_config['schema']
            }
        )

        if dict_config['external_task_dependencies']:
            for ext_task_depen in dict_config['external_task_dependencies']:
                
                task_wait_ext_task = ExternalTaskSensor(
                    task_id=f"wait_{ext_task_depen['dag_id']}_{ext_task_depen['task_id']}",
                    external_dag_id=ext_task_depen['dag_id'],
                    external_task_id=ext_task_depen['task_id'],
                    allowed_states=['success'],
                    execution_delta=timedelta(minutes=ext_task_depen['minutes_delta'])
                )

                create_datamart_table.set_upstream(task_wait_ext_task)


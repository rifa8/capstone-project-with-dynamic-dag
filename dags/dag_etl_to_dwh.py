import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../data/'))
import yaml
import pathlib
from etl_to_dwh import main, main_2
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def get_data_source_path(file_name):
    current_path = pathlib.Path(__file__).absolute()
    file_path = current_path.parent.parent.joinpath(f'data/src/{file_name}')

    return file_path

def get_config_yaml(path_config_yaml):
    with open(path_config_yaml) as file_config_yaml:
        config_yaml = yaml.load_all(file_config_yaml, Loader=yaml.CLoader)
        for config in config_yaml:
            dict_config = {}

            dict_config['file_name'] = config.get('file_name')
            dict_config['format'] = config.get('format')
            dict_config['table_id'] = config.get('table_id')
            dict_config['schema'] = config.get('schema')
    
    return dict_config

with DAG (
    'dag_etl_to_dwh',
    description='ETL to dataset dwh in Bigquery',
    schedule_interval='0 1 * * *',
    start_date=datetime(2024, 1, 12),
    # catchup=False
) as dag:

    current_path = pathlib.Path(__file__).absolute()
    config_dir_path = current_path.parent.parent.joinpath('data/dwh_config')
    config_dir = os.listdir(config_dir_path)
    for sub_config_dir in config_dir:
        path_sub_config_dir = config_dir_path.joinpath(sub_config_dir)

        if sub_config_dir == 'from_db':
            config_files = os.listdir(path_sub_config_dir)
            for config_file in config_files:
                path_config_yaml = config_dir_path.joinpath(sub_config_dir).joinpath(config_file)
                dict_config = get_config_yaml(path_config_yaml)

                task_etl_to_dwh = PythonOperator(
                    task_id=f'from_{config_file[:-5]}',
                    python_callable=main_2,
                    op_kwargs={
                        'table_id': dict_config['table_id'],
                        'schema': dict_config['schema']
                    }
                )     
        else:
            config_files = os.listdir(path_sub_config_dir)
            for config_file in config_files:
                path_config_yaml = config_dir_path.joinpath(sub_config_dir).joinpath(config_file)
                dict_config = get_config_yaml(path_config_yaml)

                task_etl_to_dwh = PythonOperator(
                    task_id=f'from_{config_file[:-5]}',
                    python_callable=main,
                    op_kwargs={
                        'path': get_data_source_path(dict_config['file_name']),
                        'format': dict_config['format'],
                        'table_id': dict_config['table_id'],
                        'schema': dict_config['schema']
                    }
                )


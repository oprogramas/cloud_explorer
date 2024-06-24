from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator

from datetime import datetime, timedelta
import pandas as pd

# criação do dataframe
data = {'column1': [1, 2, 3], 'column2': ['a', 'b', 'c']}
df = pd.DataFrame(data)

# definindo o id do projeto e o nome do dataset
project_id = 'airflow-000000'
dataset_id = 'teste_airflow'
table_id = 'tabela_dois_airflow'

# args do aiflow
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# definições do dag
dag = DAG(
    'bigquery_insert_dag',
    default_args=default_args,
    description='An example DAG to insert data into BigQuery',
    schedule='*/30 * * * *',  # roda a cada 30 minutos
)


# operador para inserir os dados na tabela do bigquery
insert_data_task = PythonOperator(
    task_id='insert_data',
    python_callable=lambda: df.to_gbq(f'{project_id}.{dataset_id}.{table_id}', project_id=project_id, if_exists='replace'),
    dag=dag,
)


# ordem das tasks
insert_data_task


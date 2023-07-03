from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

import random

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine, text
import uuid
from random import randrange
from datetime import timedelta
from datetime import datetime
from typing import List

tutor_database_connection_url = 'postgresql://postgres:mysecretpassword@172.22.118.220:5433/cloud_computing'


def get_max_field_value(db_url, schema_name, table_name, field, **context):
    '''
        Get max value from schema_name.table_name
    '''
    tutor_db_engine = create_engine(db_url)
    select_max_field_query = f'''
            SELECT MAX({field}) AS max_date FROM {schema_name}.{table_name};
        '''

    with tutor_db_engine.connect() as con:
        with con.begin():
            #print(con.execute(text(select_max_field_query)).fetchone()[0])
            field = con.execute(text(select_max_field_query)).fetchone()[0]
            context['ti'].xcom_push(f'max_field_from_{schema_name}.{table_name}', field.strftime("%m/%d/%Y"))

def compare_source_with_aggregation(source_shema, source_table, agregation_schema, agregation_table, **context):
    '''
            Compare max value from source_shema.source_table with max value from agregation_schema.agregation_table
    '''
    print(f'max_field_from_{source_shema}.{source_table}')
    source_value = context['ti'].xcom_pull(key=f'max_field_from_{source_shema}.{source_table}')
    agregation_value = context['ti'].xcom_pull(key=f'max_field_from_{agregation_schema}.{agregation_table}')
    print(source_value)
    if source_value >= agregation_value:
        return 'recalculate'
    else:
        return 'not success'

def recalculate(db_url, source_schema, source_table, agregation_schema, agregation_table, **context):
    tutor_db_engine = create_engine(db_url)
    recalc_query = f'''
                TRUNCATE TABLE {agregation_schema}.{agregation_table};
                INSERT INTO {agregation_schema}.{agregation_table}  SELECT * FROM {source_schema}.{source_table};
            '''

    with tutor_db_engine.connect() as con:
        with con.begin():
            field = con.execute(text(recalc_query))



default_args = {
    'owner': 'Vanislavsky',
    'start_date': datetime(year=2022, month=12, day=12),
    'email_on_failure': True
}

dag = DAG(
    f'test',
    schedule_interval='*/40 * * * *',
    default_args=default_args,
    catchup=False,
    tags=['cdc', 'cdc_events_handler', 'cdc_central_base_custom', 'kfk_to_gp'],
    description=f'Обработка ',
    max_active_runs=1
)

with dag:
    start_op = DummyOperator(task_id='start')

    get_max_sales_date_from_source_op = PythonOperator(
        task_id='get_max_sales_date_from_source',
        python_callable=get_max_field_value,
        op_args=[tutor_database_connection_url, 'source', 'invoice', 'date_modified']
    )

    get_max_sales_date_from_agregation_op = PythonOperator(
        task_id='get_max_sales_date_from_agregation',
        python_callable=get_max_field_value,
        op_args=[tutor_database_connection_url, 'agregation', 'inoice', 'date_upload']
    )

    compare_source_with_agregation_op = BranchPythonOperator(
        task_id='compare_source_with_aggregation',
        python_callable=compare_source_with_aggregation,
            op_args=['source', 'invoice', 'agregation', 'inoice'],
            dag=dag
    )

    recalculate_op = PythonOperator(
        task_id='recalculate',
        python_callable=recalculate,
        op_args=[tutor_database_connection_url, 'source', 'invoice', 'agregation', 'inoice'],
        dag=dag
    )

    noresponse_op = DummyOperator(
        task_id='not_success',
        dag=dag
    )

    end_op = DummyOperator(task_id='end')

    start_op >> [get_max_sales_date_from_source_op, get_max_sales_date_from_agregation_op] >> compare_source_with_agregation_op >> [recalculate_op, noresponse_op] >> end_op


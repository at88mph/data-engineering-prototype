import logging
import requests

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.hooks.http_hook import HttpHook
from urllib import parse as parse
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 7, 26),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'provide_context': True,
    'retry_delay': timedelta(minutes=5)
}

redis_hook = RedisHook(redis_conn_id='redis_default')
http_conn = HttpHook('GET', 'test_netrc')

# TODO - when deploying to have this actually run, catchup=True!!!!!
# and schedule_interval=timedelta(hours=1)

vlass_find_work = DAG('vlass_find_work', default_args=default_args,
                      catchup=True, schedule_interval=timedelta(hours=1))

# provide_context in default_args above must be True to get the kwargs values
def query_vlass(ds, **kwargs):
    prev_date = kwargs['prev_execution_date'].to_datetime_string()
    next_date = kwargs['next_execution_date'].to_datetime_string()
    query_meta = "SELECT fileName FROM archive_files WHERE archiveName ='VLASS'" \
                 " AND ingestDate > '{}' and ingestDate <= '{}'".format(
                     prev_date, next_date)
    data = {'QUERY': query_meta, 'LANG': 'ADQL', 'FORMAT': 'csv'}

    with http_conn.run('/ad/auth-sync?{}'.format(parse.urlencode(data))) as response:
        artifact_files_list = response.text.split()
        logging.error(artifact_files_list)
        return artifact_files_list

# caching to Redis
def cache_query_result(**context):
    results = context['task_instance'].xcom_pull('vlass_periodic_query')
    logging.error(results)
    redis_hook.get_conn().set('vlass_{}'.format(execution_date.strftime('%Y_%m_%d_%H_%M_%S')), results)


t1 = PythonOperator(task_id='vlass_periodic_query',
                    python_callable=query_vlass,
                    provide_context=True,
                    dag=vlass_find_work)

t2 = PythonOperator(task_id='cache_vlass_periodic_query',
                    python_callable=cache_query_result,
                    provide_context=True,
                    dag=vlass_find_work)

t1 >> t2

import airflow
import airflow.settings
import logging
import json
import time

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
# from airflow.plugins.redis_kubernetes_operator import RedisKubernetesOperator
from airflow.models import DAG, Variable
from airflow.hooks.http_hook import HttpHook
from airflow.contrib.hooks.redis_hook import RedisHook

from datetime import datetime, timedelta
from urllib import parse as parse
from urllib import request as request


PARENT_DAG_NAME = 'omm'
CHILD_DAG_NAME = 'run_omm_instance'

config = {'working_directory': '/root/airflow',
          'resource_id': 'ivo://cadc.nrc.ca/sc2repo',
          'use_local_files': False,
          'logging_level': 'DEBUG',
          'task_types': 'TaskType.INGEST'}
docker_image_tag = 'client5'

default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

dag = DAG(dag_id='{}.{}'.format(PARENT_DAG_NAME, default_args['start_date'].strftime("%Y-%m-%d_%H_%M_%S")), catchup=True, default_args=default_args, schedule_interval=None)
redis_key = dag.dag_id

def extract(**kwargs):    
    logging.info('Populating inputs.')
    # start_date = default_args['start_date']
    # end_date = default_args['start_date'] + timedelta(hours=1)
    query = Variable.get('omm_input_uri_query')
    redis = RedisHook(redis_conn_id='redis_default')
    data = {'QUERY': query, 'REQUEST': 'doQuery', 'LANG': 'ADQL', 'FORMAT': 'csv'}
    http_connection = HttpHook(method='GET', http_conn_id='tap_service_host')
    count = -1

    with http_connection.run('/tap/sync?', parse.urlencode(data)) as response:
        redis.get_conn().delete(redis_key)
        arr = response.text.split('\n')
        count = len(arr)
        logging.info('Found {} items.'.format(count))
        sanitized_uris = []
        for uri in arr[1:]:
            if uri:
                artifact_uri = uri.split('/')[1].strip()
                sanitized_artifact_uri = artifact_uri.replace('+', '_').replace('%', '__')
                logging.info('Output is {}'.format(sanitized_artifact_uri))
                sanitized_uris.append(sanitized_artifact_uri)
        redis.get_conn().rpush(redis_key, *sanitized_uris)
    return 'Extracted {} items'.format(len(sanitized_uris))

def print_uris(**kwargs):    
    redis = RedisHook(redis_conn_id='redis_default')
    redis_conn = redis.get_conn()    
    uri_keys = redis_conn.lrange(redis_key, 0, -1)
    logging.info('Looping over {} items.'.format(len(uri_keys)))
    for uri_key in uri_keys:
        decoded_key = uri_key.decode('utf-8')
        logging.info('Next key: {}'.format(decoded_key))
    return 'Transformed {} items'.format(len(uri_keys))

with dag:
    extract_op = PythonOperator(task_id='extract', python_callable=extract, dag=dag)
    load_op = DummyOperator(task_id='complete', dag=dag)    
    transform_op = PythonOperator(task_id='transform', python_callable=print_uris, dag=dag)

    extract_op >> transform_op >> load_op

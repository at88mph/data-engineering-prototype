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
from airflow.models import DAG, Variable
from airflow.hooks.http_hook import HttpHook
from airflow.contrib.hooks.redis_hook import RedisHook

from datetime import datetime, timedelta
from urllib import parse as parse
from urllib import request as request


PARENT_DAG_NAME = 'omm'
CHILD_DAG_NAME = 'run_omm_instance'
REDIS_LIST_NAME = 'redis_omm'

config = {'working_directory': '/root/airflow',
          'resource_id': 'ivo://cadc.nrc.ca/sc2repo',
          'use_local_files': False,
          'logging_level': 'DEBUG',
          'task_types': 'TaskType.INGEST'}
docker_image_tag = 'client5'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 11, 25),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

dag = DAG(dag_id=PARENT_DAG_NAME, default_args=default_args, schedule_interval=None)

def populate_inputs(**kwargs):    
    logging.info('Populating inputs.')
    query = Variable.get('omm_input_uri_query')
    redis = RedisHook(redis_conn_id='redis_default')
    data = {'QUERY': query, 'REQUEST': 'doQuery', 'LANG': 'ADQL', 'FORMAT': 'csv'}
    http_connection = HttpHook(method='GET', http_conn_id='tap_service_host')
    count = -1

    with http_connection.run('/tap/sync?', parse.urlencode(data)) as response:
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
                time.sleep(3)
        redis.get_conn().rpush(REDIS_LIST_NAME, *sanitized_uris)
    
    logging.info('Sleeping...')
    time.sleep(3)

    return 'Finished inserting {} items into Redis.'.format(count)

def sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval, redis_list_name):   
    sub_dag = DAG(
      '{}.{}'.format(parent_dag_name, child_dag_name),
      schedule_interval=schedule_interval,
      start_date=datetime(2019, 11, 25),
    )
    redis = RedisHook(redis_conn_id='redis_default')
    redis_conn = redis.get_conn()
    start_sub_dag = DummyOperator(task_id='{}.start'.format(sub_dag.dag_id), dag=sub_dag)
    complete_sub_dag = DummyOperator(task_id='{}.complete'.format(sub_dag.dag_id), dag=sub_dag)
    logging.info('Looping items.')
    uri_key = redis_conn.rpop(redis_list_name)
    while uri_key:
        decoded_key = uri_key.decode('utf-8')
        logging.info('Next key: {}'.format(decoded_key))
        task = KubernetesPodOperator(
                 namespace='default',
                 task_id='{}.{}'.format(sub_dag.dag_id, decoded_key),
                 image='ubuntu:18.10',
                 in_cluster=True,
                 get_logs=True,
                 cmds=['echo'],
                 arguments=[decoded_key],
                 name='airflow-test-pod',
                 dag=sub_dag)
        uri_key = redis_conn.rpop(redis_list_name)
        start_sub_dag >> task >> complete_sub_dag

    return sub_dag

# start = DummyOperator(task_id='start', dag=dag)
sub_dag_pointer = sub_dag
start = PythonOperator(task_id='populate_inputs', python_callable=populate_inputs, dag=dag)
sub_dag_operator = SubDagOperator(subdag=sub_dag_pointer(PARENT_DAG_NAME, CHILD_DAG_NAME, dag.start_date, dag.schedule_interval, REDIS_LIST_NAME), task_id=CHILD_DAG_NAME, dag=dag)
complete = DummyOperator(task_id='complete', dag=dag)

start >> sub_dag_operator >> complete

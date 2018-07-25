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

dag = DAG(dag_id='omm', default_args=default_args, schedule_interval=None)

def populate_inputs(**kwargs):
    time.sleep(5)
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
   
    redis.get_conn().lpush('list_omm', sanitized_uris)

    return 'Finished inserting {} items into Redis.'.format(count)


def sub_dag(parent_dag_id, child_dag_id, **kwargs):
    redis = RedisHook(redis_conn_id='redis_default')
    sub_dag = DAG(dag_id=child_dag_id, default_args=default_args, schedule_interval=None)
    redis_conn = redis.get_conn()
    start_sub_dag = DummyOperator(task_id='{}.start'.format(child_dag_id), dag=sub_dag)
    complete_sub_dag = DummyOperator(task_id='{}.complete'.format(child_dag_id), dag=sub_dag)
    logging.info('Looping items.')
    uri_key = redis_conn.lpop('list_omm')
    while uri_key:
        decoded_key = uri_key.decode('utf-8')
        logging.info('Next key: {}'.format(decoded_key))
        task = KubernetesPodOperator(
                 namespace='default',
                 task_id='{}.{}'.format(child_dag_id, decoded_key),
                 image='ubuntu:18.10',
                 in_cluster=True,
                 get_logs=True,
                 cmds=['echo'],
                 arguments=[decoded_key],
                 name='airflow-test-pod',
                 dag=sub_dag)
        uri_key = redis_conn.lpop('list_omm')
        start_sub_dag >> task >> complete_sub_dag    

    return sub_dag         

# start = DummyOperator(task_id='start', dag=dag)
start = PythonOperator(task_id='populate_inputs', python_callable=populate_inputs, dag=dag)
sub_dag_task_id = 'run_omm'
sub_dag_id = '{}.{}'.format(dag.dag_id, sub_dag_task_id)
sub_dag = sub_dag(dag.dag_id, sub_dag_id)
sub_dag_operator = SubDagOperator(subdag=sub_dag, task_id=sub_dag_task_id, default_args=default_args, dag=dag)
complete = DummyOperator(task_id='complete', dag=dag)

start >> sub_dag_operator >> complete

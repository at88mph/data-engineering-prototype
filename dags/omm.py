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
from airflow.plugins.redis_kubernetes_operator import RedisKubernetesOperator
from airflow.models import DAG, Variable
from airflow.hooks.http_hook import HttpHook
from airflow.contrib.hooks.redis_hook import RedisHook

from datetime import datetime, timedelta
from urllib import parse as parse
from urllib import request as request


PARENT_DAG_NAME = 'omm'
CHILD_DAG_NAME = 'run_omm_instance'
REDIS_KEY = 'redis_omm'

config = {'working_directory': '/root/airflow',
          'resource_id': 'ivo://cadc.nrc.ca/sc2repo',
          'use_local_files': False,
          'logging_level': 'DEBUG',
          'task_types': 'TaskType.INGEST'}
docker_image_tag = 'client5'

default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

dag = DAG(dag_id='{}.{}'.format(PARENT_DAG_NAME, default_args['start_date'].strftime("%Y-%m-%d_%H_%M_%S")), default_args=default_args, schedule_interval=None)

def extract(**kwargs):    
    logging.info('Populating inputs.')
    query = Variable.get('omm_input_uri_query')
    redis = RedisHook(redis_conn_id='redis_default')
    data = {'QUERY': query, 'REQUEST': 'doQuery', 'LANG': 'ADQL', 'FORMAT': 'csv'}
    http_connection = HttpHook(method='GET', http_conn_id='tap_service_host')
    count = -1

    with http_connection.run('/tap/sync?', parse.urlencode(data)) as response:
        redis.get_conn().delete(REDIS_KEY)
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
        redis.get_conn().rpush(REDIS_KEY, *sanitized_uris)    

# def transform(parent_dag_name, child_dag_name, start_date, schedule_interval, redis_list_name):
#     sub_dag = DAG(
#       '{}.{}'.format(parent_dag_name, child_dag_name),
#       schedule_interval=schedule_interval,
#       start_date=datetime(2019, 11, 25)
#     )
#     redis = RedisHook(redis_conn_id='redis_default')
#     redis_conn = redis.get_conn()
#     start_sub_dag = DummyOperator(task_id='{}.start'.format(sub_dag.dag_id), dag=sub_dag)
#     complete_sub_dag = DummyOperator(task_id='{}.complete'.format(sub_dag.dag_id), dag=sub_dag)
#     logging.info('Looping items.')
#     uri_key = redis_conn.rpop(redis_list_name)
#     while uri_key:
#         decoded_key = uri_key.decode('utf-8')
#         logging.info('Next key: {}'.format(decoded_key))
#         task = KubernetesPodOperator(
#                  namespace='default',
#                  task_id='{}.{}'.format(sub_dag.dag_id, decoded_key),
#                  image='ubuntu:18.10',
#                  in_cluster=True,
#                  get_logs=True,
#                  cmds=['echo'],
#                  arguments=[decoded_key],
#                  name='airflow-test-pod',
#                  dag=sub_dag)
#         uri_key = redis_conn.rpop(redis_list_name)
#         start_sub_dag >> task >> complete_sub_dag

#     return sub_dag

with dag:
    extract_op = PythonOperator(task_id='extract', python_callable=extract, dag=dag)
    # transform_op = SubDagOperator(subdag=transform(PARENT_DAG_NAME, CHILD_DAG_NAME, dag.start_date, dag.schedule_interval, REDIS_KEY), task_id=CHILD_DAG_NAME, dag=dag)
    transform_op = RedisKubernetesOperator(redis_key=REDIS_KEY,
                    namespace='default',
                    task_id='{}.process_data'.format(dag.dag_id),
                    image='ubuntu:18.10',
                    in_cluster=True,
                    get_logs=True,
                    cmds=['echo'],                    
                    name='airflow-test-pod',
                    dag=dag)
    load_op = DummyOperator(task_id='complete', dag=dag)

    extract_op >> transform_op >> load_op

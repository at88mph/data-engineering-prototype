import logging
import requests

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.plugins.cadc_plugins import TransformFileNamesOperator
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from urllib import parse as parse
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 7, 26),
    'email': ['djenkins.cadc@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'provide_context': True,
    'retry_delay': timedelta(minutes=5)
}

volume_mount = VolumeMount('cadc-volume',
                           mount_path='/data/cadc',
                           sub_path=None,
                           read_only=False)

volume_config = {
    'persistentVolumeClaim':
    {
        'claimName': 'cadc-volume'
    }
}
volume = Volume(name='cadc-volume', configs=volume_config)

http_conn_id = 'test_netrc'
redis_conn_id = 'redis_default'
output_format = 'csv'
collection = 'VLASS'
dag_id = 'vlass_processing'
datetime_format = '%Y-%m-%d %H:%M:%S'

# TODO - when deploying to have this actually run, catchup=True!!!!!
# and schedule_interval=timedelta(hours=1)

vlass_dag = DAG(dag_id, default_args=default_args,
                catchup=True, schedule_interval=timedelta(hours=1))


def _to_milliseconds(dt):
    return int(round(dt.timestamp() * 1000))


def query_and_extract(**context):
    http_conn = HttpHook('GET', http_conn_id)
    redis_conn = RedisHook(redis_conn_id)
    prev_exec_date = context.get('prev_execution_date')
    next_exec_date = context.get('next_execution_date')
    query_meta = "SELECT fileName FROM archive_files WHERE archiveName = '{}'" \
        " AND ingestDate > '{}' and ingestDate <= '{}' ORDER BY ingestDate".format(collection,
                                                                                   prev_exec_date.strftime(
                                                                                       datetime_format),
                                                                                   next_exec_date.strftime(datetime_format))
    logging.info('Query: {}'.format(query_meta))
    data = {'QUERY': query_meta, 'LANG': 'ADQL',
            'FORMAT': '{}'.format(output_format)}

    with http_conn.run('/ad/auth-sync?{}'.format(parse.urlencode(data))) as response:
        artifact_files_list = response.text.split()[1:]
        if artifact_files_list:
            redis_key = '{}_{}_{}.{}'.format(collection, _to_milliseconds(
                prev_exec_date), _to_milliseconds(next_exec_date), output_format)
            redis_conn.get_conn().rpush(redis_key, artifact_files_list)
            return redis_key


def sub_dag(child_dag_id, input_file_names, key):
    sub_dag = DAG('{}.{}'.format(dag_id, child_dag_id), default_args=default_args,
                  catchup=False, schedule_interval=vlass_dag.schedule_interval)
    http_conn = HttpHook('GET', http_conn_id)
    auth_conn = HttpHook.get_connection(http_conn_id)

    with http_conn.run('/cred/auth/priv/users/{}'.format(auth_conn.login)) as response:
        cert = response.text
        for idx, x in enumerate(input_file_names):
            KubernetesPodOperator(dag=sub_dag,
                                  namespace='default',
                                  task_id='vlass-transform-{}-{}'.format(
                                      idx, key),
                                  in_cluster=True,
                                  get_logs=True,
                                  cmds=['{}_run_single'.format(
                                      collection.lower())],
                                  arguments=[x, cert],
                                  name='airflow-vlass-transform-pod',
                                  volumes=[volume],
                                  volume_mounts=[volume_mount])

    return sub_dag


def create_transform_task(redis_key):
    redis_conn = RedisHook(redis_conn_id)
    input_file_names = redis_conn.get_conn().lrange(redis_key, 0, -1)
    child_dag_id = '_files_{}'.format(redis_key)
    return SubDagOperator(subdag=sub_dag(child_dag_id, input_file_names, redis_key), task_id=child_dag_id, dag=vlass_dag)


with vlass_dag:
    start_task = DummyOperator(task_id="start_task", dag=vlass_dag)
    extract_task = PythonOperator(
        task_id='vlass-extract-files', dag=vlass_dag, python_callable=query_and_extract)
    prev_exec_date = '{{ ds }}'
    next_exec_date = '{{ ds }}'
    redis_key = '{}_{}_{}.{}'.format(collection, _to_milliseconds(
        prev_exec_date), _to_milliseconds(next_exec_date), output_format)
    transform_task = create_transform_task(redis_key)
    end_task = DummyOperator(task_id="end_task", dag=vlass_dag)

    start_task >> extract_task >> transform_task >> end_task

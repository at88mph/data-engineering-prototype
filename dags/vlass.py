import logging
import requests

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
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

# http_conn = HttpHook('GET', 'test_netrc')
# http_conn = BaseHook.get_connection('test_netrc')
http_conn_id = 'test_netrc'
redis_conn_id = 'redis_default'
output_format = 'csv'
collection = 'VLASS'
datetime_format = '%Y-%m-%d %H:%M:%S'

# TODO - when deploying to have this actually run, catchup=True!!!!!
# and schedule_interval=timedelta(hours=1)

# vlass_dag = DAG('vlass_find_work', default_args=default_args,
#                       catchup=True, schedule_interval=timedelta(hours=1))

vlass_dag = DAG('vlass_processing', default_args=default_args,
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
        artifact_files_list = response.text
        redis_conn.get_conn().set('{}_{}_{}.{}'.format(collection, _to_milliseconds(
            prev_exec_date), _to_milliseconds(next_exec_date), output_format), artifact_files_list)

# def create_filter_task():
    # return ExtractFileNamesOperator(
    #     namespace='default',
    #     task_id='vlass-put-files',
    #     in_cluster=True,
    #     get_logs=True,
    #     http_conn_id='test_netrc',
    #     redis_conn_id='redis_default',
    #     collection='VLASS',
    #     name='airflow-vlass-put-files-pod',
    #     volumes=[volume],
    #     volume_mounts=[volume_mount],
    #     dag=vlass_dag)


def create_transform_task():
    return TransformFileNamesOperator(
        namespace='default',
        task_id='vlass-transform-batch',
        in_cluster=True,
        get_logs=True,
        http_conn_id='test_netrc',
        redis_conn_id='redis_default',
        collection='VLASS',
        name='airflow-vlass-transform-pod',
        volumes=[volume],
        volume_mounts=[volume_mount],
        dag=vlass_dag)


start_task = DummyOperator(task_id="start_task", dag=vlass_dag)
filter_task = PythonOperator(
    task_id='vlass-extract-files', dag=vlass_dag, python_callable=query_and_extract)
transform_task = create_transform_task()
end_task = DummyOperator(task_id="end_task", dag=vlass_dag)

start_task >> filter_task >> transform_task >> end_task

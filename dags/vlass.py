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


def extract_and_transform(**context):
    http_conn = HttpHook('GET', http_conn_id)
    # redis_conn = RedisHook(redis_conn_id)
    auth_conn = HttpHook.get_connection(http_conn_id)
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
        # artifact_files_list = response.text.decode('utf-8').split()[1:]
        artifact_files_list = response.text.split()[1:]
        # redis_key = '{}_{}_{}.{}'.format(collection, _to_milliseconds(
        # prev_exec_date), _to_milliseconds(next_exec_date), output_format)
        # redis_conn.get_conn().rpush(redis_key, artifact_files_list)
        if artifact_files_list:
            with http_conn.run('/cred/auth/priv/users/{}'.format(auth_conn.login)) as cert_response:
                cert = cert_response.text
                for idx, x in enumerate(artifact_files_list):
                    KubernetesPodOperator(namespace='default',
                                          image='opencadc/vlass2caom2',
                                          task_id='vlass-transform-{}'.format(idx),
                                          in_cluster=True,
                                          get_logs=True,
                                          cmds=['{}_run_single'.format(
                                                collection.lower())],
                                          arguments=[x, cert],
                                          name='airflow-vlass-transform-pod',
                                          volumes=[volume],
                                          volume_mounts=[volume_mount],
                                          dag=vlass_dag)

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


# def create_transform_subdag(child_dag_id):
#     redis_conn = RedisHook(redis_conn_id)
#     http_conn = HttpHook('GET', http_conn_id)
#     auth_conn = HttpHook.get_connection(http_conn_id)
#     sub_dag = DAG('{}.{}'.format(dag_id, child_dag_id), default_args=default_args,
#                   schedule_interval=timedelta(hours=1))
#     with http_conn.run('/cred/auth/priv/users/{}'.format(auth_conn.login)) as response:
#         cert = response.text
#         prev_date = _to_milliseconds(context.get('prev_execution_date'))
#         next_date = _to_milliseconds(context.get('next_execution_date'))
#         input_file_names_key = '{}_{}_{}.{}'.format(
#             collection, prev_date, next_date, output_format)
#         input_file_names = redis_conn.get_conn().lrange(input_file_names_key, 0, -1)

#         for x in input_file_names:
#             KubernetesPodOperator(namespace='default',
#                                   task_id='vlass-transform-{}'.format(x),
#                                   in_cluster=True,
#                                   get_logs=True,
#                                   cmd=['{}_run_single'.format(
#                                       collection.lower())],
#                                   arguments=[x, cert],
#                                   name='airflow-vlass-transform-pod',
#                                   volumes=[volume],
#                                   volume_mounts=[volume_mount],
#                                   dag=sub_dag)

#     return sub_dag


# def create_transform_task():
#     child_dag_id = 'vlass_processing_single'
#     return SubDagOperator(subdag=create_transform_subdag(child_dag_id), dag=vlass_dag, task_id=child_dag_id)

# def create_transform_task():
#     return TransformFileNamesOperator(
#         namespace='default',
#         task_id='vlass-transform-batch',
#         in_cluster=True,
#         get_logs=True,
#         http_conn_id='test_netrc',
#         redis_conn_id='redis_default',
#         collection='VLASS',
#         name='airflow-vlass-transform-pod',
#         volumes=[volume],
#         volume_mounts=[volume_mount],
#         dag=vlass_dag)


start_task = DummyOperator(task_id="start_task", dag=vlass_dag)
extract_and_transform_task = PythonOperator(
    task_id='vlass-extract-and-transform', dag=vlass_dag, python_callable=extract_and_transform)
# transform_task = create_transform_task()
end_task = DummyOperator(task_id="end_task", dag=vlass_dag)

# start_task >> filter_task >> transform_task >> end_task
start_task >> extract_and_transform_task >> end_task

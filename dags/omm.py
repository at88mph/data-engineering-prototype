import airflow
import airflow.settings
import logging
import json
import time

from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow.contrib.kubernetes.volume import Volume
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.sensors.redis_key_sensor import RedisKeySensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
from airflow.contrib.hooks.redis_hook import RedisHook

from datetime import datetime, timedelta
from urllib import parse as parse
from urllib import request as request


PARENT_DAG_NAME = 'omm_dag'
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

dags_volume_mount = VolumeMount('airflow-dags',
                                mount_path='/root/airflow/dags',
                                sub_path=None,
                                read_only=True)
dags_volume_config = {
    'persistentVolumeClaim':
        {
            'claimName': 'airflow-dags'
        }
}
dags_volume = Volume(name='airflow-dags', configs=dags_volume_config)

logs_volume_mount = VolumeMount('airflow-logs',
                                mount_path='/root/airflow/logs',
                                sub_path=None,
                                read_only=True)
logs_volume_config = {
    'persistentVolumeClaim':
        {
            'claimName': 'airflow-logs'
        }
}
logs_volume = Volume(name='airflow-logs', configs=logs_volume_config)

dag = DAG(dag_id='{}.{}'.format(PARENT_DAG_NAME, default_args['start_date'].strftime(
    "%Y-%m-%d_%H_%M_%S")), catchup=True, default_args=default_args, schedule_interval=None)
redis_key = 'omm_snapshot'


def extract(**kwargs):
    logging.info('Extracting inputs.')
    redis = RedisHook(redis_conn_id='redis_default')
    uri_keys = redis.get_conn().lrange(redis_key, 0, -1)
    logging.info('Extracted {} items'.format(len(uri_keys)))
    return uri_keys


def transform_and_load(uri, dag, **kwargs):
    decoded_key = uri.decode('utf-8')
    logging.info('Next key: {}'.format(decoded_key))
    return KubernetesPodOperator(
        namespace='default',
        task_id='transform',
        image='ubuntu:18.10',
        in_cluster=True,
        get_logs=True,
        cmds=['echo'],
        arguments=['{}.{}'.format(dag.dag_id, uri)],
        volume_mounts=[dags_volume_mount, logs_volume_mount],
        volumes=[dags_volume, logs_volume],
        name='airflow-test-pod',
        dag=dag)


def load(**kwargs):
    redis = RedisHook(redis_conn_id='redis_default')
    redis.get_conn().delete(redis_key)
    return 'Loaded {}.'.format(redis_key)


with dag:
    # start_op = DummyOperator(task_id='start_dag', dag=dag)
    start_op = RedisKeySensor(
        task_id='redis_key_check',
        redis_conn_id='redis_default',
        poke_interval=5,
        timeout=5,
        soft_fail=True,
        dag=dag,
        key=redis_key)
    complete_op = DummyOperator(task_id='complete_dag', dag=dag)

    complete_op.set_upstream(start_op)
    
    # extract_op = PythonOperator(task_id='extract', python_callable=extract, dag=dag)
    # transform_op = KubernetesPodOperator(
    #             namespace='default',
    #             task_id='transform',
    #             image='ubuntu:18.10',
    #             in_cluster=True,
    #             get_logs=True,
    #             cmds=['echo'],
    #             arguments=[dag.dag_id],
    #             volume_mounts=[dags_volume_mount, logs_volume_mount],
    #             volumes=[dags_volume, logs_volume],
    #             name='airflow-test-pod',
    #             dag=dag)
    # for uri in uris:
    #     start_op >> transform_and_load(uri, dag) >> complete_op
    # transform_op = PythonOperator(task_id='transform', python_callable=print_uris, dag=dag)
    # load_op = PythonOperator(task_id='load', python_callable=load, dag=dag)

import airflow
import airflow.settings
import logging
import json
import time
import re

from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.secret import Secret
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.redis_key_sensor import RedisKeySensor
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.hooks.http_hook import HttpHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG, Connection
from airflow.hooks.base_hook import BaseHook

from datetime import datetime, timedelta

config = {'working_directory': '/root/airflow',
          'resource_id': 'ivo://cadc.nrc.ca/sc2repo',
          'use_local_files': False,
          'logging_level': 'DEBUG',
          'task_types': 'TaskType.INGEST'}

args = {
    'start_date': datetime.utcnow(),
    'provide_context': True,
    'owner': 'airflow',
}

auth_conn = HttpHook.get_connection('test_netrc')
http_conn = HttpHook('GET', 'test_netrc')
redis_hook = RedisHook(redis_conn_id='redis_default')

dag = DAG(
    dag_id='vlass_execute',
    default_args=args,
    schedule_interval=None)

start_task = DummyOperator(task_id='start_task', dag=dag)
end_task = DummyOperator(task_id='end_task', dag=dag)

# provide_context in default_args above must be True to get the kwargs values
def get_file_names(**kwargs):
    prev_date = kwargs['prev_execution_date']
    next_date = kwargs['next_execution_date']
    redis_conn = redis_hook.get_conn()
    redis_keys = redis_conn.keys('vlass_*')
    results = []

    for r in redis_keys:
        key_datetime = datetime.strptime(r[6:], '%Y_%m_%d_%H_%M_%S')
        if prev_date < key_datetime < next_date:
            results.append(redis_conn.get(r).decode('utf-8').split()[1:])

    logging.info('Found {} file names'.format(results))
    return results


def get_caom_command(file_name, count, certificate):
    return KubernetesPodOperator(
        namespace='default',
        task_id='vlass-transform-{}'.format(count),
        image='opencadc/vlass2caom2',
        in_cluster=True,
        get_logs=True,
        cmds=['vlass_run_single'],
        arguments=[file_name, "{}".format(certificate)],
        name='airflow-vlass-transform-pod',
        dag=dag)


with http_conn.run('/cred/auth/priv/users/{}'.format(auth_conn.login)) as response:
    cert = response.text
    counter = 0
    for ii in get_file_names():
        x = get_caom_command(ii, counter, cert)
        counter += 1
        start_task >> x >> end_task

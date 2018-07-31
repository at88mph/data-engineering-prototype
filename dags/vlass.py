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
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG, Variable

from datetime import datetime, timedelta


# FIXME: How to inject a new File URIs?  Dynamically create these DAG scripts?
INPUT_FILE = Variable.get('vlass_input_file_name')
NOISE_FILE = Variable.get('vlass_noise_file_name')
X509_CERT_STRING = Variable.get('vlass_cert')

file_pattern = re.compile('VLASS1\.(.*)-.*', re.IGNORECASE)
file_pattern_match = file_pattern.match(INPUT_FILE)
PARENT_DAG_NAME = 'vlass_dag_{}'.format(file_pattern_match.group(1).replace('+', '_').replace('/', '__'))

config = {'working_directory': '/root/airflow',
          'resource_id': 'ivo://cadc.nrc.ca/sc2repo',
          'use_local_files': False,
          'logging_level': 'DEBUG',
          'task_types': 'TaskType.INGEST'}

docker_registry_secret = Secret('volume', '/root/.docker/config.json', 'airflow-secrets', '.dockerconfigjson')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 11, 25),
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

dags_volume_config= {
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

logs_volume_config= {
    'persistentVolumeClaim':
      {
        'claimName': 'airflow-logs'
      }
    }
logs_volume = Volume(name='airflow-logs', configs=logs_volume_config)


dag = DAG(dag_id='{}'.format(PARENT_DAG_NAME), catchup=True, default_args=default_args, schedule_interval=None)

with dag:
    start_op = DummyOperator(task_id='vlass_start_dag', dag=dag)

    transform_op = KubernetesPodOperator(
                namespace='default',
                task_id='vlass-transform',
                image='bucket.canfar.net/vlass2caom2',
                in_cluster=True,
                get_logs=True,
                cmds=['echo'],
                arguments=[INPUT_FILE],
                volume_mounts=[dags_volume_mount, logs_volume_mount],
                volumes=[dags_volume, logs_volume],
                secrets=[docker_registry_secret],
                name='airflow-vlass-transform-pod',
                dag=dag)         

    complete_op = DummyOperator(task_id='vlass_complete_dag', dag=dag)

    start_op >> transform_op >> complete_op

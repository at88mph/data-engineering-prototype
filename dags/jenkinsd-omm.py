import airflow
import airflow.settings
import logging
import json

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
from airflow.models import DagModel
from airflow.hooks.base_hook import BaseHook

from datetime import timedelta
from urllib import parse as parse
from urllib import request as request


#connection = BaseHook.get_connection("caom2-cred")
#cert = connection.extra
config = {'working_directory': '/root/airflow',
          'resource_id': 'ivo://cadc.nrc.ca/sc2repo',
          'use_local_files': False,
          'logging_level': 'DEBUG',
          'task_types': 'TaskType.INGEST'}
limit = '10'
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

dag = DAG(dag_id='jenkinsd-omm-{}'.format(limit), default_args=default_args, schedule_interval=None)

output_cmd = """
    bash -cx "echo {{ params.uri }}"
"""

def get_artifact_uris(**kwargs):
    query_meta = "SELECT Artifact.uri " \
                "FROM caom2.Artifact AS Artifact " \
                "JOIN caom2.Plane AS Plane  " \
                "ON Artifact.planeID = Plane.planeID " \
                "JOIN caom2.Observation AS Observation " \
                "ON Plane.obsID = Observation.obsID " \
                "WHERE Observation.collection = 'OMM' " \
                "AND Artifact.uri not like '%jpg' " \
                "AND Artifact.uri like 'ad:OMM/%' " \
                "AND Observation.lastModified < '2018-07-01 00:00:00.000' " \
                "LIMIT " + limit
    data = {'QUERY': query_meta, 'REQUEST': 'doQuery', 'LANG': 'ADQL', 'FORMAT': 'csv'}
    url = 'http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/tap/sync?{}'.format(parse.urlencode(data))
    with request.urlopen(url) as response:
        return response.read().decode('utf-8').split('\n')

def op_commands(uri, **kwargs):    
    artifact_uri = uri.split('/')[1].strip()
    sanitized_artifact_uri = artifact_uri.replace('+', '_').replace('%', '__')
    # return BashOperator(task_id='bash_{}'.format(sanitized_artifact_uri),
    #                     bash_command='echo {}'.format(sanitized_artifact_uri), dag=dag)
    output = 'kuber_{}'.format(sanitized_artifact_uri)
    task_id = 'kube_{}'.format(sanitized_artifact_uri)
    logging.info('Output is {}'.format(output))
    return KubernetesPodOperator(
                namespace='default',
                task_id=task_id,
                image='ubuntu:18.10',
                in_cluster=True,
                get_logs=True,
                cmds=[output_cmd],
                name='airflow-test-pod',
                params={'uri': sanitized_artifact_uri},
                dag=dag)            

complete = DummyOperator(task_id='complete', dag=dag)

artifact_uri_array = get_artifact_uris()
logging.info('Found {} items.'.format(len(artifact_uri_array)))

# Skip the first item as it's the column header.
for uri in artifact_uri_array[1:]:
    if uri:
        op_commands(uri) >> complete

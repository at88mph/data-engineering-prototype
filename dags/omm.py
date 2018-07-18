import airflow
import logging

from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.models import DAG

from datetime import timedelta
from urllib import parse as parse
from urllib import request as request


config = {'working_directory': '/root/airflow',
          'netrc_filename': '/root/airflow/test_netrc',
          'resource_id': 'ivo://cadc.nrc.ca/sc2repo',
          'use_local_files': False,
          'logging_level': 'INFO',
          'task_types': 'TaskType.INGEST'}

limit = "1000"


def do_that(**kwargs):
    logging.error(config)
    logging.error(kwargs['artifact'])

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

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

poc_dag = DAG(dag_id='poc', default_args=default_args, schedule_interval=None)


def get_observations(**kwargs):
    query_meta = "SELECT Artifact.uri " \
                 "FROM caom2.Artifact AS Artifact " \
                 "JOIN caom2.Plane AS Plane  " \
                 "ON Artifact.planeID = Plane.planeID " \
                 "JOIN caom2.Observation AS Observation " \
                 "ON Plane.obsID = Observation.obsID " \
                 "WHERE Observation.collection = 'OMM' " \
                 "AND Artifact.uri not like '%jpg' " \
                 "LIMIT " + limit
    data = {"QUERY": query_meta, "REQUEST": "doQuery", "LANG": "ADQL",
            "FORMAT": "csv"}
    url = "http://sc2.canfar.net/sc2tap/sync?{}".format(parse.urlencode(data))
    local_filename, headers = request.urlretrieve(url)
    html = open(local_filename)
    artifact_uri_list = html.readlines()
    html.close()
    artifact_files_list = []
    for uri in artifact_uri_list:
        if uri.startswith('ad:OMM/'):
            artifact_files_list.append(uri.split('/')[1].strip())
    return artifact_files_list


def caom_commands(artifact, **kwargs):
    uri_list = "{{ task_instance.xcom_pull(task_ids='get_observations') }}"
    # return PythonOperator(python_callable=do_that, provide_context=True,
    #                       task_id='meta_{}'.format(artifact),
    #                       dag=poc_dag, op_kwargs={'artifact': artifact})

    # file not found error
    # x = DockerOperator(docker_url='unix:///var/run/docker.sock',
    # connection refused
    # x = DockerOperator(docker_url='tcp://localhost:2375',
    # connection refused
    x = DockerOperator(docker_url='tcp://localhost:2376',
                       command='omm_run {}'.format(artifact),
                       image='opencadc/omm2caom2',
                       network_mode='bridge',
                       task_id='meta_{}'.format(artifact),
                       docker_conn_id='my_docker',
                       dag=poc_dag)
    return x

uri_list = PythonOperator(
    task_id='get_observations',
    python_callable=get_observations,
    dag=poc_dag)

complete = DummyOperator(task_id='complete', dag=poc_dag)

for artifact in get_observations():
    uri_list >> caom_commands(artifact) >> complete

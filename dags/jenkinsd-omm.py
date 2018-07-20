import airflow
import logging
import json

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
from airflow.hooks.base_hook import BaseHook

from datetime import timedelta
from urllib import parse as parse
from urllib import request as request


connection = BaseHook.get_connection("caom2-cred")
cert = connection.extra
config = {"working_directory": "/root/airflow",
          "resource_id": "ivo://cadc.nrc.ca/sc2repo",
          "use_local_files": False,
          "logging_level": "DEBUG",
          "task_types": "TaskType.INGEST"}
limit = "200"
docker_image_tag = "client5"

default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)    
}

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
data = {"QUERY": query_meta, "REQUEST": "doQuery", "LANG": "ADQL", "FORMAT": "csv"}
url = "http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/tap/sync?{}".format(parse.urlencode(data))
with request.urlopen(url) as response:
    artifact_uri_list = response.read().decode('utf-8').split('\n')
    logging.info('Found {} items.'.format(len(artifact_uri_list)))
    # Skip the first item as it's the column header.
    for uri in artifact_uri_list[1:]:
        logging.info('Processing {}'.format(uri))
        artifact_uri = uri.split('/')[1].strip()
        sanitized_artifact_uri = artifact_uri.replace("+", "_").replace("%", "__")
        dag = DAG(dag_id='jenkinsd-poc-{}'.format(sanitized_artifact_uri), default_args=default_args, schedule_interval=None)
        BashOperator(
            task_id="runme_" + sanitized_artifact_uri,
            bash_command='echo "Hello world - {}"'.format(sanitized_artifact_uri),
            dag=dag)

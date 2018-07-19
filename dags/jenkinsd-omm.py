import airflow
import logging
import json

from airflow.operators.python_operator import PythonOperator
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
limit = "1000"
docker_image_tag = "client3"


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


poc_dag = DAG(dag_id='jenkinsd-poc', default_args=default_args, schedule_interval=None)


def get_observations(**kwargs):
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
    data = {"QUERY": query_meta, "REQUEST": "doQuery", "LANG": "ADQL",
            "FORMAT": "csv"}
    url = "http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/tap/sync?{}".format(parse.urlencode(data))
    local_filename, headers = request.urlretrieve(url)
    html = open(local_filename)
    artifact_uri_list = html.readlines()
    html.close()
    artifact_files_list = []
    # Skip the first item as it's the column header.
    for uri in artifact_uri_list[1:]:
        artifact_files_list.append(uri.split('/')[1].strip())
    return artifact_files_list


complete = DummyOperator(task_id='complete', dag=poc_dag)


for artifact in get_observations():
    omm_cmd_args = []
    omm_cmd_args.append("{}".format(artifact))
    omm_cmd_args.append(cert)
    sanitized_artifact_uri = artifact.replace("+", "_").replace("%", "__")
    task = KubernetesPodOperator(image="opencadc/omm2caom2:{}".format(docker_image_tag),
                                 namespace='default',
                                 dag=poc_dag,
                                 startup_timeout_seconds=480,
                                 cmds=["omm_run_single"],
                                 arguments=omm_cmd_args,
                                 image_pull_policy="IfNotPresent",
                                 in_cluster=True,
                                 name="omm-caom2",
                                 get_logs=True,
                                 task_id="meta_{}".format(sanitized_artifact_uri))
    task.set_downstream(complete)

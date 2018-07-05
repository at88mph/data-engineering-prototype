import airflow

from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.models import DAG

from datetime import timedelta
from urllib import parse as parse
from urllib import request as request


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


conn_hook = BaseHook(source=None)
conn = conn_hook.get_connection('test_netrc')


def get_observations(**kwargs):
    query_meta = "SELECT Artifact.uri " \
                 "FROM caom2.Artifact AS Artifact " \
                 "JOIN caom2.Plane AS Plane  " \
                 "ON Artifact.planeID = Plane.planeID " \
                 "JOIN caom2.Observation AS Observation " \
                 "ON Plane.obsID = Observation.obsID " \
                 "WHERE Observation.collection = 'OMM' " \
                 "AND Artifact.uri not like '%jpg' " \
                 "LIMIT 10"
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
    x = DockerOperator(docker_url='unix:///var/run/docker.sock',
                       command='omm_run_single {} {} {}'.format(
                           artifact, conn.login, conn.password),
                       image='omm_run_int',
                       network_mode='bridge',
                       task_id='meta_{}'.format(artifact),
                       dag=poc_dag)
    return x


uri_list = PythonOperator(
    task_id='get_observations',
    python_callable=get_observations,
    dag=poc_dag)

complete = DummyOperator(task_id='complete', dag=poc_dag)

for artifact in get_observations():
    uri_list >> caom_commands(artifact) >> complete

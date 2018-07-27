import airflow
import logging

from airflow.models import DAG, Variable
from airflow.hooks.http_hook import HttpHook
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from urllib import parse as parse
from urllib import request as request


PARENT_DAG_NAME = 'omm_snapshot'

default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(0),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

dag = DAG(dag_id='{}.{}'.format(PARENT_DAG_NAME, default_args['start_date'].strftime(
    "%Y-%m-%d_%H_%M_%S")), catchup=True, default_args=default_args, schedule_interval='@hourly')
redis_key = dag.dag_id


def snapshot(**kwargs):
    """
      Query the TAP service and snapshot the OMM data.  
      #FIXME: The query should have some conditions to limit the data.
    """

    logging.info('Populating inputs.')
    query = Variable.get('omm_input_uri_query')
    redis = RedisHook(redis_conn_id='redis_default')
    data = {'QUERY': query, 'REQUEST': 'doQuery',
            'LANG': 'ADQL', 'FORMAT': 'csv'}
    http_connection = HttpHook(method='GET', http_conn_id='tap_service_host')
    count = -1

    with http_connection.run('/tap/sync?', parse.urlencode(data)) as response:
        arr = response.text.split('\n')
        count = len(arr)
        logging.info('Found {} items.'.format(count))
        sanitized_uris = []
        for uri in arr[1:]:
            if uri:
                artifact_uri = uri.split('/')[1].strip()
                sanitized_artifact_uri = artifact_uri.replace(
                    '+', '_').replace('%', '__')
                logging.info('Output is {}'.format(sanitized_artifact_uri))
                sanitized_uris.append(sanitized_artifact_uri)
        redis.get_conn().rpush(redis_key, *sanitized_uris)
        redis.get_conn().persist(redis_key)
    return 'Extracted {} items'.format(len(sanitized_uris))


start_op = DummyOperator(task_id='start_snapshot', dag=dag)
snapshot_op = PythonOperator(
    task_id='snapshot', dag=dag, python_callable=snapshot)
complete_op = DummyOperator(task_id='complete_snapshot', dag=dag)

start_op >> snapshot_op >> complete_op

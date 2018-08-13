from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.redis_hook import RedisHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 7, 26),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'provide_context': True,
    'retry_delay': timedelta(minutes=5)
}

redis_hook = RedisHook(redis_conn_id='redis_default')

# TODO - when deploying to have this actually run, catchup=True!!!!!
# and schedule_interval=timedelta(hours=1)

vlass_find_work = DAG('vlass_find_work', default_args=default_args,
                      catchup=True,
                      schedule_interval=timedelta(hours=1))


# provide_context must be true to get the kwargs values
def query_vlass(ds, **kwargs):
    prev_date = kwargs['prev_execution_date'].to_datetime_string()
    next_date = kwargs['next_execution_date'].to_datetime_string()
    query_meta = "SELECT fileName FROM archive_files WHERE archiveName ='VLASS'" \
                 " AND ingestDate > '{}' and ingestDate <= '{}'".format(
                    prev_date, next_date)
    data = {"QUERY": query_meta, "LANG": "ADQL", "FORMAT": "csv"}
    from urllib import parse as parse
    url = "http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/ad/auth-sync?{}".format(
        parse.urlencode(data))
    import logging
    logging.error(url)
    import requests
    artifact_files_list = requests.get(url, auth=(
        'goliaths', 'loreenam')).content.decode('utf-8')
    logging.error(artifact_files_list.split())
    return artifact_files_list.split()

# caching to redis attempt


def cache_query_result(**context):
    results = context['task_instance'].xcom_pull('vlass_periodic_query')
    import logging
    logging.error(results)
    redis_hook.get_conn().set('test_key', results)


t1 = PythonOperator(task_id='vlass_periodic_query',
                    python_callable=query_vlass,
                    provide_context=True,
                    dag=vlass_find_work)

t2 = PythonOperator(task_id='cache_vlass_periodic_query',
                    python_callable=cache_query_result,
                    provide_context=True,
                    dag=vlass_find_work)

t1 >> t2

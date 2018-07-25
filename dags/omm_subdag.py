import logging

from datetime import datetime
from airflow.models import DAG
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

def sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval, redis_list_name):   
    sub_dag = DAG(
      '{}.{}'.format(parent_dag_name, child_dag_name),
      schedule_interval=schedule_interval,
      start_date=datetime(2019, 11, 25),
    )
    redis = RedisHook(redis_conn_id='redis_default')
    redis_conn = redis.get_conn()
    start_sub_dag = DummyOperator(task_id='{}.start'.format(sub_dag.dag_id), dag=sub_dag)
    complete_sub_dag = DummyOperator(task_id='{}.complete'.format(sub_dag.dag_id), dag=sub_dag)
    logging.info('Looping items.')
    uri_key = redis_conn.rpop(redis_list_name)
    while uri_key:
        decoded_key = uri_key.decode('utf-8')
        logging.info('Next key: {}'.format(decoded_key))
        task = KubernetesPodOperator(
                 namespace='default',
                 task_id='{}.{}'.format(sub_dag.dag_id, decoded_key),
                 image='ubuntu:18.10',
                 in_cluster=True,
                 get_logs=True,
                 cmds=['echo'],
                 arguments=[decoded_key],
                 name='airflow-test-pod',
                 dag=sub_dag)
        uri_key = redis_conn.rpop(redis_list_name)
        start_sub_dag >> task >> complete_sub_dag

    return sub_dag 

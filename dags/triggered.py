# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from airflow.operators.python_operator import PythonOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.redis_key_sensor import RedisKeySensor
from airflow.models import DAG, Connection
from datetime import datetime

# from airflow.contrib.sensors import RedisKeySensor
import redis


from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


args={
    'start_date': datetime.utcnow(),
    'owner': 'airflow',
}

dag = DAG(
    dag_id='vlass_execute',
    default_args=args,
    schedule_interval=None)

sensor = RedisKeySensor(
    task_id='check_task',
    redis_conn_id='redis_default',
    dag=dag,
    key='test_key')

start_task = DummyOperator(task_id='start_task', dag=dag)
end_task = DummyOperator(task_id='end_task', dag=dag)


def get_file_names():
    redis_conn = redis.StrictRedis()
    results = redis_conn.get('test_key')
    if results is not None:
        file_name_list = results.decode('utf-8').split()[1:]
        # redis_conn.delete('test_key')
        return file_name_list
    else:
        return []


conn = Connection('test_netrc')


def get_caom_command(file_name, count):
    # return DockerOperator(docker_url='unix:///var/run/docker.sock',
    #                       command='vlass_run_single {}'.format(file_name),
    #                       image='vlass',
    #                       network_mode='bridge',
    #                       task_id='meta_{}'.format(count),
    #                       docker_conn_id='my_docker',
    #                       api_version='auto',
    #                       dag=dag)
    # return DockerOperator(docker_url='tcp://localhost:2375',
    #                       command='vlass_run_single {}'.format(file_name),
    #                       image='vlass',
    #                       network_mode='bridge',
    #                       task_id='meta_{}'.format(count),
    #                       api_version='auto',
    #                       dag=dag)
    return BashOperator(task_id='meta_{}'.format(count),
                        bash_command='vlass_run_single {} {}'.format(
                            file_name, conn.extra),
                        dag=dag)


counter = 0
for ii in get_file_names():
    x = get_caom_command(ii, counter)
    counter += 1
    start_task >> x >> end_task

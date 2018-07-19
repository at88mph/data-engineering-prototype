#!/bin/bash
#
#  Licensed to the Apache Software Foundation (ASF) under one   *
#  or more contributor license agreements.  See the NOTICE file *
#  distributed with this work for additional information        *
#  regarding copyright ownership.  The ASF licenses this file   *
#  to you under the Apache License, Version 2.0 (the            *
#  "License"); you may not use this file except in compliance   *
#  with the License.  You may obtain a copy of the License at   *
#                                                               *
#    http://www.apache.org/licenses/LICENSE-2.0                 *
#                                                               *
#  Unless required by applicable law or agreed to in writing,   *
#  software distributed under the License is distributed on an  *
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY       *
#  KIND, either express or implied.  See the License for the    *
#  specific language governing permissions and limitations      *
#  under the License.                                           *

# Wait for up to 10 minutes for everything to be deployed
for i in {1..150}
do
  echo "------- Running kubectl get pods -------"
  PODS=$(kubectl get pods | awk 'NR>1 {print $0}')
  echo "$PODS"
  NUM_AIRFLOW_SCHEDULER_READY=$(echo $PODS | grep airflow-scheduler | awk '{print $2}' | grep -E '([0-9])\/(\1)' | wc -l | xargs)
  NUM_AIRFLOW_WEBSERVER_READY=$(echo $PODS | grep airflow-webserver | awk '{print $2}' | grep -E '([0-9])\/(\1)' | wc -l | xargs)  
  NUM_POSTGRES_READY=$(echo $PODS | grep -E '^postgres' | awk '{print $2}' | grep -E '([0-9])\/(\1)' | wc -l | xargs)
  if [ "$NUM_AIRFLOW_SCHEDULER_READY" == "1" ] && [ "$NUM_AIRFLOW_WEBSERVER_READY" == "1" ] && [ "$NUM_POSTGRES_READY" == "1" ]; then
    break
  else
    echo "Airflow Scheduler instance count is $NUM_AIRFLOW_SCHEDULER_READY"
    echo "Airflow Web Server instance count is $NUM_AIRFLOW_WEBSERVER_READY"
    echo "PostgreSQL instance count is $NUM_POSTGRES_READY"
  fi
  sleep 4
done

SCHEDULER_POD=$(kubectl get pods -o go-template --template '{{range .items}}{{.metadata.name}}{{"\n"}}{{end}}' | grep airflow-scheduler | head -1)
WEBSERVER_POD=$(kubectl get pods -o go-template --template '{{range .items}}{{.metadata.name}}{{"\n"}}{{end}}' | grep airflow-webserver | head -1)

echo "------- Scheduler Pod description -------"
kubectl describe pod $SCHEDULER_POD
echo "------- Web Server Pod description -------"
kubectl describe pod $WEBSERVER_POD
echo "------- webserver logs -------"
kubectl logs $WEBSERVER_POD webserver
echo "------- scheduler logs -------"
kubectl logs $SCHEDULER_POD scheduler
echo "--------------"

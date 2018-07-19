#!/bin/bash

DIRNAME=$(cd "$(dirname "$0")"; pwd)

kubectl delete -f $DIRNAME/postgres.yaml
kubectl delete -f $DIRNAME/airflow-webserver.yaml
kubectl delete -f $DIRNAME/airflow-scheduler.yaml
kubectl delete -f $DIRNAME/secrets.yaml

# Extra cleanup for left over pods.
kubectl delete deployments --all
kubectl delete pods --all

echo "Pods:"
kubectl get pods

echo "Services: "
kubectl get services

echo "Deployments: "
kubectl get deployments

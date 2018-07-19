#!/bin/bash

kubectl delete services airflow-webserver airflow-scheduler postgres-airflow
kubectl delete deployments --all
kubectl delete pods --all
kubectl delete pvc --all
kubectl delete pv --all

echo "Pods:"
kubectl get pods

echo "Services: "
kubectl get services

echo "Deployments: "
kubectl get deployments

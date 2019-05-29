#!/usr/bin/env bash

set -e

kubectl apply -f k8s/aqueduct-pipe-namespace.yaml
kubectl apply -f k8s/aqueduct-pipe-deployer-account.yaml
kubectl apply -f k8s/aqueduct-pipe-service.yaml
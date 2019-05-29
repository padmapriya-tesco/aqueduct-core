#!/usr/bin/env bash

set -e

kubectl apply -f k8s/aqueduct-pipe-namespace.yaml
kubectl apply -f k8s/aqueduct-pipe-deployer-account.yaml

jenkins_pod_id=`kubectl get pod -l app=jenkins -o jsonpath="{.items[0].metadata.name}" -n jenkins-ns`

kubectl cp jenkins/jobs/* jenkins-ns/${jenkins_pod_id}:/var/jenkins_home/jobs

echo "The job xml for the aqueduct pipe Deployer has now be uploaded to jenkins."
echo "Please reload the jenkins configuration from disk using the UI:"
echo "Manage Jenkins > Reload Configuration from Disk"

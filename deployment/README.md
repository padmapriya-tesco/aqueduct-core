# Deployment

## Jenkins

In order to add the Aqueduct Pipe deployer job to Jenkins please run:

```bash
$ ./setup.sh
```

The setup script will also apply the aqueduct-pipe-namespace.yaml and aqueduct-pipe-deployer-account.yaml manifest files
see below.

## Kubernetes manifest files

The re-entrant/idempotent kubernetes manifest files are deployed to kubernetes using:

`kubectl apply -f <file>`

### aqueduct-pipe-namespace.yaml

The namespace manifest file adds a Kubernetes namespace, which allows permissions
to be applied to accounts that need to interact with aqueduct pipe (for example, Jenkins)

The namespace can only be modified by an account that has a role which provides the
relevant permissions. For example, it will not be possible for an account
that can delete pods in the Jenkins namespace to be able to delete pods in the aqueduct pipe
namespace.

### aqueduct-pipe-deployer-account.yaml

The following is added by the namespace manifest file:

* Service account (Jenkins aqueduct pipe deployer account)
  - An account that lives in the Jenkins namespace
* Role
  - A role that offers permissions to perform actions inside the aqueduct pipe namespace
  - Can be bound to none or many accounts
* Role binding
  - A binding that links an account to a role
  
The account is used when deploying and thus needs permissions to create, read, update and delete pods. It
shouldn't have access to do something in another application's namespace and thus it will be bound to the
namespace created above.

## aqueduct-pipe-deployment.yaml

The deployment file that provisions the aqueduct pipe container inside a pod on the cluster

## aqueduct-pipe-service.yaml

The service file that exposes the pods through an endpoint

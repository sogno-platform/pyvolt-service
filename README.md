# README

## Installation

First, install dependencies

```
pip install -r requirements.txt
```

Then, install the state estimation algorithm package itself by moving to the submodule folder

```
cd state-estimation
```

and following [the corresponding instructions](https://git.rwth-aachen.de/acs/core/automation/python-state-estimation/)

## Docker
```
docker build --no-cache -t pyvolt-service -f docker/Dockerfile .
docker run --env-file docker/env-file.txt pyvolt-service

```
The file `docker/env-file.txt` can be used to pass environment variables into the container.

## Kubernetes

### Install local Docker regestry

```
docker run -d -p 5000:5000 --restart=always --name registry registry:2
```

### Install pyvolt-service using helm and local docker regestry


```
docker build --no-cache -t pyvolt-service -f docker/Dockerfile .
docker tag pyvolt-service:latest localhost:5000/pyvolt-service:latest
docker push localhost:5000/pyvolt-service
helm install pyvolt-service helm -f mqtt-values.yaml 

```
The file `mqtt-values.yaml` contains an example values file for providing mqtt configuration to the SE container.



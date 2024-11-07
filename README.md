# LDI reference project for PySpark consuming Apache Kafka topic
Created: 7/11/24
This project template attend to show:
- project structure for PySpark
- with GH Action CICD
- with PYSpark consuming Kafka topics
- using PySpark structured streaming (Spark SQL). We do not use DStream (RDD)

## Prerequisites
Install KIND
Install DOCKER DESKTOP (for getting locally K8S) and run it with K8S enabled

## Enable venv
`source .venv/bin/activate`


## Infrastructure 

### Kafka cluster 
We will deploy a Kafka cluster using Kind.
Please refer to detailed instructions here: 
`https://medium.com/@martin.hodges/deploying-kafka-on-a-kind-kubernetes-cluster-for-development-and-testing-purposes-ed7adefe03cb`
This will create one control-plane node (master) and 3 worker nodes.
You can also see that we expose the network port 30092 on our host machine. 
<br>This is because Kind implements its Kubernetes nodes as Docker containers 
and we need to expose any nodePort services to our local machine. In this case, it is port 30092.




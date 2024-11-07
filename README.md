# LDI reference project for PySpark consuming Apache Kafka topic
Created: 7/11/24
This project template attends to show:
- project structure for PySpark
- with GH Action CICD
- with PYSpark consuming Kafka topics
- using PySpark structured streaming (Spark SQL). We do not use DStream (RDD)

## Prerequisites
Install KIND
Install DOCKER DESKTOP (for getting locally K8S) and run it with K8S enabled
Install HELM
Install Kafka locally in MacOs

## Enable venv
`source .venv/bin/activate`


## Infrastructure 

### Kafka cluster 
We use Docker.
`cd infrastructure`
`docker-compose up -d`

Connect to Kafka:
`localhost:9093`

This setup uses Bitnami’s Kafka and Zookeeper images with anonymous login for easy local development.

### Let's create a topic:
Install locally Kafka client. 
`brew install kafka`
Find the container name : `docker ps`

Create the topic: 
`docker exec -it infrastructure-kafka-1 \ 
    kafka-topics.sh --create --topic my-topic \
    --bootstrap-server localhost:9092 \ 
    --partitions 1 --replication-factor 1`







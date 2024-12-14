# LDI reference project for PySpark consuming Apache Kafka topic
Created: 7/11/24
This project template attends to show:
- project structure for PySpark
- with GH Action CICD
- with PYSpark consuming Kafka topics
- using PySpark structured streaming (Spark SQL). We do not use DStream (RDD)

## Prerequisites
Install DOCKER DESKTOP (for getting locally K8S) and run it with K8S enabled

## Enable venv
`source .venv/bin/activate`


## Infrastructure : 
### setup and start Kafka local dev cluster
`docker-compose -f infrastructure/docker-compose.yaml up -d`

### Access the Kafka container 
`docker exec -it kafka bash`

### Create a topic 
```
/usr/bin/kafka-topics --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### Send messages to Kafka (producer)
```
/usr/bin/kafka-console-producer --broker-list localhost:9092 --topic test-topic
```

###  Read Messages from Kafka (Consuming) 
```
/usr/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic test-topic --from-beginning
```

Tested on 30/11/24 it works


## Run Spark scripts

```
python -m app.application --catalog_path='data/cat_mini.csv'
```
or from Pycharm (after setup of working dir to project root)

Tested on 30/11/24 works

## AWS S3
See Notions for detailed instructions
- check Spark and Hadoop versions
- copy in $SPARK_HOME/jars the proper libs:
```
aws-java-sdk-bundle-1.11.901.jar
hadoop-aws-3.3.2.jar
```

NB: The JARS in `.venv...../pyspark/jars` are not considered when running PySpark from the .venv enable..


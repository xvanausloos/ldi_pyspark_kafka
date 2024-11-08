import logging
from venv import logger

from kafka3 import KafkaProducer
from kafka3.errors import KafkaTimeoutError, KafkaError

logging.basicConfig(level=logging.INFO)

class Producer:
    def __init__(self):
        self._init_kafka_producer()

    def _init_kafka_producer(self):
        self.kafka_host="localhost:9092"
        self.kafka_topic="my_topic"
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_host, value_serializer=lambda v: json.dumps(v).encode(),
        )

    def publish_to_kafka(self, message:str):
        try:
            self.producer.send(topic=self.kafka_topic, value=message)
            self.producer.flush()
        except KafkaError as ex:
            logging.error(f"Exception {ex}")
        else:
            logging.info(f"Published message {message} into topic {self.kafka_topic}")




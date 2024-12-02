import json
import logging
import time
from datetime import datetime

import faker
from kafka3 import KafkaProducer
from kafka3.errors import KafkaError

logging.basicConfig(level=logging.INFO)

class Producer:
    def __init__(self):
        self._init_kafka_producer()

    def _init_kafka_producer(self):
        self.kafka_host = "localhost:9092"
        self.kafka_topic = "my_topic"
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_host,
            value_serializer=lambda v: json.dumps(v).encode(),
        )

    def publish_to_kafka(self, message):
        try:
            self.producer.send(topic=self.kafka_topic, value=message)
            self.producer.flush()
        except KafkaError as ex:
            logging.error(f"Exception {ex}")
        else:
            logging.info(f"Published message {message} into topic {self.kafka_topic}")

    @staticmethod
    def create_random_email() -> dict:
        f = faker.Faker()

        new_contact = dict(
            username=f.user_name(),
            first_name=f.first_name(),
            last_name=f.last_name(),
            email=f.email(),
            date_created=str(datetime.utcnow()),
        )

        return new_contact

if __name__ == "__main__":
    producer = Producer()
    while True:
        count = 0
        while count < 3000:
            random_email = producer.create_random_email()
            producer.publish_to_kafka(random_email)
        time.sleep(1)

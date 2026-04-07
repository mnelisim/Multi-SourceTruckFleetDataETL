import os
import pandas as pd
import time
import json
import sys
import threading
from ingestion.truck_simulator import DataExtractor
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
from utils.logger_factory import LoggerFactory 
from config import KAFKA_BROKER, KAFKA_TOPIC

class Producer:

    def __init__(self):
        # 1. Initialize Logger for this class
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)
        self.extract = DataExtractor()

        # STEP 1: Wait for Kafka readiness
        self.wait_for_kafka()

        # STEP 2: Create topic AFTER Kafka is ready
        self.create_topic()

        # STEP 3: Create producer
        self.create_producer()

    def wait_for_kafka(self):
        while True:
            try:
                admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
                topics = admin.list_topics()
                self.logger.info(f"Kafka READY. Topics: {topics}")
                return
            except Exception as e:
                self.logger.error(f"Waiting for Kafka: {e}")
                time.sleep(5)

    def create_topic(self):
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
        topic_list = [NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)]

        try:
            admin.create_topics(new_topics=topic_list)
            self.logger.info("Topic created!")
        except Exception as e:
            self.logger.error(f"Topic exists or error: {e}")

    def create_producer(self):
        while True:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BROKER,
                    retries=5,
                    request_timeout_ms=30000,
                    api_version_auto_timeout_ms=30000,
                    metadata_max_age_ms=5000,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                self.logger.info("Kafka Producer connected!")
                return
            except Exception as e:
                self.logger.error(f"Kafka not ready for producer: {e}")
                time.sleep(5)

    def producer_data(self, truck_id):
        self.logger.info("--- Producer: Sending Raw Data to Kafka ---")
        self.logger.info(f"--- Starting {truck_id} Journey ---")
        # Get generated from simulator
        telemetry_points = self.extract.get_truck_data(truck_id)
        for telemetry_point in telemetry_points:
            # Send raw dictionary data to Kafka Topic
            self.producer.send('raw_truck_data', value=telemetry_point)
            self.logger.info(f"Ingested: {telemetry_point['truck_id']} at {telemetry_point['latitude']}")
        self.producer.flush()

    def run_fleet(self):
        """Starts 10 trucks at the SAME TIME using threads"""
        threads = []
        for i in range(1, 11):
            truck_id = f"TRUCK_REAL_{i:03d}"
            t = threading.Thread(target=self.producer_data, args=(truck_id,))
            threads.append(t)
            t.start()
            time.sleep(0.2) # Small delay to stagger starts

        for t in threads:
            t.join()
        
        self.producer.close()
            
if __name__ == "__main__":
    try:
        p = Producer()
        p.producer_data()
    except KeyboardInterrupt:
            p.logger.warning("\nThe was an error when producing data.")

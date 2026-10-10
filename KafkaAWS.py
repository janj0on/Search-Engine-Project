import os
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
import boto3
from confluent_kafka import *
import json
from dotenv import load_dotenv

load_dotenv()

consumer_config= {
    "bootstrap.servers": os.getenv("CONSUMER_CONFIG"),
    "group.id": os.getenv("GROUP_AWSID"),
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
}
consumer= Consumer(consumer_config)

topic_aws= os.getenv("KAFKA_AWS_TOPIC")
index_name = os.getenv("OPENSEARCH_INDEX")


host = os.getenv("OPENSEARCH_HOST")
region = os.getenv("AWS_REGION")

credentials = boto3.Session().get_credentials()

auth = AWSV4SignerAuth(
    credentials,
    region,
    "es"
)


client = OpenSearch(
    hosts=[{"host": host, "port": 443}],
    http_auth=auth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)


if not client.indices.exists(index=index_name):
    client.indices.create(index=index_name)
    print("Created index:", index_name)


consumer.subscribe([topic_aws])
print("Waiting for Kafka messages...")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue

            print("Kafka error:", msg.error())
            continue

        try:
            
            document = json.loads(msg.value().decode("utf-8"))

            if not isinstance(document, dict):
                raise ValueError("Expected a JSON object")

            
            document_id = document.get("id")

            if document_id is not None:
                client.index(
                    index=index_name,
                    id=str(document_id),
                    body=document,
                    refresh=False
                )
            else:
                client.index(
                    index=index_name,
                    body=document,
                    refresh=False
                )

            
            consumer.commit(message=msg, asynchronous=False)

            print("Indexed document:", document_id or "(generated ID)")

        except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as e:
            print("Invalid Kafka message:", e)
            

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    consumer.close()
    print("Kafka consumer closed.")
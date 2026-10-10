from confluent_kafka import *
import os
import json
import time
from dotenv import load_dotenv
from elasticsearch import Elasticsearch

load_dotenv()
# THIS IS FOR ON-PREM
consumer_config={
    "bootstrap.servers": os.getenv("CONSUMER_CONFIG"),
    "group.id": os.getenv("GROUP_ID"),
    "auto.offset.reset": "earliest"
}

consumer= Consumer(consumer_config)

consumer.subscribe(["valid_docs"])

es= Elasticsearch(os.getenv("ELASTIC_UI"))

INDEX_NAME= os.getenv("INDEX_NAME")

if not es.ping():
     raise Exception("Cannot connect to Elasticsearch")

print("Connected to Elasticsearch")

if not es.indices.exists(index=INDEX_NAME):
     es.indices.create(index=INDEX_NAME)
     print(f"Created index: {INDEX_NAME}")


print("Waiting for VALID documents from Flink...")

print("Waiting for VALID documents from Flink...")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("Kafka error:", msg.error())
            continue

        document = json.loads(msg.value().decode("utf-8"))

        print(f"Received valid document: {document.get('docID')}")

        es.index(
            index=INDEX_NAME,
            id=str(document["docID"]),
            document=document
        )

        print(f"Stored document {document['docID']} in Elasticsearch")

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    consumer.close()
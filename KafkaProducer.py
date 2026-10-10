from confluent_kafka import *
import os
import json
import time
import ir_datasets
from dotenv import load_dotenv

load_dotenv()

prod_config = {
    "bootstrap.servers": os.getenv("PRODUCER_CONFIG")
}

prod = Producer(prod_config)


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed:", err)
    else:
        print(
            f"Message delivered to {msg.topic()} "
            f"[partition {msg.partition()}]"
        )


dataset = ir_datasets.load("cranfield")

for count, doc in enumerate(dataset.docs_iter()):

    

    doc_data = {
        "docID": doc.doc_id,
        "title": doc.title,
        "text": doc.text,
        "author": doc.author,
        "bib": doc.bib
    }

    value = json.dumps(doc_data).encode("utf-8")

    prod.produce(
        os.getenv("KAFKA_TOPIC"),
        value=value,
        callback=delivery_report
    )

    prod.flush()

    print("Sent:", doc.doc_id)

    
    


# Cranfield Search Engine Project

A distributed document processing and search pipeline built using **Apache Kafka, Apache Flink, Python, and Amazon OpenSearch**. The project processes research documents from the Cranfield dataset, validates incoming data through stream processing, and indexes valid documents for full-text search.

## Project Overview

This project demonstrates how to build a data pipeline that ingests research documents, processes them in real time, and makes them searchable through OpenSearch.

The pipeline uses Kafka for message streaming, Flink for data validation, and Amazon OpenSearch for indexing and retrieving relevant research papers.

## Architecture

```text
Cranfield Dataset
       |
       v
Python Kafka Producer
       |
       v
Kafka Topic: doc_topic
       |
       v
Apache Flink SQL
(Data Validation)
       |
       v
Kafka Topic: valid_docs
       |
       v
Python Kafka Consumer
       |
       v
Amazon OpenSearch
       |
       v
Full-Text Search
```

## Technologies Used

* **Python** — document ingestion and Kafka consumer logic
* **Apache Kafka** — distributed event streaming
* **Apache Flink SQL** — streaming data validation
* **Amazon OpenSearch Service** — document indexing and full-text search
* **OpenSearch Dashboards** — exploring indexed documents and executing queries
* **Jupyter Notebook** — dataset exploration and search experiments
* **Docker Compose** — running the Kafka and Flink services
* **Cranfield Dataset** — research documents used as the data source

## Project Structure

```text
Search-Engine-Project/
├── docker/
│   ├── Dockerfile
│   └── Flink-Kafka-docker-compose.yml
├── kafka/
│   ├── KafkaProducer.py
│   ├── KafkaConsumer.py
│   └── KafkaAWS.py
├── notebooks/
│   ├── Data_Reading.ipynb
│   └── ElasticSearch.ipynb
├── flink/
│   └── SQL scripts and configuration
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

*Note: Adjust the tree if your actual filenames or folder contents differ.*

## How It Works

### 1. Data Ingestion

The Python producer loads research documents from the Cranfield dataset using `ir_datasets`. Each document is serialized as JSON and published to the Kafka topic `doc_topic`.

Document fields include:

* `docID`
* `title`
* `text`
* `author`
* `bib`

### 2. Stream Processing and Validation

Apache Flink SQL consumes documents from the Kafka source table and validates them before indexing.

The validation rules ensure that:

* The document ID is not null and is positive.
* The title is not null or empty.
* The document text is not null or empty.

Valid documents are enriched with a processing timestamp and published to the Kafka topic `valid_docs`.

### 3. Document Indexing

A Python Kafka consumer reads valid documents from `valid_docs` and indexes them into Amazon OpenSearch.

The OpenSearch index used in this project is `cranfield-documents`.

### 4. Full-Text Search

OpenSearch supports searching across document titles and text. Title matches receive a higher boost to help prioritize relevant research papers.

Example query:

```json
GET cranfield-documents/_search
{
  "query": {
    "multi_match": {
      "query": "turbulent flow",
      "fields": ["title^3", "text"]
    }
  },
  "size": 10
}
```

This query searches for the phrase's terms across titles and document text, with title matches weighted more heavily.

## Setup and Installation

### Prerequisites

* Python 3
* Docker Desktop
* Apache Kafka and Apache Flink configured through Docker Compose
* AWS account with appropriate permissions, if using Amazon OpenSearch
* Git

### 1. Clone the Repository

```bash
git clone https://github.com/janj0on/Search-Engine-Project.git
cd Search-Engine-Project
```

### 2. Install Python Dependencies

```bash
python -m pip install -r requirements.txt
```

### 3. Configure Environment Variables

Create a local `.env` file using `.env.example` as a template.

Configure the Kafka bootstrap server, Kafka topic names, AWS region, and OpenSearch endpoint as required by your scripts.

**Never commit real AWS credentials, passwords, or API keys to GitHub.** Use an appropriate AWS credentials profile or IAM role for authentication.

### 4. Start Kafka and Flink

```bash
docker compose -f docker/Flink-Kafka-docker-compose.yml up -d
```

The Compose file must contain the correct build context and service configuration for the project layout.

### 5. Run the Producer

From the project root:

```bash
python kafka/KafkaProducer.py
```

The producer publishes Cranfield documents to the configured Kafka topic.

### 6. Run Flink SQL

Open the Flink SQL Client inside the running JobManager container:

```bash
docker exec -it flink-jobmanager ./bin/sql-client.sh
```

Execute the Flink SQL source and sink table definitions, followed by the validation `INSERT INTO` query.

Ensure that the Kafka connector JAR is available to Flink and that the Kafka bootstrap address matches the Docker network configuration.

### 7. Run the Consumer

Run the appropriate consumer script from the project root:

```bash
python kafka/KafkaAWS.py
```

Use `KafkaConsumer.py` instead if that is the script configured to index documents into OpenSearch. Confirm which file contains your final consumer implementation before running it.

## Security and Cost Notes

* Keep `.env` out of version control.
* Use least-privilege AWS permissions.
* Stop local Docker services when they are no longer needed:

```bash
docker compose -f docker/Flink-Kafka-docker-compose.yml down
```

* Amazon OpenSearch is an AWS service that may incur charges while resources are provisioned. Delete unused cloud resources and check AWS Billing and Credits for any outstanding charges.

## Learning Outcomes

Through this project, I explored:

* Building a Kafka-based document ingestion pipeline
* Streaming data validation using Flink SQL
* Integrating Python applications with Kafka
* Indexing and retrieving documents using OpenSearch
* Working with Docker-based data infrastructure
* Managing cloud resources and credentials securely

## Future Improvements

* Route invalid documents to a dedicated Kafka error topic.
* Build a simple web-based search interface.
* Improve search relevance using additional ranking strategies.
* Add automated tests and monitoring.
* Deploy the pipeline using a scalable cloud architecture.

## Dataset

The project uses the **Cranfield collection**, a research document dataset commonly used in information retrieval experiments.



CREATE TABLE documents (
    docID INT,
    title STRING,
    text STRING,
    author STRING,
    bib STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'doc_topic',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-cranfield-validation',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);
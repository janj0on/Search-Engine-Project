DROP TABLE valid_documents;

CREATE TABLE valid_documents (
    docID INT,
    title STRING,
    text STRING,
    author STRING,
    bib STRING,
    processed_at TIMESTAMP_LTZ(3)
) WITH (
     'connector' = 'kafka',
    'topic' = 'valid_docs',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-cranfield-validation',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);






INSERT INTO valid_documents
SELECT
    docID,
    title,
    text,
    author,
    bib,
    CURRENT_TIMESTAMP AS processed_at
FROM Documents
WHERE docID IS NOT NULL
  AND docID > 0
  AND title IS NOT NULL
  AND TRIM(title) <> ''
  AND text IS NOT NULL
  AND TRIM(text) <> '';



CREATE TABLE invalid_documents (
    docID INT,
    title STRING,
    text STRING,
    author STRING,
    bib STRING,
    validation_error STRING,
    processed_at TIMESTAMP_LTZ(3)
) WITH (
     'connector' = 'kafka',
    'topic' = 'invalid_docs',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-cranfield-validation',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);



INSERT INTO invalid_documents
SELECT
    docID,
    title,
    text,
    author,
    bib,
    CASE
        WHEN docID IS NULL THEN 'Missing document ID'
        WHEN docID <= 0 THEN 'Document ID must be positive'
        WHEN title IS NULL OR TRIM(title) = ''
            THEN 'Missing title'
        WHEN text IS NULL OR TRIM(text) = ''
            THEN 'Missing document text'
        ELSE 'Invalid document'
    END AS validation_error,
    CURRENT_TIMESTAMP AS processed_at
FROM Documents
WHERE docID IS NULL
   OR docID <= 0
   OR title IS NULL
   OR TRIM(title) = ''
   OR text IS NULL
   OR TRIM(text) = '';
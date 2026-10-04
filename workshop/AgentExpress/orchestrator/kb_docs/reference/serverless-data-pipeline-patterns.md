# Serverless Data Pipeline Patterns on AWS

This reference document is sample content for the Knowledge Base used by the
`knowledge_research` (RAG) agent. Replace it with your own reference material.

## Ingestion

- **Amazon Kinesis Data Streams** suits high-throughput, ordered, real-time
  event ingestion where multiple consumers read the same stream.
- **Amazon Data Firehose** is the simplest path to land streaming data into
  Amazon S3, Amazon Redshift, or OpenSearch with buffering and optional
  transformation, and requires no consumer application to manage.
- **Amazon SQS** decouples producers from consumers for at-least-once,
  unordered work; use a FIFO queue when strict ordering and exactly-once
  processing are required.

## Processing

- **AWS Lambda** fits short, event-driven transforms (under 15 minutes) and
  scales automatically per event. It is a strong default for glue and
  record-level transformation.
- **AWS Glue** (Spark or Python shell jobs) fits larger batch ETL, schema
  discovery via crawlers, and a managed data catalog.
- **AWS Step Functions** orchestrates multi-step workflows with retries, error
  handling, and human-approval steps, and is well suited to coordinating Lambda,
  Glue, and other services without custom orchestration code.

## Storage

- **Amazon S3** is the common landing zone and data lake foundation. Partition
  data by date or another high-cardinality key to reduce scanned bytes.
- Use lifecycle policies to transition cold data to lower-cost storage classes.
- Store data in a columnar format such as Apache Parquet to cut query cost for
  analytical workloads.

## Query and analytics

- **Amazon Athena** queries data directly in S3 with standard SQL and no
  servers to manage; cost scales with data scanned, which partitioning reduces.
- **Amazon Redshift** (including Redshift Serverless) suits repeated,
  low-latency analytical queries over large, structured datasets.

## Operational guidance

- Prefer managed, serverless services first to minimize undifferentiated
  operational work.
- Make each stage idempotent so retries do not corrupt downstream state.
- Emit metrics and structured logs from every stage and alarm on failure rates
  and processing lag.

# Well-Architected Notes (Sample Reference)

This reference document is sample content for the Knowledge Base used by the
`knowledge_research` (RAG) agent. Replace it with your own reference material.

## Operational excellence

- Automate deployments with infrastructure as code so environments are
  reproducible and changes are reviewable.
- Instrument workloads with metrics, logs, and traces, and define alarms tied to
  business-relevant thresholds rather than raw resource counters alone.

## Security

- Apply least-privilege IAM: grant each component only the actions and resources
  it needs, and scope permissions to specific ARNs.
- Encrypt data at rest and in transit by default; manage keys with a dedicated
  key management service.
- Keep secrets out of source control and configuration files; inject them at
  deploy or run time from a secrets manager or environment variables.

## Reliability

- Design for failure: use retries with backoff, idempotent operations, and dead
  letter queues for messages that cannot be processed.
- Prefer managed services with built-in redundancy over self-managed components
  where practical.

## Performance efficiency

- Choose the compute model that matches the workload shape: event-driven
  functions for spiky, short tasks; batch jobs for large throughput.
- Cache expensive, repeatable lookups, and avoid re-fetching data that has not
  changed.

## Cost optimization

- Match storage classes and compute sizing to actual access patterns.
- Reduce data scanned in analytical queries by partitioning and using columnar
  formats.
- Turn off or scale down non-production resources when idle.

## Sustainability

- Right-size resources and prefer serverless, which scales to zero when idle,
  to avoid paying for and powering unused capacity.

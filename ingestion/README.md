## Ingestion

This module contains code for ingesting raw source data into Databricks.

Data is loaded from external files into the Bronze layer with minimal transformation, preserving the original schema and values. The goal of this layer is to ensure reproducibility and traceability of raw data.

### Responsibilities
- Read raw data sources (CSV)
- Apply minimal schema inference
- Persist raw data as Delta tables (Bronze layer)
- Add ingestion metadata where applicable

# End-to-End Databricks Lakehouse Analytics Pipeline

This project demonstrates an end-to-end data pipeline built using Databricks and Delta Lake, covering data ingestion, transformation, analytics, and basic machine learning.

## Architecture
The pipeline follows a Bronze–Silver–Gold (Medallion) architecture:
- **Bronze**: Raw data ingestion with minimal transformation
- **Silver**: Cleaned and standardized data
- **Gold**: Analytics-ready and ML-ready datasets

## Tech Stack
- Databricks (PySpark, Spark SQL)
- Delta Lake
- Python
- SQL
- scikit-learn

## Project Structure
(databricks-lakehouse-analytics/)
├── ingestion/

├── transformations/

├── analytics/

├── modeling/

## Status
🚧 In progress — building incrementally with a production-style approach.

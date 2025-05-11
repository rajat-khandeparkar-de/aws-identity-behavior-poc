# AWS Data Engineering POC – Identity & Behavior Targeting (Delivery Hero Inspired)

This repo contains a complete data engineering pipeline on AWS to simulate identity resolution, behavioral feature engineering, churn scoring, and campaign generation for a global consumer product.

## 💡 Use Case
Built for Delivery Hero-style use cases involving:
- Identity resolution
- Customer behavioral analytics
- Churn prediction & activation campaigns

## 🔧 Components

- **S3**: Event storage and feature outputs
- **Glue (PySpark)**: ETL jobs for ingestion, identity resolution, behavior analysis, and scoring
- **Step Functions (optional)**: Workflow orchestration
- **CloudWatch**: Logging & monitoring
- **IAM + Glue Catalog**: Metadata and access control

## 📁 Pipeline Steps

| Step | Description |
|------|-------------|
| 1    | Ingest raw event logs (CSV/JSON from multiple sources) |
| 2    | Resolve identities using heuristics & UUID mapping |
| 3    | Aggregate behavioral features per customer |
| 4    | Rule-based churn risk scoring |
| 5    | Export campaign CSVs for high-value/at-risk customers |


## 🚀 Quickstart

```bash
# Run locally in Spark (if testing)
spark-submit glue_jobs/04_churn_scoring.py


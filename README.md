# Data Engineering Zoomcamp 2026 — My Journey

My coursework and final project for the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) by [DataTalks.Club](https://datatalks.club/) (Cohort 2026).

## About the Course

The Data Engineering Zoomcamp is a free, 9-week intensive course covering the fundamentals of data engineering — from containerization and infrastructure-as-code to batch and stream processing. It is taught by [Alexey Grigorev](https://linkedin.com/in/agrigorev) and the DataTalks.Club team.

## Technologies & Tools

| Area | Tools |
|------|-------|
| Containerization | Docker, Docker Compose |
| Infrastructure as Code | Terraform, GCP |
| Workflow Orchestration | Kestra |
| Data Ingestion | dlt (data load tool) |
| Data Warehouse | BigQuery, DuckDB |
| Analytics Engineering | dbt |
| Data Platforms | Bruin |
| Batch Processing | Apache Spark |
| Stream Processing | Apache Kafka, Apache Flink |
| Dashboard | Streamlit, Plotly |
| Language | Python, SQL |

## Homework

All homework solutions for each module are in the [`cohorts/2026/`](cohorts/2026/) directory:

| Module | Topic | Homework |
|--------|-------|----------|
| 1 | Docker & Terraform | 
| 2 | Workflow Orchestration (Kestra) |
| Workshop 1 | Data Ingestion with dlt |
| 3 | Data Warehouse (BigQuery) |
| 4 | Analytics Engineering (dbt) |
| 5 | Data Platforms (Bruin) |
| 6 | Batch Processing (Spark) |
| 7 | Stream Processing (Kafka & Flink) |

## Final Project — Skin Lesion Analytics Dashboard

An end-to-end data engineering pipeline that processes the **HAM10000** dermatoscopy dataset (10,015 skin lesion images across 7 diagnostic categories) and presents insights through an interactive Streamlit dashboard.

**[Full Project README →](skin-lesion-analytics/README.md)** ·

### Pipeline Architecture

```
Kaggle API ──► Raw CSV ──► Parquet (Data Lake) ──► DuckDB (Warehouse) ──► dbt ──► Streamlit
```

### Key Features

- **Ingestion** — Downloads HAM10000 metadata from Kaggle, cleans the data, and writes Parquet files
- **Warehouse** — Loads data into DuckDB with optimized/clustered tables
- **Transformations** — dbt models for staging and marts (diagnosis summary, demographics, body location analysis)
- **Dashboard** — Interactive Streamlit app with Plotly charts (diagnosis distribution, age groups, body location breakdown)
- **Orchestration** — Python DAG runner + Makefile (`make all` runs the full pipeline)

### Quick Start

```bash
cd skin-lesion-analytics
make setup    # Install dependencies
make all      # Run full pipeline (ingest → warehouse → transform → test)
make dashboard # Launch Streamlit dashboard
```

## Repository Structure

```
.
├── Home/                # Homework solutions for each module
│   ├── 01-docker-terraform/
│   ├── 02-workflow-orchestration/
│   ├── 03-data-warehouse/
│   ├── 04-analytics-engineering/
│   ├── 05-data-platforms/
│   ├── 06-batch/
│   ├── 07-streaming/
│   └── workshops/dlt/
├── skin-lesion-analytics/       # Final project
│   ├── pipeline/                #   Ingestion & warehouse loading
│   ├── dbt_skin_lesion/         #   dbt models & tests
│   ├── dashboard/               #   Streamlit app
│   └── Makefile                 #   Pipeline orchestration
├── 01-docker-terraform/         # Course material (modules 1-7)
├── 02-workflow-orchestration/
├── ...
└── README.md                 # ← You are here
```

## Acknowledgements

Thanks to [Alexey Grigorev](https://linkedin.com/in/agrigorev) and the entire [DataTalks.Club](https://datatalks.club/) team for offering this incredible course for free. Special thanks to all the instructors and the community on Slack for their support throughout the cohort.

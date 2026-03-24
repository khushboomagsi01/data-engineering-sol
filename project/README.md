# Skin Lesion Analytics Dashboard

An end-to-end data engineering pipeline that processes the **HAM10000** dermatoscopy dataset
(10,015 skin lesion images across 7 diagnostic categories) and presents insights through an
interactive Streamlit dashboard.

## Problem Statement

Skin cancer is the most common cancer worldwide. Early detection is critical for patient outcomes.
This project builds a batch data pipeline to ingest, transform, and visualize metadata from the
HAM10000 dataset — a large collection of multi-source dermatoscopic images of pigmented lesions.

The dashboard answers questions like:
- What is the distribution of diagnosis types across the dataset?
- How do skin lesion diagnoses vary across patient age groups?
- Which body locations are most affected by each type of lesion?

## Architecture

```
Kaggle API ──► Raw CSV ──► Parquet (Data Lake) ──► DuckDB (Warehouse) ──► dbt ──► Streamlit
```

| Component              | Technology               |
|------------------------|---------------------------|
| Data Source            | Kaggle API               |
| Data Lake Format       | Apache Parquet           |
| Data Warehouse         | DuckDB (clustered table) |
| Transformations        | dbt-duckdb               |
| Dashboard              | Streamlit + Plotly       |
| Orchestration          | Python DAG runner + Make |
| Language               | Python / SQL             |

## Dataset

**HAM10000** ("Human Against Machine with 10000 training images")
- Source: [Kaggle — Skin Cancer MNIST: HAM10000](https://www.kaggle.com/datasets/kmader/skin-cancer-mnist-ham10000)
- 10,015 dermatoscopic images with metadata
- 7 diagnostic categories: Melanocytic Nevi, Melanoma, Benign Keratosis, Basal Cell Carcinoma, Actinic Keratoses, Vascular Lesions, Dermatofibroma
- Metadata fields: `lesion_id`, `image_id`, `dx`, `dx_type`, `age`, `sex`, `localization`

## Prerequisites

- Python 3.10+
- [Kaggle API credentials](https://www.kaggle.com/docs/api#authentication) (place `kaggle.json` in `~/.kaggle/`)

## Quick Start

```bash
# 1. Install dependencies
make setup

# 2. Run the full DAG pipeline (ingest → warehouse → dbt_transform → dbt_test)
make all

# 3. Launch the dashboard
make dashboard
```

## Step-by-Step

```bash
# Install Python dependencies
make setup

# Download dataset from Kaggle and convert to parquet
make ingest

# Load parquet into DuckDB warehouse
make warehouse

# Run dbt transformations
make transform

# Launch Streamlit dashboard (opens browser)
make dashboard
```

## Project Structure

```
skin-lesion-analytics/
├── Makefile                    # Pipeline orchestration
├── README.md                   # This file
├── SOLUTION.md                 # Detailed solution walkthrough
├── requirements.txt            # Python dependencies
├── .gitignore
├── pipeline/
│   ├── orchestrate.py          # DAG runner (ingest → warehouse → dbt → test)
│   ├── ingest.py               # Download from Kaggle → parquet
│   └── load_warehouse.py       # Load parquet → DuckDB (clustered)
├── dbt_skin_lesion/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       │   ├── stg_lesions.sql
│       │   └── schema.yml
│       └── marts/
│           ├── diagnosis_summary.sql
│           ├── demographics_analysis.sql
│           ├── body_location_analysis.sql
│           └── schema.yml
├── dashboard/
│   └── app.py                  # Streamlit dashboard
├── data/                       # Raw & lake data (gitignored)
└── warehouse/                  # DuckDB database (gitignored)
```

## Dashboard Preview

The dashboard includes:
1. **Diagnosis Distribution** — bar chart of lesion types (categorical)
2. **Cases by Age Group** — line chart across age bands (temporal/numerical)
3. **Body Location Breakdown** — pie chart of affected areas
4. **Key Metrics** — total images, unique lesions, diagnosis types

## Cleaning Up

```bash
make clean
```

.PHONY: all setup ingest warehouse transform test pipeline dashboard clean

all: setup pipeline

setup:
	pip install -r requirements.txt

# Full DAG-based pipeline: ingest → warehouse → dbt_transform → dbt_test
pipeline:
	python pipeline/orchestrate.py

# Individual steps (can also be run standalone)
ingest:
	python pipeline/ingest.py

warehouse:
	python pipeline/load_warehouse.py

transform:
	cd dbt_skin_lesion && dbt run --profiles-dir .

test:
	cd dbt_skin_lesion && dbt test --profiles-dir .

dashboard:
	streamlit run dashboard/app.py

clean:
	rm -rf data/raw data/lake
	rm -f warehouse/skin_lesions.duckdb warehouse/skin_lesions.duckdb.wal
	cd dbt_skin_lesion && dbt clean

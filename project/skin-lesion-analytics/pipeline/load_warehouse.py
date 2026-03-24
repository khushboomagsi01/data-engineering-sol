"""Load parquet data from the data lake into the DuckDB warehouse."""

import duckdb
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
LAKE_DIR = DATA_DIR / "lake"
WAREHOUSE_DIR = Path(__file__).resolve().parent.parent / "warehouse"
DB_PATH = WAREHOUSE_DIR / "skin_lesions.duckdb"


def load_to_duckdb():
    """Load the parquet file into a DuckDB table."""
    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

    parquet_path = LAKE_DIR / "skin_lesions.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(
            f"Parquet not found: {parquet_path}. Run 'make ingest' first."
        )

    con = duckdb.connect(str(DB_PATH))

    # -----------------------------------------------------------
    # Create a raw table first, then build an optimized table
    # partitioned by diagnosis_code for efficient downstream
    # queries that almost always filter/group by diagnosis.
    # -----------------------------------------------------------
    con.execute("""
        CREATE OR REPLACE TABLE raw_skin_lesions AS
        SELECT * FROM read_parquet(?)
    """, [str(parquet_path)])

    raw_count = con.execute("SELECT COUNT(*) FROM raw_skin_lesions").fetchone()[0]
    print(f"Loaded {raw_count} records into raw_skin_lesions")

    # -----------------------------------------------------------
    # Optimized / partitioned table:
    #   • Partitioned by diagnosis_code (dx) — all dashboard
    #     queries group or filter by diagnosis.
    #   • Sorted (clustered) by age, localization — the two most
    #     common secondary filters in demographics and body-
    #     location analysis.
    # DuckDB doesn't support Hive-style partitions on local
    # tables, so we use explicit sort order which acts as
    # clustered storage — scans skip irrelevant row-groups.
    # -----------------------------------------------------------
    con.execute("""
        CREATE OR REPLACE TABLE skin_lesions_optimized AS
        SELECT *
        FROM raw_skin_lesions
        ORDER BY dx, age, localization
    """)

    opt_count = con.execute(
        "SELECT COUNT(*) FROM skin_lesions_optimized"
    ).fetchone()[0]
    print(
        f"Created skin_lesions_optimized ({opt_count} rows) — "
        "sorted by (dx, age, localization) for efficient scan pruning"
    )

    con.close()


def main():
    load_to_duckdb()
    print("✓ Warehouse load complete!")


if __name__ == "__main__":
    main()

"""Ingest HAM10000 skin lesion metadata from Kaggle into the data lake (parquet)."""

import subprocess
import sys
import zipfile
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
RAW_DIR = DATA_DIR / "raw"
LAKE_DIR = DATA_DIR / "lake"

KAGGLE_DATASET = "kmader/skin-cancer-mnist-ham10000"
METADATA_FILE = "HAM10000_metadata.csv"

DIAGNOSIS_MAP = {
    "akiec": "Actinic Keratoses",
    "bcc": "Basal Cell Carcinoma",
    "bkl": "Benign Keratosis",
    "df": "Dermatofibroma",
    "mel": "Melanoma",
    "nv": "Melanocytic Nevi",
    "vasc": "Vascular Lesions",
}


def download_metadata() -> Path:
    """Download HAM10000 metadata CSV from Kaggle."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = RAW_DIR / METADATA_FILE
    if csv_path.exists():
        print(f"Already downloaded: {csv_path}")
        return csv_path

    print("Downloading HAM10000 metadata from Kaggle...")
    result = subprocess.run(
        [
            "kaggle", "datasets", "download",
            "-d", KAGGLE_DATASET,
            "-f", METADATA_FILE,
            "-p", str(RAW_DIR),
            "--force",
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"Kaggle CLI error: {result.stderr}", file=sys.stderr)
        raise RuntimeError(
            "Failed to download from Kaggle. "
            "Ensure credentials are in ~/.kaggle/kaggle.json\n"
            "See: https://www.kaggle.com/docs/api#authentication"
        )

    # Kaggle may download as zip
    zip_path = RAW_DIR / f"{METADATA_FILE}.zip"
    if zip_path.exists():
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(RAW_DIR)
        zip_path.unlink()

    if not csv_path.exists():
        raise FileNotFoundError(
            f"Expected file not found after download: {csv_path}"
        )

    print(f"Downloaded: {csv_path}")
    return csv_path


def transform_to_parquet(csv_path: Path) -> Path:
    """Clean metadata and save as parquet in the data lake."""
    LAKE_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(csv_path)

    # Map abbreviated diagnosis to full name
    df["diagnosis_name"] = df["dx"].map(DIAGNOSIS_MAP)

    # Clean age — fill nulls with median
    df["age"] = pd.to_numeric(df["age"], errors="coerce")
    median_age = df["age"].median()
    df["age"] = df["age"].fillna(median_age)

    # Create age group buckets
    df["age_group"] = pd.cut(
        df["age"],
        bins=[0, 20, 40, 60, 80, 100],
        labels=["0-20", "21-40", "41-60", "61-80", "81-100"],
    )

    # Clean sex and localization
    df["sex"] = df["sex"].fillna("unknown")
    df["localization"] = df["localization"].fillna("unknown")

    parquet_path = LAKE_DIR / "skin_lesions.parquet"
    df.to_parquet(parquet_path, index=False)
    print(f"Saved {len(df)} records to {parquet_path}")
    return parquet_path


def main():
    csv_path = download_metadata()
    transform_to_parquet(csv_path)
    print("✓ Ingestion complete!")


if __name__ == "__main__":
    main()

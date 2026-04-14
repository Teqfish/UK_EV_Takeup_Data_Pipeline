import os
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from google.cloud import storage


#### === GCS HELPERS === ####

def download_file(url: str, destination_path: Path) -> None:
    """Download a file from the web to a local path."""
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

    with requests.get(url, headers=headers, stream=True, timeout=60) as response:
        response.raise_for_status()
        with open(destination_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)

    print(f"Downloaded file locally: {destination_path}")


def upload_file_to_gcs(bucket_name: str, source_file_path: Path, destination_blob_name: str) -> None:
    """Upload a local file to a GCS blob path."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(str(source_file_path))
    print(f"Uploaded to GCS: gs://{bucket_name}/{destination_blob_name}")


def remove_local_file(file_path: Path) -> None:
    """Delete a local file if it exists."""
    if file_path.exists():
        file_path.unlink()
        print(f"Deleted local file: {file_path}")
    else:
        print(f"Local file not found, skipping delete: {file_path}")


#### === MAIN INGESTION FLOW === ####

def main() -> None:
    # Load environment variables from .env
    load_dotenv()

    # Read config from environment
    source_url = os.environ["DESNZ_PETROLEUM_PRODUCTS_PRICES_URL"]
    bucket_name = os.environ["GCS_BUCKET"]
    local_raw_dir = Path(os.environ["LOCAL_RAW_DIR"])
    local_prepared_dir = Path(os.environ["LOCAL_PREPARED_DIR"])

    # Create local temp folders if needed
    local_raw_dir.mkdir(parents=True, exist_ok=True)
    local_prepared_dir.mkdir(parents=True, exist_ok=True)

    # Define file names and local paths
    raw_filename = "desnz_petroleum_products_prices.xlsx"
    prepared_filename = "prepared_desnz_petroleum_products_prices.parquet"

    raw_local_path = local_raw_dir / raw_filename
    prepared_local_path = local_prepared_dir / prepared_filename

    #### === DOWNLOAD RAW FILE === ####

    # Download source Excel file from the web
    print("Downloading petroleum prices source file...")
    download_file(source_url, raw_local_path)

    #### === UPLOAD RAW FILE TO GCS === ####

    # Upload raw Excel file to raw/ zone in GCS
    upload_file_to_gcs(
        bucket_name=bucket_name,
        source_file_path=raw_local_path,
        destination_blob_name=f"raw/desnz_petroleum_products_prices/{raw_filename}",
    )

    #### === PREPARE FILE FOR BIGQUERY === ####

    # Read the required sheet and header row from the workbook
    df_raw = pd.read_excel(
        raw_local_path,
        sheet_name="4.1.1 (Quarterly)",
        header=9,
    ).copy()

    # Clean key column names we know we need later
    renaming_map = {
        "Year": "year",
        "Quarter": "quarter",
        "Motor spirit: Premium unleaded / ULSP\n(Pence per litre)\n[Note 1, 2]": "premium_unleaded",
        "Derv: Diesel / ULSD\n(Pence per litre)\n[Note 1, 2]": "diesel",
        "Crude oil acquired by refineries \n2010 = 100\n[Note 4] [r]": "crude_oil_index",
    }

    df_prepared = df_raw.rename(columns=renaming_map).copy()

    # Standardise all remaining column names enough for BigQuery
    df_prepared.columns = (
        df_prepared.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"[\n\r]+", " ", regex=True)
        .str.replace(r"[^a-z0-9]+", "_", regex=True)
        .str.replace(r"_+", "_", regex=True)
        .str.strip("_")
    )

    # Save prepared parquet locally
    df_prepared.to_parquet(prepared_local_path, index=False)
    print(f"Saved prepared parquet locally: {prepared_local_path}")

    #### === UPLOAD PREPARED FILE TO GCS === ####

    # Upload prepared parquet to prepared/ zone in GCS
    upload_file_to_gcs(
        bucket_name=bucket_name,
        source_file_path=prepared_local_path,
        destination_blob_name=f"prepared/desnz_petroleum_products_prices/{prepared_filename}",
    )

    #### === CLEAN UP LOCAL TEMP FILES === ####

    # Delete local temp files after successful upload
    remove_local_file(raw_local_path)
    remove_local_file(prepared_local_path)

    print("Petroleum prices ingestion complete.")


if __name__ == "__main__":
    main()

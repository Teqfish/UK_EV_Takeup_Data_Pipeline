import os
from pathlib import Path

import requests
import pandas as pd
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
    source_url = os.environ["BANK_OF_ENGLAND_EUR_GBP_FX_URL"]
    bucket_name = os.environ["GCS_BUCKET"]
    local_raw_dir = Path(os.environ["LOCAL_RAW_DIR"])
    local_prepared_dir = Path(os.environ["LOCAL_PREPARED_DIR"])

    # Create local temp folders if needed
    local_raw_dir.mkdir(parents=True, exist_ok=True)
    local_prepared_dir.mkdir(parents=True, exist_ok=True)

    # Define file names and local paths
    html_filename = "bank_of_england_eur_gbp_fx.html"
    raw_filename = "bank_of_england_eur_gbp_fx.csv"
    prepared_filename = "prepared_bank_of_england_eur_gbp_fx.parquet"

    html_local_path = local_raw_dir / html_filename
    raw_local_path = local_raw_dir / raw_filename
    prepared_local_path = local_prepared_dir / prepared_filename

    #### === DOWNLOAD SOURCE PAGE === ####

    # Download source HTML page from the web
    print("Downloading FX source page...")
    download_file(source_url, html_local_path)

    #### === PREPARE FILE FOR BIGQUERY === ####

    # Read the HTML table from the downloaded page
    df_raw = pd.read_html(html_local_path)[0].copy()

    # Clean column names
    df_raw.columns = ["date", "gbp_eur_rate"]

    # Save extracted table as the true raw CSV
    df_raw.to_csv(raw_local_path, index=False)
    print(f"Saved raw CSV locally: {raw_local_path}")

    #### === UPLOAD RAW FILE TO GCS === ####

    # Upload extracted raw CSV to raw/ zone in GCS
    upload_file_to_gcs(
        bucket_name=bucket_name,
        source_file_path=raw_local_path,
        destination_blob_name=f"raw/bank_of_england_eur_gbp_fx/{raw_filename}",
    )

    # Start from the extracted table
    df_prepared = df_raw.copy()

    # Cast types
    df_prepared["date"] = pd.to_datetime(
        df_prepared["date"],
        format="%d %b %y",
        errors="coerce",
    )
    df_prepared["gbp_eur_rate"] = pd.to_numeric(
        df_prepared["gbp_eur_rate"],
        errors="coerce",
    )

    # Keep only required columns and drop bad rows
    df_prepared = (
        df_prepared[["date", "gbp_eur_rate"]]
        .dropna(subset=["date", "gbp_eur_rate"])
        .sort_values("date")
        .reset_index(drop=True)
    )

    # Save prepared parquet locally
    df_prepared.to_parquet(prepared_local_path, index=False)
    print(f"Saved prepared parquet locally: {prepared_local_path}")

    #### === UPLOAD PREPARED FILE TO GCS === ####

    # Upload prepared parquet to prepared/ zone in GCS
    upload_file_to_gcs(
        bucket_name=bucket_name,
        source_file_path=prepared_local_path,
        destination_blob_name=f"prepared/bank_of_england_eur_gbp_fx/{prepared_filename}",
    )

    #### === CLEAN UP LOCAL TEMP FILES === ####

    # Delete local temp files after successful upload
    remove_local_file(html_local_path)
    remove_local_file(raw_local_path)
    remove_local_file(prepared_local_path)

    print("FX ingestion complete.")


if __name__ == "__main__":
    main()

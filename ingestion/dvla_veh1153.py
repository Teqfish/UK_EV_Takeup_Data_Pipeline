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


def upload_file_to_gcs(
    project_id: str,
    bucket_name: str,
    source_file_path: Path,
    destination_blob_name: str,
) -> None:
    """Upload a local file to a GCS blob path."""
    storage_client = storage.Client(project=project_id)
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
    source_url = os.environ["DVLA_VEH1153_URL"]
    project_id = os.environ["GCP_PROJECT_ID"]
    bucket_name = os.environ["GCS_BUCKET"]
    local_raw_dir = Path(os.environ["LOCAL_RAW_DIR"])
    local_prepared_dir = Path(os.environ["LOCAL_PREPARED_DIR"])

    # Create local temp folders if needed
    local_raw_dir.mkdir(parents=True, exist_ok=True)
    local_prepared_dir.mkdir(parents=True, exist_ok=True)

    # Define file names and local paths
    raw_filename = "dvla_veh1153.ods"
    prepared_filename = "prepared_dvla_veh1153.parquet"

    raw_local_path = local_raw_dir / raw_filename
    prepared_local_path = local_prepared_dir / prepared_filename

    #### === DOWNLOAD RAW FILE === ####

    # Download source ODS file from the web
    print("Downloading DVLA VEH1153 source file...")
    download_file(source_url, raw_local_path)

    #### === UPLOAD RAW FILE TO GCS === ####

    # Upload raw ODS file to raw/ zone in GCS
    upload_file_to_gcs(
        project_id=project_id,
        bucket_name=bucket_name,
        source_file_path=raw_local_path,
        destination_blob_name=f"raw/dvla_veh1153/{raw_filename}",
    )

    #### === PREPARE FILE FOR BIGQUERY === ####

    # Read the required sheet and header row from the workbook
    df_raw = pd.read_excel(
        raw_local_path,
        sheet_name="VEH1153a_RoadUsing",
        header=4,
    ).copy()

    # Force text-like columns to string so parquet conversion is stable
    text_cols = [
        "Geography",
        "Date Interval",
        "Date",
        "Units",
        "Body Type",
        "Keepership",
    ]

    for col in text_cols:
        if col in df_raw.columns:
            df_raw[col] = df_raw[col].astype("string")

    # Save prepared parquet locally
    df_raw.to_parquet(prepared_local_path, index=False)
    print(f"Saved prepared parquet locally: {prepared_local_path}")

    #### === UPLOAD PREPARED FILE TO GCS === ####

    # Upload prepared parquet to prepared/ zone in GCS
    upload_file_to_gcs(
        project_id=project_id,
        bucket_name=bucket_name,
        source_file_path=prepared_local_path,
        destination_blob_name=f"prepared/dvla_veh1153/{prepared_filename}",
    )

    #### === CLEAN UP LOCAL TEMP FILES === ####

    # Delete local temp files after successful upload
    remove_local_file(raw_local_path)
    remove_local_file(prepared_local_path)

    print("DVLA VEH1153 ingestion complete.")


if __name__ == "__main__":
    main()

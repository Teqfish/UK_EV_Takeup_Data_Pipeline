import os

from dotenv import load_dotenv
from google.cloud import bigquery


#### === BIGQUERY LOAD === ####

def main() -> None:
    # Load environment variables from .env
    load_dotenv()

    # Read config from environment
    project_id = os.environ["GCP_PROJECT_ID"]
    bucket_name = os.environ["GCS_BUCKET"]
    raw_dataset = os.environ["BQ_RAW_DATASET"]

    # Build source URI and target table
    source_uri = (
        f"gs://{bucket_name}/prepared/dvla_veh1153/"
        f"prepared_dvla_veh1153.parquet"
    )
    table_id = f"{project_id}.{raw_dataset}.raw_dvla_veh1153"

    # Create BigQuery client
    client = bigquery.Client(project=project_id)

    # Configure load job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    print(f"Loading {source_uri} into {table_id}...")

    # Start load job
    load_job = client.load_table_from_uri(
        source_uri,
        table_id,
        job_config=job_config,
    )

    # Wait for completion
    load_job.result()

    print(f"Loaded table: {table_id}")


if __name__ == "__main__":
    main()

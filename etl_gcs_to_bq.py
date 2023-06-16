from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block  = GcsBucket.load('de-gcs')
    gcs_block.get_directory(from_path=gcs_path, local_path=f".")
    return Path(f"./{gcs_path}")

@task()
def transform(path: Path) -> pd.DataFrame:
    """Data Cleaning example"""
    df = pd.read_parquet(path)
    print(f"Pre: Missing passenger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    print(f"Post: Missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task()
def write_to_bq(df: pd.DataFrame) -> None:
    """Write the dataframe to Bigquery"""
    gcp_credentials_block = GcpCredentials.load("de-gcp-creds")

    df.to_gbq(
        destination_table="dedataset.rides",
        project_id="prefect-de",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

@flow()
def etl_gcs_to_bq():
    """Main ETL Flow to load data into bigquery data warehosue"""
    color = 'yellow'
    year = 2021
    month = 1

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    
    write_to_bq(df)

if __name__ == '__main__':
    etl_gcs_to_bq()
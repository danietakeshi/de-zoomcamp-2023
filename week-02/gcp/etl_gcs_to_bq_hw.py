from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(log_prints=True)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
	"""Download trip data from GCS"""
	gcs_path = f"{color}/{color}_tripdata_{year}-{month:02}.parquet"
	gcs_block = GcsBucket.load("zoom-gcs")
	gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
	return Path(f"data/{gcs_path}")

@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
	"""Data cleaning example"""
	df = pd.read_parquet(path)
	#print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
	#df['passenger_count'].fillna(0, inplace=True)
	#print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
	try:
		df.drop('airport_fee', axis=1, inplace=True)
	except:
		print(f"no column airport_fee to drop")
	
	return df

@task(log_prints=True)
def write_bq(df: pd.DataFrame) -> None:
	"""Write DataFrame to BigQuery"""

	gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

	df.to_gbq(
		destination_table="dezoomcamp.trips_data_2019",
		project_id="coherent-bliss-275820",
		credentials= gcp_credentials_block.get_credentials_from_service_account(),
		chunksize=500_000,
		if_exists="append",
	)

@flow(log_prints=True)
def etl_gcs_to_bq(year: int, month: int, color: str):
	"""Main ETL flow to load data into Big Query"""
	
	path = extract_from_gcs(color, year, month)
	df = transform(path)
	print(f"row count: {df.shape[0]}")
	write_bq(df)

@flow()
def etl_parent_flow(
	months: list[int] = [2, 3], year: int = 2019, color: str = "yellow"
):
	for month in months:
		etl_gcs_to_bq(year, month, color)

if __name__ == "__main__":
	etl_parent_flow()

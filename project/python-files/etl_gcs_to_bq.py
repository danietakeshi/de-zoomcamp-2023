from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import os

read_dir = './parquet_files'
save_dir = './parquet_files'
bq_dataset = 'dezoomcamp_project'
bq_project_id = 'coherent-bliss-275820'

@task()
def get_fixture_id(league_name: str) -> list:
    """Get the fixture_id for the league_name"""
    fixture_id = []
    for file in os.listdir(read_dir):
        if 'fixtures' in file and int(file[9:13]) >= 2018:
            df = pd.read_parquet(f'{read_dir}/{file}')
            df = df[df["league.name"] == league_name]
            fixture_id.extend(df['fixture.id'].tolist())
    return [str(x) for x in fixture_id]

@task(log_prints=True)
def extract_from_gcs(fixture_date: str) -> Path:
	"""Download fixtures data from GCS"""
	gcs_path = f"fixtures_{fixture_date}.parquet"
	gcs_block = GcsBucket.load("gcs-bucket")
	gcs_block.get_directory(from_path=gcs_path, local_path=f"{save_dir}/")
	return Path(f"{save_dir}/{gcs_path}")

@task(log_prints=True)
def extract_stats_from_gcs(fixture_id: str) -> Path:
	"""Download players statistics data from GCS"""
	gcs_path = f"players_stats_{fixture_id}.parquet"
	gcs_block = GcsBucket.load("gcs-bucket")
	gcs_block.get_directory(from_path=gcs_path, local_path=f"{save_dir}/")
	return Path(f"{save_dir}/{gcs_path}")

@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
	"""Data transformation on fixture data"""
	df = pd.read_parquet(path)
	df['fixture.date'] = pd.to_datetime(df['fixture.date'], utc=False)
	df['teams.home.winner'] = df['teams.home.winner'].astype('bool')
	df['teams.away.winner'] = df['teams.away.winner'].astype('bool')
	for c in ('goals.home', 'goals.away', 'score.halftime.home', 'score.halftime.away', 'score.fulltime.home', 'score.fulltime.away', 'score.extratime.home', 'score.extratime.away', 'score.penalty.home','score.penalty.away'):
		df[c] = df[c].fillna(0)
		df[c] = df[c].astype('int')

	column_dict ={}
	for i in df.columns:
		df = df.rename(columns={i:i.replace('.', '_')})

	return df

@task(log_prints=True)
def transform_stats(path: Path) -> pd.DataFrame:
	"""Data transformation on Player Statistics"""
	df = pd.read_parquet(path)
	df['games_rating'] = df['games_rating'].astype('float64')
	for c in ('offsides', 'games_minutes', 'shots_total', 'shots_on', 'goals_total', 'goals_assists', 'goals_saves', 'passes_total', 'passes_key', 'passes_accuracy', 'tackles_total', 'tackles_blocks', 'tackles_interceptions', 'duels_total', 'duels_won', 'dribbles_attempts', 'dribbles_success', 'dribbles_past', 'fouls_drawn', 'fouls_committed', 'penalty_won', 'penalty_commited', 'penalty_saved'):
		df[c] = df[c].fillna(0)
		df[c] = df[c].astype('int')

	column_dict ={}
	for i in df.columns:
		df = df.rename(columns={i:i.replace('.', '_')})

	return df

@task(log_prints=True)
def write_bq(df: pd.DataFrame, table_name: str) -> None:
	"""Write Fixture DataFrame to BigQuery"""

	gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

	df.to_gbq(
		destination_table=f"{bq_dataset}.{table_name}",
		project_id=bq_project_id,
		credentials= gcp_credentials_block.get_credentials_from_service_account(),
		chunksize=1_000,
		if_exists="append",
	)

@flow(log_prints=True)
def etl_gcs_to_bq(fixture_date: str):
	"""Main ETL flow to load fixture data into Big Query"""
	
	path = extract_from_gcs(fixture_date)
	df = transform(path)
	print(f"row count: {df.shape[0]}")
	write_bq(df, 'ods_football_fixtures')

@flow(log_prints=True)
def etl_gcs_to_bq_stats(fixture_id: str):
	"""Main ETL flow to load player statistics data into Big Query"""
	
	path = extract_stats_from_gcs(fixture_id)
	df = transform_stats(path)
	print(f"row count: {df.shape[0]}")
	write_bq(df, 'ods_football_stats')

@flow()
def etl_insert_fixtures():
	"""Main Deployment for Fixture Data"""
	df = pd.read_parquet('world_cup_dates.parquet')
	for date in df.values.tolist():
		etl_gcs_to_bq(date[0])


@flow()
def etl_insert_stats(league_name: str = 'World Cup'):
	"""Main Deployment for Player Statistics Data"""
	#fixture_list = get_fixture_id(league_name)
	df = pd.read_parquet('fixture_list.parquet')
	for fixture_id in df.values.tolist():
		etl_gcs_to_bq_stats(fixture_id[0])

if __name__ == "__main__":
	#etl_insert_fixtures()
	print(os.getcwd())
	etl_insert_stats('World Cup')
	
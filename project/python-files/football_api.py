from pathlib import Path
import requests
import pandas as pd
from prefect import flow, task
from prefect_gcp import GcsBucket
from prefect.blocks.system import Secret
from prefect.tasks import task_input_hash
from datetime import timedelta
import os

secret_block = Secret.load('football-api-key')

headers = {
    "X-RapidAPI-Key": f"{secret_block.get()}",
    "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
}

save_dir = './parquet_files'

@task(retries=1, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def get_fixtures(date: str):
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures" 
    querystring = {"date":f"{date}"}
    fixture_date = date.replace('-', '')

    response = requests.request("GET", url, headers=headers, params=querystring)
    resp = response.json()['response']
    df = pd.json_normalize(resp)
    df.to_parquet(f'{save_dir}/fixtures_{fixture_date}.parquet')
    print(f'file fixtures_{fixture_date}.parquet saved on folder!')

    fixture_id = []
    df = df[df["league.name"] == 'World Cup']
    fixture_id.extend(df['fixture.id'].tolist())

    return Path(f'{save_dir}/fixtures_{fixture_date}.parquet'), f'fixtures_{fixture_date}.parquet', [str(x) for x in fixture_id]

@task()
def get_fixture_id(league_name: str) -> list:
    fixture_id = []
    for file in os.listdir(save_dir):
        if 'fixtures' in file and int(file[9:13]) >= 2018:
            df = pd.read_parquet(f'{save_dir}/{file}')
            df = df[df["league.name"] == 'World Cup']
            fixture_id.extend(df['fixture.id'].tolist())
    return [str(x) for x in fixture_id]

@task(retries=1, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def get_players_stats(fixture_id: str):
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures/players"
    querystring = {"fixture":f"{fixture_id}"}

    response = requests.request("GET", url, headers=headers, params=querystring)

    full_table = []

    if len(response.json()['response']) > 0:
        for team in response.json()['response']:
            team_info = pd.json_normalize(team)[['team.id', 'team.name', 'team.logo', 'players']]
            for player in team_info['players'][0]:
                player_info = pd.json_normalize(player)
                for stat in player_info['statistics'][0]:
                    stat_info = pd.json_normalize(stat)
                    full_table.append([int(fixture_id)] + team_info[['team.id', 'team.name', 'team.logo']].values[0].tolist() + player_info[['player.id', 'player.name', 'player.photo']].values[0].tolist() + stat_info.values[0].tolist())
        
        columns = ['fixture_id'] + team_info[['team.id', 'team.name', 'team.logo']].columns.tolist() + player_info[['player.id', 'player.name', 'player.photo']].columns.tolist() + stat_info.columns.tolist()
        columns = [s.replace('.', '_') for s in columns]

        df = pd.DataFrame(full_table, columns=columns)
        df.to_parquet(f'{save_dir}/players_stats_{fixture_id}.parquet')
        print(f'file players_stats_{fixture_id}.parquet saved on folder!')

    return Path(f'{save_dir}/players_stats_{fixture_id}.parquet'), f'players_stats_{fixture_id}.parquet'

@task()
def write_gcs(path: Path, file: str) -> None:
    """Upload local parquet file to gcs"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("gcs-bucket")
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path=f"{path}",
        to_path=f"{file}"
    )
    return 

@flow()
def api_to_gcs_fixtures(dates: list = ['2022-12-18', '2022-12-17', '2022-12-14']) -> None:
    for date in dates:
        path, file, fixture_list = get_fixtures(date)
        write_gcs(path, file)
        
        for fixture_id in fixture_list:
            path, file = get_players_stats(fixture_id)
            write_gcs(path, file)

@flow()
def api_to_gcs_player_stats(league_name: str = 'World Cup') -> None:
    fixture_list = get_fixture_id(league_name)
    for file in os.listdir(save_dir):
        if 'players_stats' in file:
            fixture_list.remove(file[14:20])

    for fixture_id in fixture_list:
        path, file = get_players_stats(fixture_id)
        write_gcs(path, file)

@flow()
def api_to_gcs_player_stats_file(league_name: str, file: str) -> None:
    fixture_list = get_fixture_id(league_name, file)

    for fixture_id in fixture_list:
        path, file = get_players_stats(fixture_id)
        write_gcs(path, file)

if __name__ == '__main__':
    dates = [
    '1994-06-27',
    '1994-06-26',
    '1994-06-25',
    ]
    #api_to_gcs_fixtures(dates)
    #get_players_stats('979139')
    api_to_gcs_fixtures()
    #api_to_gcs_player_stats('World Cup')
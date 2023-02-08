## Week 3 Homework
<b><u>Important Note:</b></u> <p>You can load the data however you would like, but keep the files in .GZ Format. 
If you are using orchestration such as Airflow or Prefect do not load the data into Big Query using the orchestrator.</br> 
Stop with loading the files into a bucket. </br></br>
<u>NOTE:</u> You can use the CSV option for the GZ files when creating an External Table</br>

<b>SETUP:</b></br>
Create an external table using the fhv 2019 data. (`coherent-bliss-275820.dezoomcamp.external_fhv_tripdata`)</br>
Create a table in BQ using the fhv 2019 data (do not partition or cluster this table). (`coherent-bliss-275820.dezoomcamp.fhv_tripdata`)</br>
Data can be found here: https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv </p>

## Question 1:
What is the count for fhv vehicle records for year 2019?
- [ ] 65,623,481
- [x] 43,244,696
- [ ] 22,978,333
- [ ] 13,942,414

```sql
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `coherent-bliss-275820.dezoomcamp.external_fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://zoomcamp-bucket/fhv_tripdata_2019-*.csv.gz']
);


select count(*) from `coherent-bliss-275820.dezoomcamp.external_fhv_tripdata`;
```

## Question 2:
Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.</br> 
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- [ ] 25.2 MB for the External Table and 100.87MB for the BQ Table
- [ ] 225.82 MB for the External Table and 47.60MB for the BQ Table
- [ ] 0 MB for the External Table and 0MB for the BQ Table
- [x] 0 MB for the External Table and 317.94MB for the BQ Table 

```sql
select count(distinct Affiliated_base_number) from `coherent-bliss-275820.dezoomcamp.fhv_tripdata`; --# 317,94 MB

select count(distinct Affiliated_base_number) from `coherent-bliss-275820.dezoomcamp.external_fhv_tripdata`; --# 0 B
```

## Question 3:
How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
- [x] 717,748
- [ ] 1,215,687
- [ ] 5
- [ ] 20,332

```sql
select count(*) from `coherent-bliss-275820.dezoomcamp.fhv_tripdata`
where 1=1
  and PUlocationID is null
  and DOlocationID is null;
```

## Question 4:
What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?
- [ ] Cluster on pickup_datetime Cluster on affiliated_base_number
- [x] Partition by pickup_datetime Cluster on affiliated_base_number
- [ ] Partition by pickup_datetime Partition by affiliated_base_number
- [ ] Partition by affiliated_base_number Cluster on pickup_datetime

## Question 5:
Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 03/01/2019 and 03/31/2019 (inclusive).</br> 
Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.
- [ ] 12.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- [x] 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table
- [ ] 582.63 MB for non-partitioned table and 0 MB for the partitioned table
- [ ] 646.25 MB for non-partitioned table and 646.25 MB for the partitioned table

```sql
-- Impact of partition
-- Scanning 647.87MB of data
select count(distinct Affiliated_base_number)
from `coherent-bliss-275820.dezoomcamp.fhv_tripdata`
where date(pickup_datetime) between '2019-03-01' and '2019-03-31';

-- Scanning 23.05MB of data
select count(distinct Affiliated_base_number)
from `coherent-bliss-275820.dezoomcamp.fhv_tripdata_partitoned`
where date(pickup_datetime) between '2019-03-01' and '2019-03-31';
```

## Question 6: 
Where is the data stored in the External Table you created?

- [ ] Big Query
- [x] GCP Bucket
- [ ] Container Registry
- [ ] Big Table


## Question 7:
It is best practice in Big Query to always cluster your data:
- [ ] True
- [x] False


## (Not required) Question 8:
A better format to store these files may be parquet. Create a data pipeline to download the gzip files and convert them into parquet. Upload the files to your GCP Bucket and create an External and BQ Table. 

Pipeline:
```python
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp import GcsBucket
##from prefect.filesystems import GCS
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read data from web into pandas DataFrame"""

    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    df['dropOff_datetime'] = pd.to_datetime(df['dropOff_datetime'])
    #print(df.head(2))
    #print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")

    return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet dile"""
    path = Path(f"data/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task(log_prints=True)
def write_gcs(path: Path, dataset_file: str) -> None:
    """Upload local parquet file to gcs"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return 

@flow()
def etl_web_to_gcs(year: int, month: int) -> None:
    """The Main ETL function"""
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, dataset_file)
    write_gcs(path, dataset_file)

@flow()
def etl_parent_flow(
	months: list[int] = [3, 4], year: int = 2019
):
	for month in months:
		etl_web_to_gcs(year, month)

if __name__ == '__main__':
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    year = 2019
    etl_parent_flow(months, year)

```

Create External BQ Table
```sql
CREATE OR REPLACE EXTERNAL TABLE `coherent-bliss-275820.dezoomcamp.external_fhv_tripdata`
OPTIONS (
  format = 'Parquet',
  uris = ['gs://zoomcamp-bucket/fhv_tripdata_2019-*.parquet']
);


select count(*) from `coherent-bliss-275820.dezoomcamp.external_fhv_tripdata`;
```

Note: Column types for all files used in an External Table must have the same datatype. While an External Table may be created and shown in the side panel in Big Query, this will need to be validated by running a count query on the External Table to check if any errors occur. 
 
## Submitting the solutions

* Form for submitting: https://forms.gle/rLdvQW2igsAT73HTA
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 13 February (Monday), 22:00 CET


## Solution

We will publish the solution here
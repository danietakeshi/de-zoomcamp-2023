## Week 2 Homework

The goal of this homework is to familiarise users with workflow orchestration and observation. 


## Question 1. Load January 2020 data

Using the `etl_web_to_gcs.py` flow that loads taxi data into GCS as a guide, create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. Look at the logs to find out how many rows the dataset has.

How many rows does that dataset have?

- [x] 447,770
- [ ] 766,792
- [ ] 299,234
- [ ] 822,132

Using parametrized deployment and changing parameters to ```{ "color":"green", "months":[1] , "year":2020 }```

```
15:45:53.047 | INFO    | prefect.engine - Created flow run 'agate-mule' for flow 'etl-parent-flow'
15:45:53.378 | INFO    | Flow run 'agate-mule' - Created subflow run 'flying-raptor' for flow 'etl-web-to-gcs'
15:45:53.521 | INFO    | Flow run 'flying-raptor' - Created task run 'fetch-b4598a4a-0' for task 'fetch'
15:45:53.523 | INFO    | Flow run 'flying-raptor' - Executing 'fetch-b4598a4a-0' immediately...
15:45:53.561 | INFO    | Task run 'fetch-b4598a4a-0' - Finished in state Cached(type=COMPLETED)
15:45:54.560 | INFO    | Flow run 'flying-raptor' - Created task run 'clean-b9fd7e03-0' for task 'clean'
15:45:54.560 | INFO    | Flow run 'flying-raptor' - Executing 'clean-b9fd7e03-0' immediately...
15:45:55.211 | INFO    | Task run 'clean-b9fd7e03-0' - rows: 447770
15:45:55.349 | INFO    | Task run 'clean-b9fd7e03-0' - Finished in state Completed()
15:45:55.382 | INFO    | Flow run 'flying-raptor' - Created task run 'write_local-f322d1be-0' for task 'write_local'
15:45:55.386 | INFO    | Flow run 'flying-raptor' - Executing 'write_local-f322d1be-0' immediately...
15:45:56.528 | INFO    | Task run 'write_local-f322d1be-0' - Finished in state Completed()
15:45:56.560 | INFO    | Flow run 'flying-raptor' - Created task run 'write_gcs-1145c921-0' for task 'write_gcs'
15:45:56.561 | INFO    | Flow run 'flying-raptor' - Executing 'write_gcs-1145c921-0' immediately...
15:45:56.710 | INFO    | Task run 'write_gcs-1145c921-0' - Getting bucket 'zoomcamp-bucket'.
15:45:57.602 | INFO    | Task run 'write_gcs-1145c921-0' - Uploading from 'data\\green\\green_tripdata_2020-01.parquet' to the bucket 'zoomcamp-bucket' path 'data\\green\\green_tripdata_2020-01.parquet'.
15:46:01.699 | INFO    | Task run 'write_gcs-1145c921-0' - Finished in state Completed()
15:46:01.742 | INFO    | Flow run 'flying-raptor' - Finished in state Completed('All states completed.')
15:46:01.791 | INFO    | Flow run 'agate-mule' - Finished in state Completed('All states completed.')
```

## Question 2. Scheduling with Cron

Cron is a common scheduling specification for workflows. 

Using the flow in `etl_web_to_gcs.py`, create a deployment to run on the first of every month at 5am UTC. What???s the cron schedule for that?

- [x] `0 5 1 * *`
- [ ] `0 0 5 1 *`
- [ ] `5 * 1 0 *`
- [ ] `* * 5 1 0`

```???At 05:00 on day-of-month 1.???```
```
0 5 1 * *
??? ??? ??? ??? ????????????weekday
??? ??? ??? ????????????month
??? ??? ????????????day
??? ????????????hour
????????????minute
```

## Question 3. Loading data to BigQuery 

Using `etl_gcs_to_bq.py` as a starting point, modify the script for extracting data from GCS and loading it into BigQuery. This new script should not fill or remove rows with missing values. (The script is really just doing the E and L parts of ETL).

The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.

Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color. 

Make any other necessary changes to the code for it to function as required.

Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).

Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. Run your deployment to append this data to your BiqQuery table. How many rows did your flow code process?

- [x] 14,851,920
- [ ] 12,282,990
- [ ] 27,235,753
- [ ] 11,338,483

```
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
	#df['passenger_count'].fillna(0, inplace=True) --> remove filling null values
	#print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
	try:
		df.drop('airport_fee', axis=1, inplace=True) # --> drop column 'airport_fee'
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
	print(f"row count: {df.shape[0]}") # --> print number of rows processed
	write_bq(df)

@flow()
def etl_parent_flow(
	months: list[int] = [2, 3], year: int = 2019, color: str = "yellow"
):
	for month in months:
		etl_gcs_to_bq(year, month, color)

if __name__ == "__main__":
	etl_parent_flow()
```

```
21:57:24.833 | INFO    | prefect.engine - Created flow run 'solemn-muskrat' for flow 'etl-parent-flow'
21:57:25.193 | INFO    | Flow run 'solemn-muskrat' - Created subflow run 'rare-piculet' for flow 'etl-gcs-to-bq'
21:57:25.293 | INFO    | Flow run 'rare-piculet' - Created task run 'extract_from_gcs-968e3b65-0' for task 'extract_from_gcs'
21:57:25.293 | INFO    | Flow run 'rare-piculet' - Executing 'extract_from_gcs-968e3b65-0' immediately...
21:57:25.929 | INFO    | Task run 'extract_from_gcs-968e3b65-0' - Finished in state Completed()
21:57:25.963 | INFO    | Flow run 'rare-piculet' - Created task run 'transform-a7d916b4-0' for task 'transform'
21:57:25.964 | INFO    | Flow run 'rare-piculet' - Executing 'transform-a7d916b4-0' immediately...
21:57:27.003 | INFO    | Task run 'transform-a7d916b4-0' - no column airport_fee to drop
21:57:27.027 | INFO    | Task run 'transform-a7d916b4-0' - Finished in state Completed()
21:57:27.027 | INFO    | Flow run 'rare-piculet' - row count: 7019375
21:57:27.065 | INFO    | Flow run 'rare-piculet' - Created task run 'write_bq-b366772c-0' for task 'write_bq'
21:57:27.065 | INFO    | Flow run 'rare-piculet' - Executing 'write_bq-b366772c-0' immediately...
21:59:50.445 | INFO    | Task run 'write_bq-b366772c-0' - Finished in state Completed()
21:59:50.479 | INFO    | Flow run 'rare-piculet' - Finished in state Completed('All states completed.')
21:59:50.578 | INFO    | Flow run 'solemn-muskrat' - Created subflow run 'premium-loon' for flow 'etl-gcs-to-bq'
21:59:50.662 | INFO    | Flow run 'premium-loon' - Created task run 'extract_from_gcs-968e3b65-0' for task 'extract_from_gcs'
21:59:50.662 | INFO    | Flow run 'premium-loon' - Executing 'extract_from_gcs-968e3b65-0' immediately...
21:59:51.295 | INFO    | Task run 'extract_from_gcs-968e3b65-0' - Finished in state Completed()
21:59:51.347 | INFO    | Flow run 'premium-loon' - Created task run 'transform-a7d916b4-0' for task 'transform'
21:59:51.347 | INFO    | Flow run 'premium-loon' - Executing 'transform-a7d916b4-0' immediately...
21:59:52.540 | INFO    | Task run 'transform-a7d916b4-0' - no column airport_fee to drop
21:59:52.611 | INFO    | Task run 'transform-a7d916b4-0' - Finished in state Completed()
21:59:52.613 | INFO    | Flow run 'premium-loon' - row count: 7832545
21:59:52.683 | INFO    | Flow run 'premium-loon' - Created task run 'write_bq-b366772c-0' for task 'write_bq'
21:59:52.685 | INFO    | Flow run 'premium-loon' - Executing 'write_bq-b366772c-0' immediately...
22:01:42.011 | INFO    | Task run 'write_bq-b366772c-0' - Finished in state Completed()
22:01:42.037 | INFO    | Flow run 'premium-loon' - Finished in state Completed('All states completed.')
22:01:42.073 | INFO    | Flow run 'solemn-muskrat' - Finished in state Completed('All states completed.')
```



## Question 4. Github Storage Block

Using the `web_to_gcs` script from the videos as a guide, you want to store your flow code in a GitHub repository for collaboration with your team. Prefect can look in the GitHub repo to find your flow code and read it. Create a GitHub storage block from the UI or in Python code and use that in your Deployment instead of storing your flow code locally or baking your flow code into a Docker image. 

Note that you will have to push your code to GitHub, Prefect will not push it for you.

Run your deployment in a local subprocess (the default if you don???t specify an infrastructure). Use the Green taxi data for the month of November 2020.

How many rows were processed by the script?

- [ ] 88,019
- [ ] 192,297
- [x] 88,605
- [ ] 190,225

```
from prefect.deployments import Deployment
from prefect.filesystems import GitHub

github_block = GitHub.load("github-zoom")

github_block.get_directory()

from parametrized_flow import etl_parent_flow

deployment = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="git-run",
    version=1,
    parameters={"color":"green", "months": [11], "year": 2020},
)

deployment.apply()
```

```
23:01:16.419 | INFO    | prefect.agent - Submitting flow run 'fb0dd07d-c5f4-467a-83bd-db9e415ec8c3'
23:01:16.495 | INFO    | prefect.infrastructure.process - Opening process 'omicron4-humboldt-nebula'...
23:01:16.525 | INFO    | prefect.agent - Completed submission of flow run 'fb0dd07d-c5f4-467a-83bd-db9e415ec8c3'
c:\users\d.takeshi\appdata\local\programs\python\python39\lib\runpy.py:127: RuntimeWarning: 'prefect.engine' found in sys.modules after import of package 'prefect', but prior to execution of 'prefect.engine'; this may result in unpredictable behaviour
  warn(RuntimeWarning(msg))
23:01:20.494 | INFO    | Flow run 'omicron4-humboldt-nebula' - Downloading flow code from storage at 'C:\\Users\\d.takeshi\\Documents\\workspace\\de-zoomcamp-2023\\week-02\\homework'
23:01:20.774 | INFO    | Flow run 'omicron4-humboldt-nebula' - Created subflow run 'vague-flamingo' for flow 'etl-web-to-gcs'
23:01:20.843 | INFO    | Flow run 'vague-flamingo' - Created task run 'fetch-ba00c645-0' for task 'fetch'
23:01:20.843 | INFO    | Flow run 'vague-flamingo' - Executing 'fetch-ba00c645-0' immediately...
23:01:21.009 | INFO    | Task run 'fetch-ba00c645-0' - Finished in state Cached(type=COMPLETED)
23:01:21.215 | INFO    | Flow run 'vague-flamingo' - Created task run 'clean-2c6af9f6-0' for task 'clean'
23:01:21.215 | INFO    | Flow run 'vague-flamingo' - Executing 'clean-2c6af9f6-0' immediately...
23:01:21.358 | INFO    | Task run 'clean-2c6af9f6-0' - rows: 88605
23:01:21.376 | INFO    | Task run 'clean-2c6af9f6-0' - Finished in state Completed()
23:01:21.425 | INFO    | Flow run 'vague-flamingo' - Created task run 'write_local-09e9d2b8-0' for task 'write_local'
23:01:21.426 | INFO    | Flow run 'vague-flamingo' - Executing 'write_local-09e9d2b8-0' immediately...
23:01:21.720 | INFO    | Task run 'write_local-09e9d2b8-0' - Finished in state Completed()
23:01:21.761 | INFO    | Flow run 'vague-flamingo' - Created task run 'write_gcs-67f8f48e-0' for task 'write_gcs'
23:01:21.761 | INFO    | Flow run 'vague-flamingo' - Executing 'write_gcs-67f8f48e-0' immediately...
23:01:21.892 | INFO    | Task run 'write_gcs-67f8f48e-0' - Getting bucket 'zoomcamp-bucket'.
23:01:24.161 | INFO    | Task run 'write_gcs-67f8f48e-0' - Uploading from 'data\\green\\green_tripdata_2020-11.parquet' to the bucket 'zoomcamp-bucket' path 'data\\green\\green_tripdata_2020-11.parquet'.
23:01:28.392 | INFO    | Task run 'write_gcs-67f8f48e-0' - Finished in state Completed()
23:01:28.440 | INFO    | Flow run 'vague-flamingo' - Finished in state Completed('All states completed.')
23:01:28.466 | INFO    | Flow run 'omicron4-humboldt-nebula' - Finished in state Completed('All states completed.')
```

## Question 5. Email or Slack notifications

Q5. It???s often helpful to be notified when something with your dataflow doesn???t work as planned. Choose one of the options below for creating email or slack notifications.

The hosted Prefect Cloud lets you avoid running your own server and has Automations that allow you to get notifications when certain events occur or don???t occur. 

Create a free forever Prefect Cloud account at app.prefect.cloud and connect your workspace to it following the steps in the UI when you sign up. 

Set up an Automation that will send yourself an email when a flow run completes. Run the deployment used in Q4 for the Green taxi data for April 2019. Check your email to see the notification.

Alternatively, use a Prefect Cloud Automation or a self-hosted Orion server Notification to get notifications in a Slack workspace via an incoming webhook. 

Join my temporary Slack workspace with [this link](https://join.slack.com/t/temp-notify/shared_invite/zt-1odklt4wh-hH~b89HN8MjMrPGEaOlxIw). 400 people can use this link and it expires in 90 days. 

In the Prefect Cloud UI create an [Automation](https://docs.prefect.io/ui/automations) or in the Prefect Orion UI create a [Notification](https://docs.prefect.io/ui/notifications/) to send a Slack message when a flow run enters a Completed state. Here is the Webhook URL to use: https://hooks.slack.com/services/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

Test the functionality.

Alternatively, you can grab the webhook URL from your own Slack workspace and Slack App that you create. 


How many rows were processed by the script?

- [ ] `125,268`
- [ ] `377,922`
- [ ] `728,390`
- [x] `514,392`

```used a Slack webhook```

```
Takeshi's Flow:
Flow run etl-parent-flow/chestnut-teal entered state Completed at 2023-02-02T10:46:37.090236+00:00.
Flow ID: 9139412a-94d0-4435-be00-0b03e474c2fd
Flow run ID: 1f2dbbc4-c74c-455b-ba3f-fcdf0f8ac7db
Flow run URL: https://app.prefect.cloud/account/a81054f4-1636-4881-973f-eef111659f5d/workspace/0813832e-7235-48b2-b585-e5a0debab32d/flow-runs/flow-run/1f2dbbc4-c74c-455b-ba3f-fcdf0f8ac7db
State message: All states completed.
```
```
07:46:28.265 | INFO    | Flow run 'chestnut-teal' - Created subflow run 'dark-cuscus' for flow 'etl-web-to-gcs'
07:46:29.118 | INFO    | Flow run 'dark-cuscus' - Created task run 'fetch-ba00c645-0' for task 'fetch'
07:46:29.119 | INFO    | Flow run 'dark-cuscus' - Executing 'fetch-ba00c645-0' immediately...
07:46:29.596 | INFO    | Task run 'fetch-ba00c645-0' - Finished in state Cached(type=COMPLETED)
07:46:31.067 | INFO    | Flow run 'dark-cuscus' - Created task run 'clean-2c6af9f6-0' for task 'clean'
07:46:31.068 | INFO    | Flow run 'dark-cuscus' - Executing 'clean-2c6af9f6-0' immediately...
07:46:32.412 | INFO    | Task run 'clean-2c6af9f6-0' - rows: 514392
07:46:32.682 | INFO    | Task run 'clean-2c6af9f6-0' - Finished in state Completed()
07:46:32.920 | INFO    | Flow run 'dark-cuscus' - Created task run 'write_local-09e9d2b8-0' for task 'write_local'
07:46:32.921 | INFO    | Flow run 'dark-cuscus' - Executing 'write_local-09e9d2b8-0' immediately...
07:46:35.162 | INFO    | Task run 'write_local-09e9d2b8-0' - Finished in state Completed()
07:46:35.386 | INFO    | Flow run 'dark-cuscus' - Created task run 'write_gcs-67f8f48e-0' for task 'write_gcs'
07:46:35.387 | INFO    | Flow run 'dark-cuscus' - Executing 'write_gcs-67f8f48e-0' immediately...
07:46:36.647 | INFO    | Task run 'write_gcs-67f8f48e-0' - Finished in state Completed()
07:46:36.927 | INFO    | Flow run 'dark-cuscus' - Finished in state Completed('All states completed.')
07:46:37.183 | INFO    | Flow run 'chestnut-teal' - Finished in state Completed('All states completed.')
07:46:38.411 | INFO    | prefect.infrastructure.process - Process 'chestnut-teal' exited cleanly.
```

## Question 6. Secrets

Prefect Secret blocks provide secure, encrypted storage in the database and obfuscation in the UI. Create a secret block in the UI that stores a fake 10-digit password to connect to a third-party service. Once you???ve created your block in the UI, how many characters are shown as asterisks (*) on the next page of the UI?

- [ ] 5
- [ ] 6
- [x] 8
- [ ] 10

![secret](homework/prefect_six.png)

## Submitting the solutions

* Form for submitting: https://forms.gle/PY8mBEGXJ1RvmTM97
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 6 February (Monday), 22:00 CET


## Solution

We will publish the solution here
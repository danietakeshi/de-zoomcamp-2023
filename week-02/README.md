## Week 2 Homework

The goal of this homework is to familiarise users with workflow orchestration and observation. 


## Question 1. Load January 2020 data

Using the `etl_web_to_gcs.py` flow that loads taxi data into GCS as a guide, create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. Look at the logs to find out how many rows the dataset has.

How many rows does that dataset have?

- [x] 447,770
- [ ] 766,792
- [ ] 299,234
- [ ] 822,132

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

Using the flow in `etl_web_to_gcs.py`, create a deployment to run on the first of every month at 5am UTC. What’s the cron schedule for that?

- [x] `0 5 1 * *`
- [ ] `0 0 5 1 *`
- [ ] `5 * 1 0 *`
- [ ] `* * 5 1 0`

```“At 05:00 on day-of-month 1.”```

## Question 3. Loading data to BigQuery 

Using `etl_gcs_to_bq.py` as a starting point, modify the script for extracting data from GCS and loading it into BigQuery. This new script should not fill or remove rows with missing values. (The script is really just doing the E and L parts of ETL).

The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.

Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color. 

Make any other necessary changes to the code for it to function as required.

Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).

Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. Run your deployment to append this data to your BiqQuery table. How many rows did your flow code process?

- [ ] 14,851,920
- [ ] 12,282,990
- [ ] 27,235,753
- [ ] 11,338,483



## Question 4. Github Storage Block

Using the `web_to_gcs` script from the videos as a guide, you want to store your flow code in a GitHub repository for collaboration with your team. Prefect can look in the GitHub repo to find your flow code and read it. Create a GitHub storage block from the UI or in Python code and use that in your Deployment instead of storing your flow code locally or baking your flow code into a Docker image. 

Note that you will have to push your code to GitHub, Prefect will not push it for you.

Run your deployment in a local subprocess (the default if you don’t specify an infrastructure). Use the Green taxi data for the month of November 2020.

How many rows were processed by the script?

- [ ] 88,019
- [ ] 192,297
- [ ] 88,605
- [ ] 190,225



## Question 5. Email or Slack notifications

Q5. It’s often helpful to be notified when something with your dataflow doesn’t work as planned. Choose one of the options below for creating email or slack notifications.

The hosted Prefect Cloud lets you avoid running your own server and has Automations that allow you to get notifications when certain events occur or don’t occur. 

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
- [ ] `514,392`


## Question 6. Secrets

Prefect Secret blocks provide secure, encrypted storage in the database and obfuscation in the UI. Create a secret block in the UI that stores a fake 10-digit password to connect to a third-party service. Once you’ve created your block in the UI, how many characters are shown as asterisks (*) on the next page of the UI?

- [ ] 5
- [ ] 6
- [ ] 8
- [ ] 10


## Submitting the solutions

* Form for submitting: https://forms.gle/PY8mBEGXJ1RvmTM97
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 6 February (Monday), 22:00 CET


## Solution

We will publish the solution here
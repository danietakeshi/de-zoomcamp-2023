'''from prefect.filesystems import GCS

gcp_cloud_storage_bucket_block = GCS.load("zoom-gcs")

print(dir(gcp_cloud_storage_bucket_block))'''

from prefect.blocks.notifications import SlackWebhook

slack_webhook_block = SlackWebhook.load("zoomcamp")
slack_webhook_block.notify("Hello from Prefect!")
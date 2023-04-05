
from football_api import api_to_gcs_fixtures
from prefect import get_client
from prefect.deployments import Deployment
from prefect.filesystems import GCS
from prefect_gcp.cloud_run import CloudRunJob

client = get_client()

gcs_block = GCS.load("zoom-gcs")
cloud_run_job_block = CloudRunJob.load('cloud-run-zoomcamp')

deployment = Deployment.build_from_flow(
    flow=api_to_gcs_fixtures,
    name='API to GCS (CR)',
    storage=gcs_block,
    infrastructure=cloud_run_job_block,
)

if __name__ == '__main__':
    deployment.apply()
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
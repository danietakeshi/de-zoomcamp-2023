from prefect.deployments import Deployment
from prefect.filesystems import GitHub

github_block = GitHub.load("github-zoom")

#github_block.get_file("week-02")
print(github_block.save('week-02/parametrized_flow.py'))
print(dir(github_block))

'''deployment = Deployment.build_from_flow(
    flow="week-02/parametrized_flow.py:etl_parent_flow",
    name="git",
    version=1,
    parameters={"color":"green", "months": [11], "year": 2020}
)

deployment.apply()'''
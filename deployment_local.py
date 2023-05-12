from main import main
from prefect.deployments import Deployment

deployment = Deployment.build_from_flow(
    flow=main,
    name="entry_point_local",
    work_pool_name="ccdi-mtp-work-pool-local"
)

if __name__ == "__main__":
    deployment.apply()




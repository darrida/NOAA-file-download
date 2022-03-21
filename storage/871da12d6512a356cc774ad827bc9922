from prefect import flow, task, get_run_logger
from prefect.deployments import DeploymentSpec

DeploymentSpec(
    name="leonardo-deployment",
    flow_location="./leoflow.py",
    tags=['tutorial','test'],
    parameters={'name':'Leo'}
)

@task
def log_message(name):
    logger = get_run_logger()
    logger.info(f"Hello {name}!")
    return

@flow(name="leonardo_dicapriflow")
def leonardo_dicapriflow(name: str):
    log_message(name)
    return
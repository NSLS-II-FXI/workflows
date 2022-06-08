import prefect
from prefect import task, Flow, Parameter

from tiled.client import from_profile


@task
def read_data(uid):
    logger = prefect.context.get("logger")
    c = from_profile("nsls2", username=None)
    run = c["fxi"][uid]["primary"].read()
    logger.info("Done")

with Flow("validate-data-fxi-test") as flow:
    uid = Parameter("uid")
    read_data(uid)


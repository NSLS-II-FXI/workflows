import prefect
from prefect import task, Flow, Parameter
import time as ttime
from tiled.client import from_profile

@task
def read_all_streams(uid):
    logger = prefect.context.get("logger")
    c = from_profile("nsls2", username=None)
    run = c["fxi"][uid]
    start_time = ttime.monotonic()
    for stream in run:
        stream_start_time = ttime.monotonic()
        stream_data = run[stream].read()
        stream_elapsed_time = ttime.monotonic() - stream_start_time
        logger.info(f"{stream} elapsed_time = {stream_elapsed_time}")
        logger.info(f"{stream} nbytes = {stream_data.nbytes:_}")
    elapsed_time = ttime.monotonic() - start_time
    logger.info(f"total {elapsed_time = }")


with Flow("fxi-data-validation") as flow:
    uid = Parameter("uid")
    read_all_streams(uid)


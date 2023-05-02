from prefect import task, flow, get_run_logger
from data_validation import general_data_validation
# TODO: export needs Prefect 2 migration
# from export import export


@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")

@flow
def end_of_run_workflow(stop_doc):
    uid = stop_doc["run_start"]
    general_data_validation(uid)
    # export(uid)
    log_completion()

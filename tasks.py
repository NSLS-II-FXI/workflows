from prefect import task
from tomo_recon import rotcen_test2
import databroker

@task
def call_find_rot(uid):
    img, cen = rotcen_test2(uid)

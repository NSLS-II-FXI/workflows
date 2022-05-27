from prefect import task
from tomo_recon import rotcen_test2
import databroker

@task
def call_find_rot(uid):
    data = db[uid]
    scan_id = data['start']['scan_id']
    filename = f'rotation_center_scan_{scan_id}.h5'
    rotcen_test2(filename)

from prefect import task, Flow
from load_scan import get_tomo_images
from tomo_recon_tiled import rotcen_test2
from tiled.client import from_profile

@task
def call_find_rot(uid):
    c = from_profile('fxi')
    scan_result = c[uid]
    img_tomo = get_tomo_images(scan_result) 
    img, cen = rotcen_test2(img_tomo)

with Flow("test-find-rot") as flow1:
    call_find_rot('123456')

flow1.register(project_name="TST")

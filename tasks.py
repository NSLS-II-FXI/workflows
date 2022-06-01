from prefect import task
from load_scan import get_tomo_images
from tomo_recon_tiled import rotcen_test2
from tiled.client import from_profile

@task
def call_find_rot(uid):
    c = from_profile('fxi')
    scan_result = c[uid]
    img_tomo = get_tomo_images(scan_result) 
    img, cen = rotcen_test2(img_tomo)

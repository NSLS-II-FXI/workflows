from prefect import task, Flow
from load_scan import get_tomo_images
from tomo_recon_tiled import rotcen_test2
from tiled.client import from_profile

@task
def call_find_rot(uid):
    c = from_profile('fxi')
    scan_result = c[uid]

    dark_scan_id = scan_result.start["plan_args"]["dark_scan_id"]
    bkg_scan_id = scan_result.start["plan_args"]["bkg_scan_id"]

    # sanity check: make sure we remembered the right stream name
    assert "zps_pi_r_monitor" in scan_result.stream_names
    pos = scan_result.table("zps_pi_r_monitor")
    imgs = np.array(list(scan_result.data("Andor_image")))

    s1 = imgs.shape
    chunk_size = s1[1]
    imgs = imgs.reshape(-1, s1[2], s1[3])

    # load darks and bkgs
    img_dark = np.array(list(c[dark_scan_id].data("Andor_image")))[0]
    img_bkg = np.array(list(c[bkg_scan_id].data("Andor_image")))[0]
    s = img_dark.shape
    img_dark_avg = np.mean(img_dark, axis=0).reshape(1, s[1], s[2])
    img_bkg_avg = np.mean(img_bkg, axis=0).reshape(1, s[1], s[2])

    with db.reg.handler_context({"AD_HDF5": AreaDetectorHDF5TimestampHandler}):
        chunked_timestamps = list(scan_result.data("Andor_image"))

    mot_pos = np.array(pos["zps_pi_r"])

    input_dict = {'pos': pos,
                  'imgs': imgs,
                  'chunked_timestamps': chunked_timestamps,
                  'mot_pos': mot_pos}
    img_tomo, img_angle = get_tomo_images(input_dict)
    img, cen = rotcen_test2(img_tomo, img_bkg_avg, img_dark_avg, img_angle)
    return img, cen

with Flow("test-find-rot") as flow1:
    call_find_rot('123456')

flow1.register(project_name="TST")

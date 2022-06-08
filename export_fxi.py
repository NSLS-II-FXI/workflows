import databroker
import prefect
from prefect import task, Flow

@task
def run_export_fxi():

    client = databroker.from_profile("nsls2", username=None)
    scan_id = client['fxi'][-1].start['scan_id']
    logger = prefect.context.get("logger")
    logger.info(f"Scan ID: {scan_id}")
    export_scan(scan_id) 

def export_scan(scan_id, scan_id_end=None, binning=4, date_end_by=None, fpath=None):
    """
    e.g. load_scan([0001, 0002])
    """

    # client = databroker.from_profile("nsls2", username=None)
    # scan_id = client['fxi'][-1].start['scan_id']
    logger = prefect.context.get("logger")
    logger.info(f"Scan ID: {scan_id}")

    if scan_id_end is None:
        if isinstance(scan_id, int):
            scan_id = [scan_id]
        for item in scan_id:
            
            export_single_scan(int(item), binning)
            # custom_export(int(item), binning, date_end_by=date_end_by, fpath=fpath)
            # db.reg.clear_process_cache()
            logger.info("Inside Scan ID loop")
    else:
        for i in range(scan_id, scan_id_end + 1):
            export_single_scan(int(i), binning)
            # custom_export(int(i), binning, date_end_by=date_end_by, fpath=fpath)
            # db.reg.clear_process_cache()
            logger.info("Inside Scan ID loop None")

def export_single_scan(scan_id, binning=4, fpath=None):

    import datetime
    client = databroker.from_profile("nsls2", username=None)
    logger = prefect.context.get("logger")

    # h = db[scan_id]
    # scan_id = h.start["scan_id"]
    # scan_type = h.start["plan_name"]
    scan_type = client['fxi'][-1].start['plan_name']
    #    x_eng = h.start['XEng']
    t_new = datetime.datetime(2021, 5, 1)
    #t = h.start["time"] - 3600 * 60 * 4  # there are 4hour offset
    t = client['fxi'][-1].start['time']
    t = datetime.datetime.utcfromtimestamp(t)
    if t < t_new:
        scan = "old"
    else:
        scan = "new"

    logger.info(f"Plan name: {scan_type}")
    logger.debug("Debug message")

#    if scan_type == "tomo_scan":
#        print("exporting tomo scan: #{}".format(scan_id))
#        if scan == "old":
#            export_tomo_scan_legacy(h, fpath)
#        else:
#            export_tomo_scan_legacy(h, fpath)
#        print("tomo scan: #{} loading finished".format(scan_id))
#
#    elif scan_type == "fly_scan":
#        print("exporting fly scan: #{}".format(scan_id))
#        if scan == "old":
#            export_fly_scan_legacy(h, fpath)
#        else:
#            export_fly_scan(h, fpath)
#        export_fly_scan(h, fpath)
#        print("fly scan: #{} loading finished".format(scan_id))
#
#    elif scan_type == "fly_scan2":
#        print("exporting fly scan2: #{}".format(scan_id))
#        export_fly_scan2(h, fpath)
#        print("fly scan2: #{} loading finished".format(scan_id))
#    elif scan_type == "xanes_scan" or scan_type == "xanes_scan2":
#        print("exporting xanes scan: #{}".format(scan_id))
#        if scan == "old":
#            export_xanes_scan_legacy(h, fpath)
#        else:
#            export_xanes_scan(h, fpath)
#        print("xanes scan: #{} loading finished".format(scan_id))
#
#    elif scan_type == "xanes_scan_img_only":
#        print("exporting xanes scan image only: #{}".format(scan_id))
#        export_xanes_scan_img_only(h, fpath)
#        print("xanes scan img only: #{} loading finished".format(scan_id))
#    elif scan_type == "z_scan":
#        print("exporting z_scan: #{}".format(scan_id))
#        export_z_scan(h, fpath)
#    elif scan_type == "z_scan2":
#        print("exporting z_scan2: #{}".format(scan_id))
#        export_z_scan2(h, fpath)
#    elif scan_type == "z_scan3":
#        print("exporting z_scan3: #{}".format(scan_id))
#        export_z_scan2(h, fpath)
#    elif scan_type == "test_scan":
#        print("exporting test_scan: #{}".format(scan_id))
#        export_test_scan(h, fpath)
#    elif scan_type == "multipos_count":
#        print(f"exporting multipos_count: #{scan_id}")
#        export_multipos_count(h, fpath)
#    elif scan_type == "grid2D_rel":
#        print("exporting grid2D_rel: #{}".format(scan_id))
#        export_grid2D_rel(h, fpath)
#    elif scan_type == "raster_2D":
#        print("exporting raster_2D: #{}".format(scan_id))
#        export_raster_2D(h, binning)
#    elif scan_type == "raster_2D_2":
#        print("exporting raster_2D_2: #{}".format(scan_id))
#        export_raster_2D(h, binning, fpath)
#    elif scan_type == "count" or scan_type == "delay_count":
#        print("exporting count: #{}".format(scan_id))
#        export_count_img(h, fpath)
#    elif scan_type == "multipos_2D_xanes_scan2":
#        print("exporting multipos_2D_xanes_scan2: #{}".format(scan_id))
#        export_multipos_2D_xanes_scan2(h, fpath)
#    elif scan_type == "multipos_2D_xanes_scan3":
#        print("exporting multipos_2D_xanes_scan3: #{}".format(scan_id))
#        export_multipos_2D_xanes_scan3(h, fpath)
#    elif scan_type == "delay_scan":
#        print("exporting delay_scan #{}".format(scan_id))
#        export_delay_scan(h, fpath)
#    elif scan_type in ("user_fly_only", "dmea_fly_only"):
#        print("exporting user_fly_only #{}".format(scan_id))
#        export_user_fly_only(h, fpath)
#    else:
#        print("Un-recognized scan type ......")


with Flow("export_fxi") as flow:
    #print_scanid()
    run_export_fxi()

flow.register(project_name='TST',
              labels=['fxi-2022-2.2'],
              add_default_labels=False,
              set_schedule_active=False)

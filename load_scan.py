from datetime import datetime
import pandas as pd


EPICS_EPOCH = datetime(1990, 1, 1, 0, 0)


def convert_AD_timestamps(ts):
    return pd.to_datetime(ts, unit="s", origin=EPICS_EPOCH, utc=True).dt.tz_convert(
        "US/Eastern"
    )

def export_user_fly_only(h, fpath=None):
    if fpath is None:
        fpath = "./"
    else:
        if not fpath[-1] == "/":
            fpath += "/"
    uid = h.start["uid"]
    note = h.start["note"]
    scan_type = h.start["plan_name"]
    scan_id = h.start["scan_id"]
    scan_time = h.start["time"]
    dark_scan_id = h.start["plan_args"]["dark_scan_id"]
    bkg_scan_id = h.start["plan_args"]["bkg_scan_id"]
    x_pos = h.table("baseline")["zps_sx"][1]
    y_pos = h.table("baseline")["zps_sy"][1]
    z_pos = h.table("baseline")["zps_sz"][1]
    r_pos = h.table("baseline")["zps_pi_r"][1]

    try:
        x_eng = h.start["XEng"]
    except:
        x_eng = h.start["x_ray_energy"]
    # sanity check: make sure we remembered the right stream name
    assert "zps_pi_r_monitor" in h.stream_names
    pos = h.table("zps_pi_r_monitor")
    imgs = np.array(list(h.data("Andor_image")))

    s1 = imgs.shape
    chunk_size = s1[1]
    imgs = imgs.reshape(-1, s1[2], s1[3])

    # load darks and bkgs
    img_dark = np.array(list(db[dark_scan_id].data("Andor_image")))[0]
    img_bkg = np.array(list(db[bkg_scan_id].data("Andor_image")))[0]
    s = img_dark.shape
    img_dark_avg = np.mean(img_dark, axis=0).reshape(1, s[1], s[2])
    img_bkg_avg = np.mean(img_bkg, axis=0).reshape(1, s[1], s[2])

    with db.reg.handler_context({"AD_HDF5": AreaDetectorHDF5TimestampHandler}):
        chunked_timestamps = list(h.data("Andor_image"))

    raw_timestamps = []
    for chunk in chunked_timestamps:
        raw_timestamps.extend(chunk.tolist())

    timestamps = convert_AD_timestamps(pd.Series(raw_timestamps))
    pos["time"] = pos["time"].dt.tz_localize("US/Eastern")

    img_day, img_hour = (
        timestamps.dt.day,
        timestamps.dt.hour,
    )
    img_min, img_sec, img_msec = (
        timestamps.dt.minute,
        timestamps.dt.second,
        timestamps.dt.microsecond,
    )
    img_time = (
        img_day * 86400 + img_hour * 3600 + img_min * 60 + img_sec + img_msec * 1e-6
    )
    img_time = np.array(img_time)

    mot_day, mot_hour = (
        pos["time"].dt.day,
        pos["time"].dt.hour,
    )
    mot_min, mot_sec, mot_msec = (
        pos["time"].dt.minute,
        pos["time"].dt.second,
        pos["time"].dt.microsecond,
    )
    mot_time = (
        mot_day * 86400 + mot_hour * 3600 + mot_min * 60 + mot_sec + mot_msec * 1e-6
    )
    mot_time = np.array(mot_time)

    mot_pos = np.array(pos["zps_pi_r"])
    offset = np.min([np.min(img_time), np.min(mot_time)])
    img_time -= offset
    mot_time -= offset
    mot_pos_interp = np.interp(img_time, mot_time, mot_pos)

    pos2 = mot_pos_interp.argmax() + 1
    img_tomo = imgs[: pos2 - chunk_size]  # tomo images

    fname = fpath + "fly_scan_id_" + str(scan_id) + ".h5"

    with h5py.File(fname, "w") as hf:
        hf.create_dataset("note", data=str(note))
        hf.create_dataset("uid", data=uid)
        hf.create_dataset("scan_id", data=int(scan_id))
        hf.create_dataset("scan_time", data=scan_time)
        hf.create_dataset("X_eng", data=x_eng)
        hf.create_dataset("img_bkg", data=np.array(img_bkg, dtype=np.uint16))
        hf.create_dataset("img_dark", data=np.array(img_dark, dtype=np.uint16))
        hf.create_dataset("img_bkg_avg", data=np.array(img_bkg_avg, dtype=np.float32))
        hf.create_dataset("img_dark_avg", data=np.array(img_dark_avg, dtype=np.float32))
        hf.create_dataset("img_tomo", data=np.array(img_tomo, dtype=np.uint16))
        hf.create_dataset("angle", data=img_angle)
        hf.create_dataset("x_ini", data=x_pos)
        hf.create_dataset("y_ini", data=y_pos)
        hf.create_dataset("z_ini", data=z_pos)
        hf.create_dataset("r_ini", data=r_pos)

    try:
        write_lakeshore_to_file(h, fname)
    except:
        print("fails to write lakeshore info into {fname}")

    del img_tomo
    del img_dark
    del img_bkg
    del imgs

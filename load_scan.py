from datetime import datetime
import numpy as np
import pandas as pd
from tiled.client import from_profile
from databroker.assets.handlers import AreaDetectorHDF5TimestampHandler


EPICS_EPOCH = datetime(1990, 1, 1, 0, 0)


def convert_AD_timestamps(ts):
    return pd.to_datetime(ts, unit="s", origin=EPICS_EPOCH, utc=True).dt.tz_convert(
        "US/Eastern"
    )

def get_tomo_images(h):
    db = from_profile('fxi')
    dark_scan_id = h.start["plan_args"]["dark_scan_id"]
    bkg_scan_id = h.start["plan_args"]["bkg_scan_id"]

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
    img_angle = mot_pos_interp[: pos2 - chunk_size]  # rotation angles
    img_tomo = imgs[: pos2 - chunk_size]  # tomo images
    return img_tomo, img_bkg_avg, img_dark_avg, img_angle

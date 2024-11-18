import asyncio
import json
import os
import shutil
import subprocess
from datetime import datetime
from typing import Union

import bottleneck
import fsspec
import matplotlib.pyplot as plt
import numpy as np
import requests
import ujson
import xarray as xr
from kerchunk.grib2 import scan_grib
from mpl_toolkits import axes_grid1


def str_to_bool(s: str):
    s = s.lower()
    if s in ["y", "yes", "ye"]:
        return True
    else:
        return False


async def gather_with_concurrency(n, *tasks):
    semaphore = asyncio.Semaphore(n)

    async def sem_task(task):
        async with semaphore:
            return await task

    return await asyncio.gather(*(sem_task(task) for task in tasks))


def load_paths(dir):
    "Loads the json file with associated paths for program."
    with open(
        f"{dir}/paths.json",
    ) as f:
        paths = json.load(f)
    return paths


def req_status_bool(link):
    page = requests.get(link)
    return page.ok


def replace_year(x, year):
    """Year must be a leap year for this to work"""
    # Add number of days x is from JAN-01 to year-01-01
    x_year = np.datetime64(str(year) + "-01-01") + (x - x.astype("M8[Y]"))

    # Due to leap years calculate offset of 1 day for those days in non-leap year
    yr_mn = x.astype("M8[Y]") + np.timedelta64(59, "D")
    leap_day_offset = (yr_mn.astype("M8[M]") - yr_mn.astype("M8[Y]") - 1).astype(int)

    # However, due to days in non-leap years prior March-01,
    # correct for previous step by removing an extra day
    non_leap_yr_beforeMarch1 = (x.astype("M8[D]") - x.astype("M8[Y]")).astype(int) < 59
    non_leap_yr_beforeMarch1 = np.logical_and(
        non_leap_yr_beforeMarch1, leap_day_offset
    ).astype(int)
    day_offset = np.datetime64("1970") - (
        leap_day_offset - non_leap_yr_beforeMarch1
    ).astype("M8[D]")

    # Finally, apply the day offset
    x_year = x_year - day_offset
    return x_year


def add_colorbar(im, aspect=20, pad_fraction=0.5, **kwargs):
    """Add a vertical color bar to an image plot."""
    divider = axes_grid1.make_axes_locatable(im.axes)
    width = axes_grid1.axes_size.AxesY(im.axes, aspect=1.0 / aspect)
    pad = axes_grid1.axes_size.Fraction(pad_fraction, width)
    current_ax = plt.gca()
    cax = divider.append_axes("right", size=width, pad=pad)
    plt.sca(current_ax)
    return im.axes.figure.colorbar(im, cax=cax, **kwargs)


def gen_json(file_url, fs_local, so, json_dir, ens_key="spr", return_vars=False):
    out = scan_grib(
        file_url, storage_options=so
    )  # create the reference using scan_grib
    available_vars = []
    for _, message in enumerate(out):
        key_ = [n for n in message["refs"].keys() if "0.0" in n]
        available_vars.append(key_[0].split("/")[0])
        if "prmsl" in key_[0]:
            with fs_local.open(f"{json_dir}/gefs_rt_{ens_key}.json", "w") as f:
                f.write(ujson.dumps(message))  # write to file
                print(f"File {file_url} written to {json_dir}gefs_rt_{ens_key}.json")
    if return_vars:
        return available_vars


def uri_dict_recursive(uri_dict, gefs_live_date, fhour):
    for key in uri_dict:
        uri_dict[key] = (
            f's3://noaa-gefs-pds/gefs.{gefs_live_date.strftime("%Y%m%d")}'
            f'/{gefs_live_date.strftime("%H")}/atmos/pgrb2sp25/'
            f'ge{key}.t{gefs_live_date.strftime("%H")}z.pgrb2s.0p25.f{fhour:03d}'
        )
    return uri_dict


def most_recent_gefs(gefs_live_date: datetime, fhour: int, uri_dict: dict):
    fs = fsspec.filesystem("s3", anon=True, skip_instance_cache=True)
    uri_dict = uri_dict_recursive(uri_dict, gefs_live_date, fhour)
    for key in uri_dict:
        while not fs.exists(uri_dict[key]):
            gefs_live_date -= np.timedelta64(6, "h")
            uri_dict = uri_dict_recursive(uri_dict, gefs_live_date, fhour)
    return gefs_live_date, uri_dict


def combine_fcast_and_mcli(fcast, mcli):
    big_ds = xr.concat(
        [mcli["Pressure"].drop("timestr"), fcast["Pressure"].expand_dims("time")],
        dim="time",
    )
    percentile = bottleneck.rankdata(big_ds, axis=0) / len(big_ds["time"])
    return percentile

# pyright: reportMissingImports=false, reportMissingModuleSource=false

import subprocess
import pandas as pd 
import boto3
import xarray as xr
import urllib 
import os
import re
from datetime import datetime, timedelta
import numpy as np
import click
from tempfile import TemporaryDirectory
import s3fs
import pathlib
import asyncio
from collections.abc import Iterable, Dict

def create_selection_dict(
    latitude_bounds: Iterable[float],
    longitude_bounds: Iterable[float],
    forecast_days_bounds: Iterable[float],
) -> Dict[str, slice]:
    """Generate parameters to slice an xarray Dataset.
    Parameters
    ----------
    latitude_bounds : Iterable[float]
        The minimum and maximum latitude bounds to select.
    longitude_bounds : Iterable[float]
        The minimum and maximum longitudes bounds to select.
    forecast_days_bounds : Iterable[float]
        The earliest and latest forecast days to select.
    Returns
    -------
    Dict[str, slice]
        A dictionary of slices to use on an xarray Dataset.
    """
    latitude_slice = slice(max(latitude_bounds), min(latitude_bounds))
    longitude_slice = slice(min(longitude_bounds), max(longitude_bounds))
    first_forecast_hour = pd.Timedelta(f"{min(forecast_days_bounds)} days")
    last_forecast_hour = pd.Timedelta(f"{max(forecast_days_bounds)} days")
    forecast_hour_slice = slice(first_forecast_hour, last_forecast_hour)
    selection_dict = dict(
        latitude=latitude_slice, longitude=longitude_slice, step=forecast_hour_slice
    )
    return selection_dict

def date_range_seasonal(season, date_range=None):
    if date_range is not None:
        pass
    else:
        date_range = pd.date_range('2000-01-01','2019-12-31')
    season_dict = {
        'djf':[11,12,1,2,3],
        'mam':[2,3,4,5,6],
        'jja':[5,6,7,8,9],
        'son':[8,9,10,11,12]
    }
    dr = date_range[date_range.month.isin(season_dict[season]) &
                    ((date_range.month != season_dict[season][0]) | (date_range.day >= 21)) & 
            ((date_range.month != season_dict[season][-1]) | (date_range.day <= 10))
                   ]
    return dr

async def download_file(path): 
    base_file_name = s3_prefix.split("/")[-1]
    fpath = os.path.join(save_dir, f"{base_file_name.split('.')[0]}.nc")
    if pathlib.Path(fpath).exists():
        return fpath

    fs = s3fs.S3FileSystem(anon=True)
    with TemporaryDirectory() as dir:
        grib_file = os.path.join(dir, base_file_name)
        with fs.open(s3_prefix, "rb") as f, open(grib_file, "wb") as f2:
            f2.write(f.read())
    return fpath

# def combine_ens(output: str):

@click.command()
@click.option(
    "-v",
    "--var-names",
    default=["dswrf_sfc", "tcdc_eatm", "apcp_sfc"],
    help="Gridded fields to download.",
    multiple=True,
)
@click.option(
    "-p",
    "--pressure-levels",
    default=[],
    multiple=True,
    help="Pressure levels to use, if some pressure field is used.",
)
@click.option(
    "--latitude-bounds",
    nargs=2,
    type=click.Tuple([float, float]),
    default=(20, 60),
    help="Bounds for latitude range to keep when processing data.",
)
@click.option(
    "--longitude-bounds",
    nargs=2,
    type=click.Tuple([float, float]),
    default=(180, 310),
    help="Bounds for longitude range to keep when processing data, assumes values between 0-360.",
)
@click.option(
    "--forecast-days",
    nargs=2,
    type=click.Tuple([float, float]),
    default=(0, 7),
    help="Bounds for forecast days, where something like 5.5 would be 5 days 12 hours.",
)
@click.option(
    "--n-jobs", default=28, help="Number of jobs to run in parallel.",
)
@click.option(
    "-s",
    "--season",
    default='djf',
    help="Season to pull data from. djf, mam, jja, son.",
)
def download_process_reforecast(
    var_names,
    pressure_levels,
    latitude_bounds,
    longitude_bounds,
    forecast_days,
    n_jobs,
    season):
    source = 'https://noaa-gefs-retrospective.s3.amazonaws.com/GEFSv12/reforecast/'
    bucket = 'noaa-gefs-retrospective/GEFSv12/reforecast'
    ens = ['c00','p01','p02','p03','p04']
    dr = date_range_seasonal(season)
    selection_dict = create_selection_dict(
            latitude_bounds, longitude_bounds, forecast_days
        )  
    ##1. download all 5 ensembles at a time 2. process into mean and spread 3. compress

    # loop here, bunch into ensembles for each run at a time
    asyncio.get_event_loop().run_until_complete(download_file(sites))

if __name__ == "__main__":
    download_process_reforecast()
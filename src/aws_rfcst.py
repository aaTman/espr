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
from typing import Iterable, Dict
import logging
import aiobotocore
import aiofiles
import time

def create_selection_dict(
    latitude_bounds: Iterable[float],
    longitude_bounds: Iterable[float],
    forecast_days_bounds: Iterable[float],
    pressure_levels: Iterable[float],
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
    if len(pressure_levels) > 0:
        print('pressure levels not set up yet')
    latitude_slice = slice(max(latitude_bounds), min(latitude_bounds))
    longitude_slice = slice(min(longitude_bounds), max(longitude_bounds))
    first_forecast_hour = pd.Timedelta(f"{min(forecast_days_bounds)} days")
    last_forecast_hour = pd.Timedelta(f"{max(forecast_days_bounds)} days 01:00:00")
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

async def dl(fpath, fnames):
    bucket = 'noaa-gefs-retrospective'
    filenames = [n.split('/')[-1] for n in fnames]
    folder = 'GEFSv12/reforecast/2000/2000052100/c00/Days:1-10'
    session = aiobotocore.get_session()
    async with session.create_client('s3', region_name='us-west-2') as client:
        for s3_file in fnames:
            try:
                filename = s3_file.split('/')[-1]
                async with aiofiles.open(f"{fpath}/{filename}", "wb") as data:
                    response = await client.get_object(
                        Bucket=bucket, Key=s3_file
                    )
                    async with response["Body"] as stream:
                        content = await stream.read()
                        await data.write(content)
                
            except FileNotFoundError as e:
                print(e)

async def combine(fpath, fnames, selection_dict, final_path):
    output_file = f"{fnames[0].split('.')[0][:-4]}"
    ds = xr.open_mfdataset(f"{fpath}/*.grib2",engine='cfgrib',
                               combine='nested',
                               concat_dim='member',
                               
                               coords='minimal',
                               compat='override',
                               backend_kwargs={
                        'filter_by_keys': {'dataType': 'cf'},
                        'errors': 'ignore'
                    })
    ds = ds.sel(selection_dict)
    ds_mean = ds.mean('member')
    ds_std = ds.std('member')
    ds_mean.to_netcdf(f"{final_path}/{output_file}_mean.nc")
    ds_std.to_netcdf(f"{final_path}/{output_file}_std.nc")

async def pull_compress(fpath, fnames, selection_dict, final_path):
    await dl(fpath, fnames)
    await combine(fpath, selection_dict, final_path)

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
    default=(60, 20),
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
    "-s",
    "--season",
    default='djf',
    help="Season to pull data from. djf, mam, jja, son.",
)
@click.option(
    "--final-path",
    default='home/taylorm/espr/reforecast_v12/test',
    help="Where to save final file to.",
)
async def download_process_reforecast(
    var_names,
    pressure_levels,
    latitude_bounds,
    longitude_bounds,
    forecast_days,
    season,
    final_path):
    source = 'https://noaa-gefs-retrospective.s3.amazonaws.com/GEFSv12/reforecast/'
    bucket = 'noaa-gefs-retrospective/GEFSv12/reforecast'
    ens = ['c00','p01','p02','p03','p04']
    dr = date_range_seasonal(season)
    selection_dict = create_selection_dict(
            latitude_bounds, longitude_bounds, forecast_days, pressure_levels
        )
    s3_list = ['GEFSv12/reforecast'+n.strftime('/%Y/%Y%m%d00/')+m+n.strftime(f'/Days:1-10/{wx_var}_%Y%m%d00_{m}.grib2') 
    for wx_var in var_names 
    for n in dr 
    for m in ens]
          
    ##1. download all 5 ensembles at a time 2. process into mean and spread 3. compress
    s3_list = [bucket+n.strftime('/%Y/%Y%m%d00/')+m+n.strftime(f'/Days:1-10/{wx_var}_%Y%m%d00.grib2') 
    for n in dr 
    for m in ens 
    for wx_var in var_names]
    s3_list_gen = (s3_list[i:i+5] for i in range(0, len(s3_list), 5))
    files = [n for n in s3_list_gen]
    with TemporaryDirectory() as fpath:
       [await pull_compress(fpath, files, selection_dict, final_path) for files in s3_list_gen]
    
if __name__ == "__main__":
    asyncio.run_coroutine_threadsafe(download_process_reforecast())
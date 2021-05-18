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

source = 'https://noaa-gefs-retrospective.s3.amazonaws.com/GEFSv12/reforecast/'
bucket = 'noaa-gefs-retrospective/GEFSv12/reforecast'
vnames = ['dswrf_sfc_'] # ,'apcp_sfc_']'hgt_pres_abv700mb_','pres_msl_','tmp_pres_','pwat_eatm_', 'ugrd_hgt_','vgrd_hgt_' add in later
ens = ['c00','p01','p02','p03','p04']
s3 = boto3.client('s3')

def download_file(path):
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





def subset_grib():
    

def combine_ens(output: str):

def download_process_reforecast():

if __name__ == "__main__":
    download_process_reforecast()
import xarray as xr
import numpy as np
import pandas as pd
from dask.distributed import Client
import gefs_retrieve as gr 
import farray as fa
import mclimate as mc
import transforms
import utils as ut
from datetime import datetime
import os
import bottleneck
import logging
import sys
from pytz import timezone
import gc

logging.basicConfig(filename='gefs_retrieval.log', 
                level=logging.INFO, 
                format='%(asctime)s - %(levelname)s - %(message)s')
logging.Formatter.converter = lambda *args: datetime.now(tz=timezone('UTC')).timetuple()
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

def pull_gefs_files():
    stat=['mean','sprd']
    retr = gr.GEFSRetrieve(download=True, monitor=False, variable='PRMSL')
    _ = [retr.run(n) for n in stat]

def run_fcsts(paths):
    forecast_mean = fa.ForecastArray('mean', 'slp', paths=paths)
    forecast_sprd = fa.ForecastArray('sprd', 'slp', paths=paths)
    fmean = forecast_mean.load_forecast()
    fmean['time'] = fmean['valid_time']
    fsprd = forecast_sprd.load_forecast()
    fsprd['time'] = fsprd['valid_time']
    fmean = fmean.sortby('valid_time')
    fsprd = fsprd.sortby('valid_time')
    return fmean, fsprd

def run_mcli():
    mcli = mc.MClimate(datetime.today().strftime('%Y-%m-%d'), '/home/taylorm/espr/reforecast', 'slp')
    mc_mean = mcli.generate(stat='mean')
    mc_std = mcli.generate(stat='sprd')
    return mc_mean, mc_std

def interpolate_mcli(mc_mean, mc_std, fmean):
    mc_mean = transforms.xarr_interpolate(mc_mean, fmean)
    mc_std = transforms.xarr_interpolate(mc_std, fmean)
    return mc_mean, mc_std

def align_fmean_fsprd(fmean, fsprd, mc_mean):
    fmean = fmean.where(fmean['step'].isin(mc_mean['fhour']), drop=True)
    fmean = fmean.drop({'time'}).rename({'step':'fhour','time':'fhour'})
    fsprd = fsprd.where(fsprd['step'].isin(mc_mean['fhour']), drop=True)
    fsprd = fsprd.drop({'time'}).rename({'step':'fhour','time':'fhour'})
    return fmean, fsprd

def combine_fmean_mcli(fmean, mc_mean):
    big_ds = xr.concat([mc_mean['Pressure'].drop('timestr'),fmean['Pressure'].expand_dims('time')],dim='time')
    percentile = bottleneck.rankdata(big_ds,axis=0)/len(big_ds['time'])
    return percentile

if __name__ == "__main__":
    paths = ut.load_paths()
    paths['output'] = os.path.abspath(paths['output'])
    paths['data_store'] = os.path.abspath(paths['data_store'])
    client = Client(n_workers=8, threads_per_worker=2)
    pull_gefs_files()
    fmean, fsprd = run_fcsts(paths=paths)
    date = pd.to_datetime(fmean['valid_time'][0].values)
    logging.info('mcli started')
    mc_mean, mc_std = run_mcli()
    mc_mean, mc_std = interpolate_mcli(mc_mean, mc_std, fmean)
    fmean, fsprd = align_fmean_fsprd(fmean, fsprd, mc_mean)
    logging.info('mcli dask delayed complete')
    logging.info('percentile started')
    percentile = combine_fmean_mcli(fmean, mc_mean)

    gc.collect()
    logging.info('percentile complete')
    try:
        mc_std = mc_std['Pressure'].drop('timestr')
    except:
        pass
    subset_sprd = transforms.subset_sprd(percentile, mc_std)
    logging.info('spread subset complete')
    subset_sprd.to_netcdf(f'{paths["output"]}/subset_sprd_{date.year}{date.month:02}{date.day:02}_{date.hour:02}z.nc')
    logging.info('spread subset file created')
    hsa_final = transforms.hsa(fsprd, subset_sprd)
    hsa_final.to_netcdf(f'{paths["output"]}/hsa_{date.year}{date.month:02}{date.day:02}_{date.hour:02}z.nc')
    logging.info('hsa file created')





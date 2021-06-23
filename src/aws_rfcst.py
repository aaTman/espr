# pyright: reportMissingImports=false, reportMissingModuleSource=false
import subprocess
import glob
import os
import time
import tempfile
import logging
from datetime import timedelta
import asyncio
import pandas as pd
import xarray as xr 
import aiobotocore
import aiofiles
import aioboto3
from botocore.client import Config
import asyncclick as click
import pygrib
import cfgrib
import spread_skill


logging.basicConfig(filename='output.log', level=logging.WARNING)

config = Config(
    read_timeout=60,
    connect_timeout=60,
    retries={"max_attempts": 5}
)

def create_selection_dict(
    latitude_bounds,
    longitude_bounds,
    forecast_days_bounds,
    pressure_levels,
):
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

def len_warning(fpath,filetype='.gr'):
    return len([n for n in os.listdir(fpath) if filetype in n])

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

def file_check(final_path, output_file):
    if os.path.exists(f"{final_path}/{output_file}_std.nc"):
        logging.warning(f"{output_file} already processed, skipping")
        return True
    else:
        return False

def combine(fpath, output_file, selection_dict, final_path, stats):

    if len_warning(fpath) < 5:
        logging.warning(f"{output_file} mean will be less than 5")
    print(f"{output_file}")
    with open(f"{fpath}/{output_file}.grib2", 'w') as outfile:
        subprocess.run(['cat']+ glob.glob(fpath+'/*.grib2'), stdout=outfile)
    ensemble = pygrib.open(f'{fpath}/{output_file}.grib2')
    cf = xr.open_dataset(f'{fpath}/{output_file}.grib2',
    engine='cfgrib',
    backend_kwargs={
        'filter_by_keys':{'dataType':'cf'},
        'extra_coords':{"stepRange":"step"}
        },
        chunks={'number':1,'step':10}).sel(number=0).isel(step=slice(1,None,2))   
    pf = xr.open_dataset(f'{fpath}/{output_file}.grib2',
    engine='cfgrib',
    backend_kwargs={
        'filter_by_keys':{'dataType':'pf'},
        'extra_coords':{"stepRange":"step"}
        },
        chunks={'number':1,'step':10}).isel(step=slice(1,None,2))     
    import pdb; pdb.set_trace()
    ds = xr.concat([cf,pf],'number')
    ds = ds.sel(selection_dict)
    ds_mean = ds.mean('number')
    ds_std = ds.std('number')
    if stats:
        ss_stat = spread_skill.stats(ds,final_path)
    else:
        pass        
    ds_mean.to_netcdf(f"{final_path}/{output_file}_mean.nc", compute=False)
    ds_std.to_netcdf(f"{final_path}/{output_file}_std.nc",compute=False)
    # except cfgrib.dataset.DatasetBuildError as e:
    #     logging.error(f"{output_file} not created due to dataset build error")
    #     pass
    # except KeyError as e:
    #     logging.error(f"{output_file} not created due to KeyError")
    #     pass
    # except Error as e:
    #     logging.error(f"{output_file} not created due to {e}")
    #     pass

def create_mclimate(final_path, wx_var, season, rm, stats):
    final_file_mean = f"{final_path}/{wx_var}_mean_{season}.nc"
    final_file_std = f"{final_path}/{wx_var}_std_{season}.nc"
    ds = xr.open_mfdataset(f"{final_path}/{wx_var}*.nc",
                            combine='nested',
                            concat_dim='member',
                            coords='minimal',
                            compat='override'
                            )
    ds_mean = ds.mean('member')
    ds_std = ds.std('member') 
    ds_mean.to_netcdf(final_file_mean)
    ds_std.to_netcdf(final_file_std)
    logging.info(f"{wx_var} mean and spread for {season} generated.")
    print(f"{wx_var} mean and spread for {season} generated.")
    if rm:
        rm_files(final_path, wx_var)


def str_to_bool(s):
    if s in ['y','yes','ye']:
        return True
    else:
        return False

def rm_files(final_path, wx_var):
    files = glob.glob(f"{final_path}/{wx_var}_20*")
    logging.info(f"removing downloaded files for {wx_var}")
    for fp in files:
        try:
            os.remove(fp)
        except:
            logging.info(f"{fp} not removed, error")
            pass

async def gather_with_concurrency(n, *tasks):
    semaphore = asyncio.Semaphore(n)

    async def sem_task(task):
        async with semaphore:
            return await task
    return await asyncio.gather(*(sem_task(task) for task in tasks))

async def dl(fnames, selection_dict, final_path, stats):

    bucket = 'noaa-gefs-retrospective'
    filenames = [n.split('/')[-1] for n in fnames]
    fpaths = ['/'.join(n.split('/')[0:-1]) for n in fnames]
    async with aiofiles.tempfile.TemporaryDirectory() as fpath:
        async with aioboto3.resource('s3',config=config) as s3:
            for s3_file in fnames:
                output_file = f"{fnames[0].split('/')[-1].split('.')[-2][:-4]}"
                if file_check(final_path, output_file):
                    pass
                else:
                    try:
                        filename = s3_file.split('/')[-1]
                        await s3.meta.client.download_file(bucket, s3_file, f"{fpath}/{filename}")
                        print(f'{s3_file} read success!')
                    except FileNotFoundError as e:
                        logging.warning(f"{filename} not downloaded, not found")
                        print(e)
                        pass
                    except aiobotocore.response.AioReadTimeoutError as e:
                        logging.warning(f"{filename} not downloaded, timeout")
                        print(e)
                        pass
            if file_check(final_path, output_file):
                pass
            else:
                combine(fpath, output_file, selection_dict, final_path, stats)
        return f"{s3_file} downloaded, data written, combined"

@click.command()
@click.option(
    "-v",
    "--var-names",
    default=["dswrf_sfc", "tcdc_eatm"],
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
@click.option(
    '--semaphore',
    default=10,
    help="Number of tasks to run in async at once before worker denials."
)
@click.option(
    '-rm',
    default='n',
    help="Whether to delete downloaded files after combination into mclimate file (y or n, default n)"
)
@click.option(
    '-stats',
    default='n',
    help="Whether to run stats (stats summary saves to final-path, y or n, default n)"
)
async def download_process_reforecast(
    var_names,
    pressure_levels,
    latitude_bounds,
    longitude_bounds,
    forecast_days,
    season,
    final_path,
    semaphore,
    rm,
    stats):
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
    s3_list_gen = (s3_list[i:i+5] for i in range(0, len(s3_list), 5))
    files_list = [n for n in s3_list_gen]
    stats = str_to_bool(stats)
    coro = [dl(files, selection_dict, final_path, stats) for files in files_list]
    await gather_with_concurrency(semaphore, *coro)
    rm = str_to_bool(rm)
    
    [create_mclimate(final_path, wx_var, season, rm) for wx_var in var_names]

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(download_process_reforecast())
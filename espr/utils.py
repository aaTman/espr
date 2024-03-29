import numpy as np
import os 
from datetime import datetime
import subprocess
import shutil
import matplotlib.pyplot as plt
from mpl_toolkits import axes_grid1
import json
import asyncio
import requests
import xarray as xr

def str_to_bool(s: str):
    s = s.lower()
    if s in ['y','yes','ye']:
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
    with open(f'{dir}/paths.json',) as f:
        paths = json.load(f)
    return paths

def req_status_bool(link):
    page = requests.get(link)
    return page.ok

def replace_year(x, year):
    """ Year must be a leap year for this to work """
    # Add number of days x is from JAN-01 to year-01-01 
    x_year = np.datetime64(str(year)+'-01-01') +  (x - x.astype('M8[Y]'))

    # Due to leap years calculate offset of 1 day for those days in non-leap year
    yr_mn = x.astype('M8[Y]') + np.timedelta64(59,'D')
    leap_day_offset = (yr_mn.astype('M8[M]') - yr_mn.astype('M8[Y]') - 1).astype(int)

    # However, due to days in non-leap years prior March-01, 
    # correct for previous step by removing an extra day
    non_leap_yr_beforeMarch1 = (x.astype('M8[D]') - x.astype('M8[Y]')).astype(int) < 59
    non_leap_yr_beforeMarch1 = np.logical_and(non_leap_yr_beforeMarch1, leap_day_offset).astype(int)
    day_offset = np.datetime64('1970') - (leap_day_offset - non_leap_yr_beforeMarch1).astype('M8[D]')

    # Finally, apply the day offset 
    x_year = x_year - day_offset
    return x_year

def add_colorbar(im, aspect=20, pad_fraction=0.5, **kwargs):
    """Add a vertical color bar to an image plot."""
    divider = axes_grid1.make_axes_locatable(im.axes)
    width = axes_grid1.axes_size.AxesY(im.axes, aspect=1./aspect)
    pad = axes_grid1.axes_size.Fraction(pad_fraction, width)
    current_ax = plt.gca()
    cax = divider.append_axes("right", size=width, pad=pad)
    plt.sca(current_ax)
    return im.axes.figure.colorbar(im, cax=cax, **kwargs)

def cleaner():
    paths = json.load('paths.json')
    plot_dir = paths['plot_dir']
    # for file_name in os.listdir(ps.output_dir):
    #     if (datetime.now() - datetime.strptime(file_name[0:11],'%Y%m%d_%H')).total_seconds() > 604800:
    #         os.remove(f'{ps.output_dir}{file_name}')
    for file_name in os.listdir(plot_dir):
        if (datetime.now() - datetime.strptime(file_name[0:11],'%Y%m%d_%H')).total_seconds() > 604800:
           shutil.rmtree(f'{plot_dir}{file_name}')

def scp_call(source, dest):
    subprocess.call(['scp','-r',source,dest],stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def rsync_call(source, dest):
    subprocess.call(['rsync','-avh','--delete-before',source,dest],stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)



def subset_sprd(percentile, mc_std):
    mask = np.logical_and(percentile >= percentile[-1]-0.05, percentile <= percentile[-1]+0.05)
    try:
        mc_std = mc_std[[n for n in mc_std][0]]
    except:
        pass
    mc_std.rename({'fhour':'time','time':'fhour'})
    mask_da=xr.DataArray(mask[:-1], coords={
        'fhour':mc_std.fhour.values, 
        'time':mc_std.time.values, 
        'lat':mc_std.lat.values, 
        'lon':mc_std.lon.values 
        }, 
    dims={ 
        'time': len(mc_std.time), 
        'fhour':len(mc_std.fhour), 
        'lat': len(mc_std.lat), 
        'lon': len(mc_std.lon) 
        }
    )    
    mc_std_filtered  = mc_std.where(~np.isnan(mask_da),drop=True)
    return mc_std_filtered

# Define a context manager to suppress stdout and stderr.
class suppress_stdout_stderr(object):
    '''
    A context manager for doing a "deep suppression" of stdout and stderr in 
    Python, i.e. will suppress all print, even if the print originates in a 
    compiled C/Fortran sub-function.
       This will not suppress raised exceptions, since exceptions are printed
    to stderr just before a script exits, and after the context manager has
    exited (at least, I think that is why it lets exceptions through).      
    '''
    def __init__(self):
        # Open a pair of null files
        self.null_fds =  [os.open(os.devnull,os.O_RDWR) for x in range(2)]
        # Save the actual stdout (1) and stderr (2) file descriptors.
        self.save_fds = [os.dup(1), os.dup(2)]

    def __enter__(self):
        # Assign the null pointers to stdout and stderr.
        os.dup2(self.null_fds[0],1)
        os.dup2(self.null_fds[1],2)

    def __exit__(self, *_):
        # Re-assign the real stdout/stderr back to (1) and (2)
        os.dup2(self.save_fds[0],1)
        os.dup2(self.save_fds[1],2)
        # Close all file descriptors
        for fd in self.null_fds + self.save_fds:
            os.close(fd)        
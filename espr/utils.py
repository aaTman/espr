import numpy as np
import os 
from datetime import datetime, timedelta 
import paths as ps 
import subprocess
import shutil
import matplotlib.pyplot as plt
from mpl_toolkits import axes_grid1
import logging

def setup_logger(name, log_file, level=logging.INFO):
    
    handler = logging.FileHandler(log_file)        
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger

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

    # for file_name in os.listdir(ps.output_dir):
    #     if (datetime.now() - datetime.strptime(file_name[0:11],'%Y%m%d_%H')).total_seconds() > 604800:
    #         os.remove(f'{ps.output_dir}{file_name}')
    for file_name in os.listdir(ps.plot_dir):
        if (datetime.now() - datetime.strptime(file_name[0:11],'%Y%m%d_%H')).total_seconds() > 604800:
           shutil.rmtree(f'{ps.plot_dir}{file_name}')

def scp_call(source, dest):
    subprocess.call(['scp','-r',source,dest],stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def rsync_call(source, dest):
    subprocess.call(['rsync','-avh','--delete-before',source,dest],stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

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
from ftplib import FTP 
import re
import os
from datetime import datetime
from time import sleep
import logging
import numpy as np
import sys

logging.basicConfig(filename='gefs_retrieval.log', 
                level=logging.INFO, 
                format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

class GEFSRetrieve:
    """
    Retrieval class to return GEFS data.
    GEFS data has multiple sources to retrieve from. This class
    generates and returns the data to either temp or permanent
    folders from ftp or https (grib_filter), with priority from the
    grib_filter for I/O efficiency.

    Parameters
    ---------
    variable : string
        The variable of interest. Can be slp, pwat, tmp925, tmp850,
        or wnd (surface) currently. Other short names will be 
        accepted for the most part.
    store : bool
        If data is to be stored or only temporarily downloaded.
        If true, store_path must be set.
    monitor: bool
        Set to establish continuously running code. If true,
        will download most recent files when they are available.
    monitor_interval : int
        How often in seconds the monitor will run. Default is 30sec.
    latitude_bounds : tuple or list
        The range of latitudes to download. Default is 20N to 60N.
    freq : int
        What frequency of lead time will be downloaded. Default is 3hr.
    """
    def __init__(self, 
        variable, 
        store=False, 
        monitor=False, 
        monitor_interval=30, 
        latitude_bounds=(60,20), 
        longitude_bounds=(180,310), 
        freq=3):
        self.variable = variable
        assert self.variable in self.variable_store(), f'must be one of {self.variable_store()}'
        self.store = store
        self.monitor = monitor
        self.monitor_interval = monitor_interval
        self.latitude_bounds = latitude_bounds
        self.longitude_bounds = longitude_bounds
        self.freq = freq

    def variable_store(self):
        return ['APCP', 'CAPE', 'CFRZR', 'CICEP', 'CIN', 'CRAIN', 'CSNOW', 
        'DLWRF', 'DPT', 'DSWRF', 'GUST', 'HLCY', 'ICETK', 'LHTFL', 'PRES', 
        'PRMSL', 'PWAT', 'RH', 'SHTFL', 'SNOD', 'SOILW', 'TCDC', 'TMAX', 
        'TMIN', 'TMP', 'TSOIL', 'UGRD', 'ULWRF', 'USWRF', 'VGRD', 'WEASD']

    def build_query_dict(self):
        self.query_dict = {
            'latitude_min': min(self.latitude_bounds),
            'latitude_max': max(self.latitude_bounds),
            'longitude_min': min(self.longitude_bounds),
            'longitude_max': max(self.longitude_bounds),
            'var': self.variable,

        }    

    def most_recent_dir(self, ftp):
        ftpdir_list = ftp.nlst()
        new_dir_int = max([int(re.search(r'\d+', ftpdir).group()) for ftpdir in ftpdir_list])
        new_dir = [n for n in ftpdir_list if str(new_dir_int) in n][0]
        return new_dir

    def ftp_login(self, site='ftp.ncep.noaa.gov'):
        try:
            ftp = FTP(site)
            ftp.login()
        except Exception as e:
            logging.error(f'{e} error, retrying in {self.monitor_interval} sec...')
            sleep(self.monitor_interval)
            ftp = FTP(site)
            ftp.login()
        return ftp

    def ftp_change(self, ftp_dir='./'):
        ls_prev = set()
        while True:
            ftp = self.ftp_login(sleep_time=self.monitor_interval)
            logging.info(ftp.lastresp)
            ftp.cwd(ftp_dir)
            ftp.cwd(self.most_recent_dir(ftp))
            ls = set(ftp.nlst())
            ftp.quit()
            add, rem = ls-ls_prev, ls_prev-ls
            if add or rem: yield add, rem, ftp
            ls_prev = ls
            sleep(self.monitor_interval)

    def notify_changes(self):
        for add, rem, ftp in self.ftp_change('pub/data/nccf/com/gens/prod'):
            datenow = datetime.now().strftime('%Y %m %d %H:%m')
            logging.info(f'{datenow}')
            if len(add) > 0:
                logging.info('\n'.join('+ %s' % str(i) for i in add))
            if len(rem) > 0:
                logging.info('\n'.join('- %s' % str(i) for i in rem))
            sleep(self.monitor_interval)
        datenow = datetime.now().strftime('%Y %m %d %H:%m')
        logging.info(f'exiting script at {datenow}')

    def link_builder(self, query_dict):
        

    
def loop_and_download(stat, fhours, ftp, http=True, query_dict={}):
    assert stat in ['mean','sprd']; 'stat must be mean or sprd'
    if stat == 'mean':
        gestat = 'geavg'
        save_name = 'mean'
    file_valid_list = [n for n in file_list if np.logical_and('geavg' in n, '.idx' not in n)]
    file_valid_list = [n for n in file_valid_list if np.logical_and(int(n[-3:]) <= 168, int(n[-3:]) % 6 == 0)]
    file_valid_list = [n for n in file_valid_list if '011ab' not in n]
    for n in file_valid_list:
        fname = 'gefs_mean_'+n[-3:]+'.grib2'
        try:
            with open(fname, 'wb') as f:
                # Define the callback as a closure so it can access the opened 
                # file in local scope
                def callback(data):
                    f.write(data)
                ftp.retrbinary(f'RETR {fname}', callback)
        except Exception as e:
            logging.error(f'{n} failure, {e}')
            pass

def retr_files(add, ftp, ftpdir='/'):
    ftp = ftp_login()
    ftp.cwd(ftpdir)
    import pdb; pdb.set_trace()
    while True:
        try:
            ftp.cwd(most_recent_dir(ftp))
        except AttributeError:
            continue
    if 'atmos' in ftp.nlst():
        ftp.cwd('atmos')
    if 'pgrb2sp25' in ftp.nlst():
        ftp.cwd('pgrb2sp25')
    assert 'pgrb2sp25' in ftp.pwd(); f'not in file directory, currently in {ftp.pwd()}'
    file_list = ftp.nlst()
    file_valid_list = [n for n in file_list if np.logical_and('geavg' in n, '.idx' not in n)]
    file_valid_list = [n for n in file_valid_list if np.logical_and(int(n[-3:]) <= 168, int(n[-3:]) % 6 == 0)]
    file_valid_list = [n for n in file_valid_list if '011ab' not in n]
    for n in file_valid_list:
        fname = 'gefs_mean_'+n[-3:]+'.grib2'
        try:
            with open(fname, 'wb') as f:
                # Define the callback as a closure so it can access the opened 
                # file in local scope
                def callback(data):
                    f.write(data)
                ftp.retrbinary(f'RETR {fname}', callback)
        except Exception as e:
            logging.error(f'{n} failure, {e}')
            pass
    file_valid_list = [n for n in file_list if np.logical_and('gespr' in n, '.idx' not in n)]
    file_valid_list = [n for n in file_valid_list if np.logical_and(int(n[-3:]) <= 168, int(n[-3:]) % 6 == 0)]
    for n in file_valid_list:
        fname = 'gefs_sprd_'+n[-3:]+'.grib2'
        try:
            ftp.retrbinary(f"RETR {n}", open(temp_store + fname, 'wb').write)
        except:
            with open(log_directory + 'retrieval_log.txt', "a") as f:
                f.write(n + ' failure at ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'\n')
            break
    with open(log_directory + 'retrieval_log.txt', "a") as f:
        f.write('completed at ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'\n')

if __name__ == '__main__':
    notify_changes()

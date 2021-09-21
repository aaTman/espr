from ftplib import FTP 
import re
import os
from datetime import datetime
from time import sleep
import logging
from typing import Tuple
import numpy as np
import sys
import requests
from bs4 import BeautifulSoup

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
    hour_end : int
        Which hour to end the return at. Default is 168 (7 days).
    """
    def __init__(self, 
        variable: str, 
        store: bool=False, 
        monitor: bool=False, 
        monitor_interval: int=30, 
        latitude_bounds: Tuple=(60,20), 
        longitude_bounds: Tuple=(180,310), 
        freq: int=3,
        hour_end: int=168):
        self.variable = variable.upper()
        assert self.variable in self.variable_store(), f'must be one of {self.variable_store()}'
        self.store = store
        self.monitor = monitor
        self.monitor_interval = monitor_interval
        self.latitude_bounds = latitude_bounds
        self.longitude_bounds = longitude_bounds
        self.freq = freq
        self.hour_end = hour_end

    def variable_store(self):
        return ['APCP', 'CAPE', 'CFRZR', 'CICEP', 'CIN', 'CRAIN', 'CSNOW', 
        'DLWRF', 'DPT', 'DSWRF', 'GUST', 'HLCY', 'ICETK', 'LHTFL', 'PRES', 
        'PRMSL', 'PWAT', 'RH', 'SHTFL', 'SNOD', 'SOILW', 'TCDC', 'TMAX', 
        'TMIN', 'TMP', 'TSOIL', 'UGRD', 'ULWRF', 'USWRF', 'VGRD', 'WEASD']

    def build_query_dict(self):
        self.query_dict = {
            'latitude_min': f'bottomlat={min(self.latitude_bounds)}&',
            'latitude_max': f'toplat={max(self.latitude_bounds)}&',
            'longitude_min': f'leftlon={min(self.longitude_bounds)}&',
            'longitude_max': f'rightlon={max(self.longitude_bounds)}&',
            'var': f'var_{self.variable}=on&',
        }

    def build_ensemble_dict(self):
        self.ensemble_dict = {
            'mean': 'geavg',
            'sprd': 'gespr',
            'ensembles': {n: (f'gep{n:02}' if n>0 else f'gec{n:02}') for n in range(0,31)}
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

    def most_recent(self, type_return):
        assert type_return in ['ens', 'mean', 'sprd'], 'type_return must be ens, mean, or sprd'
        base_url = 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/gens/prod/'
        atmos_pgrb = 'atmos/pgrb2sp25/'
        date_base_set = set()
        level, changes = self.request_to_bs(base_url, date_base_set)
        modelhour_base_set = set()
        if changes:
            self.date_value = max(changes)
            level, changes = self.request_to_bs(level, modelhour_base_set)  
            if changes: 
                self.hour_value = max(changes)
                self.link_builder()
        if type_return == 'ens':
            return self.ens_fhour_links
        elif type_return == 'mean':
            return self.mean_fhour_links
        elif type_return == 'sprd':
            return self.sprd_fhour_links

    def most_recent_monitor(self):
        base_url = 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/gens/prod/'
        atmos_pgrb = 'atmos/pgrb2sp25/'
        date_base_set = set()
        while True:
            level, changes = self.request_to_bs(base_url, date_base_set)
            modelhour_base_set = set()
            if changes:
                self.date_value = list(changes)[0]
                level, changes = self.request_to_bs(level, modelhour_base_set)  
                if changes: 
                    self.hour_value = list(changes)[0]
                    self.link_builder()

    def request_to_bs(self, url, in_set):
        page = requests.get(url).text
        soup = BeautifulSoup(page, 'html.parser')
        links = set([a['href'] for a in soup.find_all('a',href=True)])
        most_recent = max(links)
        recursion_url = f'{url}{most_recent}'
        changes = links - in_set
        return recursion_url, changes

    def link_builder(self):
        self.build_query_dict()
        self.build_ensemble_dict()
        base_link = 'https://nomads.ncep.noaa.gov/cgi-bin/filter_gefs_atmos_0p25s.pl?'
        base_query = f"{base_link}{''.join([f'{self.query_dict[n]}' for n in self.query_dict])}"
        date_link = f'&dir=%2Fgefs.{self.date_value}%2F{self.hour_value}%2Fatmos%2Fpgrb2sp25&'
        new_link = base_query + date_link
        self.ens_fhour_links = [f"{new_link}file={self.ensemble_dict['ensembles'][m]}.t00z.pgrb2s.0p25.f{n:03}" for n in np.arange(0,self.hour_end+1,self.freq) for m in self.ensemble_dict['ensembles']]
        self.mean_fhour_links = [f"{new_link}file={self.ensemble_dict['mean']}.t00z.pgrb2s.0p25.f{n:03}" for n in np.arange(0,self.hour_end+1,self.freq)]
        self.sprd_fhour_links = [f"{new_link}file={self.ensemble_dict['sprd']}.t00z.pgrb2s.0p25.f{n:03}" for n in np.arange(0,self.hour_end+1,self.freq)]

    def watch(self):
        assert self.monitor, 'monitor not enabled, set monitor to True'
        self.most_recent_monitor()

    # def ftp_change(self, ftp_dir='./'):
    #     ls_prev = set()
    #     while True:
    #         ftp = self.ftp_login(sleep_time=self.monitor_interval)
    #         logging.info(ftp.lastresp)
    #         ftp.cwd(ftp_dir)
    #         ftp.cwd(self.most_recent_dir(ftp))
    #         ls = set(ftp.nlst())
    #         ftp.quit()
    #         add, rem = ls-ls_prev, ls_prev-ls
    #         if add or rem: yield add, rem, ftp
    #         ls_prev = ls
    #         sleep(self.monitor_interval)

    # def notify_changes(self):
    #     for add, rem, ftp in self.ftp_change('pub/data/nccf/com/gens/prod'):
    #         datenow = datetime.now().strftime('%Y %m %d %H:%m')
    #         logging.info(f'{datenow}')
    #         if len(add) > 0:
    #             logging.info('\n'.join('+ %s' % str(i) for i in add))
    #             self.link_builder()
    #         if len(rem) > 0:
    #             logging.info('\n'.join('- %s' % str(i) for i in rem))
    #         sleep(self.monitor_interval)
    #     datenow = datetime.now().strftime('%Y %m %d %H:%m')
    #     logging.info(f'exiting script at {datenow}')
    

        

    
# def loop_and_download(stat, fhours, ftp, http=True, query_dict={}):
#     assert stat in ['mean','sprd']; 'stat must be mean or sprd'
#     if stat == 'mean':
#         gestat = 'geavg'
#         save_name = 'mean'
#     file_valid_list = [n for n in file_list if np.logical_and('geavg' in n, '.idx' not in n)]
#     file_valid_list = [n for n in file_valid_list if np.logical_and(int(n[-3:]) <= 168, int(n[-3:]) % 6 == 0)]
#     file_valid_list = [n for n in file_valid_list if '011ab' not in n]
#     for n in file_valid_list:
#         fname = 'gefs_mean_'+n[-3:]+'.grib2'
#         try:
#             with open(fname, 'wb') as f:
#                 # Define the callback as a closure so it can access the opened 
#                 # file in local scope
#                 def callback(data):
#                     f.write(data)
#                 ftp.retrbinary(f'RETR {fname}', callback)
#         except Exception as e:
#             logging.error(f'{n} failure, {e}')
#             pass

# def retr_files(add, ftp, ftpdir='/'):
#     ftp = ftp_login()
#     ftp.cwd(ftpdir)
#     import pdb; pdb.set_trace()
#     while True:
#         try:
#             ftp.cwd(most_recent_dir(ftp))
#         except AttributeError:
#             continue
#     if 'atmos' in ftp.nlst():
#         ftp.cwd('atmos')
#     if 'pgrb2sp25' in ftp.nlst():
#         ftp.cwd('pgrb2sp25')
#     assert 'pgrb2sp25' in ftp.pwd(); f'not in file directory, currently in {ftp.pwd()}'
#     file_list = ftp.nlst()
#     file_valid_list = [n for n in file_list if np.logical_and('geavg' in n, '.idx' not in n)]
#     file_valid_list = [n for n in file_valid_list if np.logical_and(int(n[-3:]) <= 168, int(n[-3:]) % 6 == 0)]
#     file_valid_list = [n for n in file_valid_list if '011ab' not in n]
#     for n in file_valid_list:
#         fname = 'gefs_mean_'+n[-3:]+'.grib2'
#         try:
#             with open(fname, 'wb') as f:
#                 # Define the callback as a closure so it can access the opened 
#                 # file in local scope
#                 def callback(data):
#                     f.write(data)
#                 ftp.retrbinary(f'RETR {fname}', callback)
#         except Exception as e:
#             logging.error(f'{n} failure, {e}')
#             pass
#     file_valid_list = [n for n in file_list if np.logical_and('gespr' in n, '.idx' not in n)]
#     file_valid_list = [n for n in file_valid_list if np.logical_and(int(n[-3:]) <= 168, int(n[-3:]) % 6 == 0)]
#     for n in file_valid_list:
#         fname = 'gefs_sprd_'+n[-3:]+'.grib2'
#         try:
#             ftp.retrbinary(f"RETR {n}", open(temp_store + fname, 'wb').write)
#         except:
#             with open(log_directory + 'retrieval_log.txt', "a") as f:
#                 f.write(n + ' failure at ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'\n')
#             break
#     with open(log_directory + 'retrieval_log.txt', "a") as f:
#         f.write('completed at ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'\n')

# if __name__ == '__main__':
#     notify_changes()

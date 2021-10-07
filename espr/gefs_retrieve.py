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
import asyncio
from espr import utils 
import aiohttp
import aiofiles
import requests
import click

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
        hour_end: int=168,
        download: bool=False,
        download_dir: str=None):

        self.variable = variable.upper()
        assert self.variable in self.variable_store(), f'must be one of {self.variable_store()}'
        self.store = store
        self.monitor = monitor
        self.monitor_interval = monitor_interval
        self.latitude_bounds = latitude_bounds
        self.longitude_bounds = longitude_bounds
        self.freq = freq
        self.hour_end = hour_end
        self.sem = 10
        self.download = download
        self.download_dir = download_dir
        if not self.download_dir:
            self.download_dir = os.getcwd()
        
    def __str__(self):
        return self.variable
    
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

    def return_most_recent_links(self, stat, monitor=False):
        assert stat in ['ens', 'mean', 'sprd'], 'stat must be ens, mean, or sprd'
        base_url = 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/gens/prod/'
        atmos_pgrb = 'atmos/pgrb2sp25/'
        if monitor:
            self._most_recent_monitor(base_url, atmos_pgrb, stat)
        else:
            date_base_set = set()
            level, changes_date_in, _ = self.request_to_bs4(base_url, date_base_set)
            modelhour_base_set = set()
            if changes_date_in:
                self.date_value = max(changes_date_in)
                level, changes_model_hour_in, _ = self.request_to_bs4(level, modelhour_base_set)  
                if changes_model_hour_in: 
                    self.hour_value = max(changes_model_hour_in)
                    self.link_builder()
            file_exists = utils.req_status_bool(self.mean_fhour_links[-1])
            if file_exists:
                pass
            elif changes_model_hour_in == '18/':
                self.hour_value = '12/'
                self.link_builder()
            elif changes_model_hour_in == '00/':
                self.date_value = sorted(changes_date_in)[-2]
                self.hour_value = '18/'
                self.link_builder()
            if self.download:
                self.download_files_async(stat)
            if stat == 'ens':
                return self.ens_fhour_links
            elif stat == 'mean':
                return self.mean_fhour_links
            elif stat == 'sprd':
                return self.sprd_fhour_links


    def _most_recent_monitor(self, base_url: str, atmos_pgrb: str, stat: str):
        date_base_set = set()
        model_hour_base_set = set()
        while True:
            level_date, changes_date_in, changes_date_out = self.request_to_bs4(base_url, date_base_set)
            if changes_date_in or changes_date_out:
                changes_date = [n for n in [changes_date_in, changes_date_out] if n][0]
                level_hour, changes_model_hour_in, changes_model_hour_out = self.request_to_bs4(level_date, model_hour_base_set)  
                if changes_model_hour_in or changes_model_hour_out:
                    changes_model_hour = [n for n in [changes_model_hour_in, changes_model_hour_out] if n][0] 
                    self.hour_value = sorted(changes_model_hour)
                    self.link_builder()
                    file_exists = utils.req_status_bool(self.mean_fhour_links[-1])
                    if self.download:
                        self.download_files_async(stat)
            if file_exists:
                pass
            else:
                if changes_model_hour == '00/':
                    self.date_value = sorted(changes_date)[-2]
                    self.hour_value = '18/'
                    self.link_builder()
                else:
                    self.hour_value = f'{int(changes_model_hour.split("/")[0])-6:02}/'
                    self.link_builder()

            sleep(self.monitor_interval)

    def request_to_bs4(self, url, in_set):
        page = requests.get(url).text
        soup = BeautifulSoup(page, 'html.parser')
        links = set([a['href'] for a in soup.find_all('a',href=True)])
        try:
            most_recent = max(links)
        except ValueError:
            print('error in response, no links present')
        recursion_url = f'{url}{most_recent}'
        changes_in = links - in_set
        changes_out = in_set - links
        return recursion_url, changes_in, changes_out

    def link_builder(self):
        self.build_query_dict()
        self.build_ensemble_dict()
        base_link = 'https://nomads.ncep.noaa.gov/cgi-bin/filter_gefs_atmos_0p25s.pl?'
        base_query = f"{base_link}{''.join([f'{self.query_dict[n]}' for n in self.query_dict])}subregion=&"
        date_link = f'dir=%2F{self.date_value.strip("/")}%2F{self.hour_value.strip("/")}%2Fatmos%2Fpgrb2sp25&'
        new_link = base_query + date_link
        self.ens_fhour_links = [f"{new_link}file={self.ensemble_dict['ensembles'][m]}.t{self.hour_value.strip('/')}z.pgrb2s.0p25.f{n:03}" for n in np.arange(0,self.hour_end+1,self.freq) for m in self.ensemble_dict['ensembles']]
        self.mean_fhour_links = [f"{new_link}file={self.ensemble_dict['mean']}.t{self.hour_value.strip('/')}z.pgrb2s.0p25.f{n:03}" for n in np.arange(0,self.hour_end+1,self.freq)]
        self.sprd_fhour_links = [f"{new_link}file={self.ensemble_dict['sprd']}.t{self.hour_value.strip('/')}z.pgrb2s.0p25.f{n:03}" for n in np.arange(0,self.hour_end+1,self.freq)]

    def watch(self):
        assert self.monitor, 'monitor not enabled, set monitor to True'
        self.most_recent_monitor()

    async def download_link(self, link):
        async with aiohttp.ClientSession() as session:
                async with session.get(link, timeout=0) as resp:
                    if resp.status < 400: 
                        content = await resp.read()    
                        if sys.getsizeof(content) < 100:
                            await asyncio.sleep(1)
                            content = await resp.read()       
                        with open(f'{self.download_dir}/{link.split("=")[-1]}', mode='+wb') as f:
                            f.write(content)

    async def download_links(self, stat):
        links = self.return_most_recent_links(stat)
        coro = [self.download_link(link) for link in links]
        await utils.gather_with_concurrency(self.sem, *coro)

    def download_files_async(self, stat):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.download_links(stat))

@click.command()
@click.option(
    "-m",
    "--monitor",
    default='n',
    help="Whether to run the script persistently or not."
)
@click.option(
    "-s",
    "--stat",
    default='mean',
    help="The files to watch for, ens, mean, or sprd."
)
@click.option(
    "-d",
    "--download",
    default='n',
    help="Whether to download files or not."
)
@click.option(
    "--dir",
    default='~/',
    help="Where files will be downloaded."
)
def cli_main(monitor, stat, download, dir):
    monitor = utils.str_to_bool(monitor)
    download = utils.str_to_bool(download)
    assert stat in ['ens', 'mean', 'sprd'], 'stat must be ens, mean, or sprd'
    retr = GEFSRetrieve(download_dir=dir)
    retr.return_most_recent_links(stat, monitor=monitor, download=download)

if __name__ == '__main__':
    cli_main()

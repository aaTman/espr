import subprocess
import pandas as pd 
import boto3
import xarray as xr
import urllib 
import os
import re
from datetime import datetime, timedelta
import numpy as np

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
bucket = 'noaa-gefs-retrospective'
vnames = ['dswrf_sfc_'] # ,'apcp_sfc_']'hgt_pres_abv700mb_','pres_msl_','tmp_pres_','pwat_eatm_', 'ugrd_hgt_','vgrd_hgt_' add in later
ens = ['c00','p01','p02','p03','p04']
s3 = boto3.client('s3')

if __name__ == '__main__':
    dr = date_range_seasonal('jja')

    for varname in vnames:
        for year in range(2000,2020):
            dr_str = [day.strftime('%Y%m%d00') for day in seasons[season]]
            out = s3.list_objects_v2(Bucket='noaa-gefs-retrospective',Delimiter='/',Prefix=f'GEFSv12/reforecast/{year}/',MaxKeys=370)
            valid_files = [m['Prefix'][-11:-1] for m in out['CommonPrefixes'] if m['Prefix'][-11:-1] in dr_str]
            source_links = [f'{source}{year}/{m}/' for m in valid_files]
            source_links_ens = [f'{o}{m}/Days:1-10/{varname}{o[-11:-1]}_{m}.grib2' for o in source_links for m in ens]
            i=0
            for link in source_links_ens:
                
                if f'nh_time_{link[98:-10]}_mean.nc' in os.listdir('mean'):
                    # print(f'nh_time_{link[98:-10]} already completed')
                    pass
                else:
                    ens_num = ens[i%5]
                    if f'{link[98:]}' in os.listdir():
                        # print(f'{link[98:]} already downloaded')
                        pass
                    else:
                        urllib.request.urlretrieve(link, f'{link[98:]}')
                    subprocess.run(['grib_to_netcdf', '-k', '4', '-d1', '-s', '-o', f'{link[98:-6]}.nc', f'{link[98:]}'])
                    if f'{link[98:-6]}.nc' in os.listdir('tmp'):
                        pass
                    elif f'{link[98:-6]}.nc' in os.listdir():
                        cds = xr.open_dataset(f'{link[98:-6]}.nc',chunks={})
                        comp = dict(zlib=True, complevel=5, shuffle=True)
                        encoding = {var: comp for var in cds.data_vars}
                        date_init = np.datetime64(datetime.strptime(link[-20:-10], '%Y%m%d00'))
                        timedeltas = pd.to_timedelta(cds.time - date_init)
                        timedeltas_7day = timedeltas[timedeltas <= '7 days 00:00:00']
                        if 'height' in varname:
                            try:
                                cds = cds.sel(isobaricInhPa=500)
                            except:
                                cds = cds.sel(level=500)
                        elif 'tmp_pres' in varname:
                            try:
                                cds = cds.sel(isobaricInhPa=850)
                            except:
                                cds = cds.sel(level=850)
                        cds = cds.isel(time = slice(0,len(timedeltas_7day)))
                        cds = cds.sel(latitude=slice(60,0))
                        cds = cds.expand_dims('member')
                        cds.to_netcdf(f'tmp/{link[98:-6]}.nc',encoding=encoding)
                    else:
                        pass
                    i+=1
                    if ens_num == 'p04':
                        lorg = xr.open_mfdataset('tmp/*.nc',combine='nested',concat_dim='member',data_vars='minimal',coords='minimal',compat='override')
                        lorg_mean = lorg.mean('member')
                        lorg_mean = lorg_mean.expand_dims('date')
                        lorg_std = lorg.std('member')
                        lorg_std = lorg_std.expand_dims('date')
                        comp = dict(zlib=True, complevel=5, shuffle=True)
                        encoding = {var: comp for var in lorg.data_vars}
                        lorg_mean.to_netcdf(f'mean/nh_time_{link[98:-10]}_mean.nc',encoding=encoding)
                        # subprocess.run(['nccopy', '-u', '-s', '-d2', f'mean/nh_time_{link[98:-10]}_mean.nc',f'mean/nh_time_{link[98:-10]}_mean_d2.nc'])
                        lorg_std.to_netcdf(f'std/nh_time_{link[98:-10]}_std.nc',encoding=encoding)
                        # subprocess.run(['nccopy', '-u', '-s', '-d2', f'std/nh_time_{link[98:-10]}_std.nc',f'std/nh_time_{link[98:-10]}_std_d2.nc'])
                        [os.remove(fname) for fname in os.listdir() if bool(re.search(r'\d.', fname))]
                        [os.remove(f'tmp/{fname}') for fname in os.listdir('tmp') if bool(re.search(r'\d.', fname))]
                        print(f'{link[98:-10]} finished')
            print(f'finished {year}')
        for folder in ['mean','std']:
            if f'{link[98:-21]}_nh_{season}_{folder}.nc' in os.listdir(f'{folder}'):
                pass
            else:
                big_xr = xr.open_mfdataset(f'{folder}/nh_time_{link[98:-21]}*',engine='netcdf4')
                big_xr.to_netcdf(f'{folder}/{link[98:-21]}_nh_{season}_{folder}.nc')
            if f'{link[98:-21]}_nh_{season}_{folder}_d1.nc' in os.listdir(f'{folder}'):
                pass
            else:
                subprocess.run(['nccopy', '-u', '-s', '-d1', f'{folder}/{link[98:-21]}_nh_{season}_{folder}.nc', f'{folder}/{link[98:-21]}_nh_{season}_{folder}_d1.nc'])
            




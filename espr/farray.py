import numpy as np 
import xarray as xr
import xarray.ufuncs as xu
import bottleneck
import datetime
import os
import paths as ps
import utils as ut
import plot
import subprocess
import logging

class ForecastArray:
    """
    Forecast grib/netcdf object.
    This class instantiates metadata and loads data
    for a forecast xarray dataset.

    Note: There must be a folder specified with historical forecasts
    available. Default name is "reforecast" for folder
    Parameters
    ---------
    date : string or datetime.date
        The date of the model run. Hour is not relevant as the 
        reforecast is only run once daily.
    path : string
        The absolute path for the location of reforecast data.
    variable : string
        The variable of interest. Can be slp, pwat, tmp925, tmp850,
        or wnd (surface) currently. Other short names will be 
        accepted for the most part.
    fhour : int
        Forecast hour of the model run; each MClimate object will
        be unique to each forecast hour.
    percentage : float
        The percentage of the MClimate distribution to subset at 
        each point, e.g. 10 will take 5% of MClimate values below 
        and above the model run's value. Default is 10%.
    period : int
        The length of the period which the forecast xarray will be 
        centered around (e.g. +-10 days about forecast date).
        Default is 10.
        
    """
    def __init__(self, stat: str, path: str, variable: str, fhour: int, group: bool=False):
        
        
        
        self.variable = self.convert_variable(variable)
        self.fhour = fhour
        self.stat = stat
        if self.stat in self._stat_list():
            pass
        else:
            self._convert_stat()
        if group == True:
            self.load_all()

    def convert_variable(self, in_var):
        if in_var in ['slp','psl','prmsl']:
            in_var = 'prmsl'
            self.key_filter = {'typeOfLevel':'meanSea'}
        elif in_var in ['precip','pwat']:
            in_var = 'pwat'
            self.key_filter = {'typeOfLevel':'unknown', 'level': 0}
        elif in_var in ['temp','tmp','tmp850','tmp925']:
            self.short_name = 't'
            if '925' in in_var:
                self.key_filter = {'typeOfLevel':'isobaricInhPa','level': 925, 'shortName': 't'}
                in_var = 'tmp925'
            elif '850' in in_var:
                self.key_filter = {'typeOfLevel':'isobaricInhPa','level': 850, 'shortName': 't'}
                in_var = 'tmp850'
            else:
                raise Exception('Temperature level must be indicated (925 or 850)')
        elif in_var in ['wnd', 'wind', 'sfc_wind', '10m_wnd', 'u10', 'v10']:
            in_var = 'wnd'
            self.key_filter = {'typeOfLevel': 'heightAboveGround', 'level': 10}
        return in_var

    def _convert_stat(self):
        if self.stat in {'avg','mu'}:
            self.stat = 'mean'
        elif self.stat in {'std', 'sigma', 'spread'}:
            self.stat = 'sprd'
    
    def _var_list(self):
        return ['prmsl','pwat','tmp','wnd']

    def _map(self, data):
        if in_var == 'prmsl':
            data = data.rename({'prmsl':'Pressure'})
        elif in_var == 'pwat':
            data = data.rename({'pwat':'Precipitable_water'})
        return data

    def _stat_list(self):
        return ['sprd', 'mean']
    
    def _get_var(self, data):
        if in_var == 'wnd':
            subset_variable = xu.sqrt(data[[n for n in data.data_vars][0]]**2+data[[n for n in data.data_vars][1]]**2)
            subset_variable = subset_variable.drop(['heightAboveGround'])
        
        elif in_var == 'tmp925':
            subset_variable = data['t'] - 273.15
        elif in_var == 'tmp850':
            subset_variable = data['t'] - 273.15
        elif in_var == 'pwat':
            subset_variable = data.drop(['level'])
        else:
            subset_variable = data
        return subset_variable
        
    def _subset_latlon(self, data, lats, lons):
        data = data.where(np.logical_and(data.lon>=np.min(lons), data.lon<=np.max(lons)), drop=True)
        data = data.where(np.logical_and(data.lat>=np.min(lats), data.lat<=np.max(lats)), drop=True)
        return data
        
    def _rename_latlon(self, forecast):
        try:
            forecast = forecast.rename_dims({'latitude':'lat','longitude':'lon'}).rename_vars({'latitude':'lat','longitude':'lon'})
        except AttributeError:
            forecast = forecast.rename({'latitude':'lat','longitude':'lon'})
        return forecast
  
    def load_forecast(self, subset_lat=None, subset_lon=None):
        try:
            new_gefs = xr.open_dataset(f'{ps.data_store}gefs_{self.stat}_{self.fhour:03}.grib2',engine='cfgrib',backend_kwargs=dict(filter_by_keys=self.key_filter,indexpath=''))
        except KeyError:
            new_gefs = xr.open_dataset(f'{ps.data_store}gefs_{self.stat}_{self.fhour:03}.grib2',engine='cfgrib',backend_kwargs=dict(filter_by_keys=self.key_filter,indexpath=''))
        subset_gefs = self._get_var(new_gefs)
        subset_gefs = self._rename_latlon(new_gefs)
        try:
            subset_gefs = self._subset_latlon(subset_gefs, subset_lat, subset_lon)
        except:
            print('error trying to subset lats and lons')
            pass
        self.date = str(subset_gefs.time.values).partition('T')[0]
        subset_gefs = self._map(subset_gefs)
        return subset_gefs

    def load_all(self, subset_lat=None, subset_lon=None):
        try:
            new_gefs = xr.open_mfdataset(f'{ps.data_store}*{self.stat}*.grib2',
            engine='cfgrib',
            combine='nested',
            concat_dim='time',
            backend_kwargs=dict(filter_by_keys=self.key_filter,indexpath='')
            )
        except KeyError:
            import cfgrib
            new_gefs = cfgrib.open_datasets(f'{ps.data_store}gefs_mean_000.grib2')
        subset_gefs = self._get_var(new_gefs)
        subset_gefs = self._rename_latlon(new_gefs)
        try:
            subset_gefs = self._subset_latlon(subset_gefs, subset_lat, subset_lon)
        except:
            print('error trying to subset lats and lons')
            pass
        self.date = str(subset_gefs.time.values).partition('T')[0]
        subset_gefs = self._map(subset_gefs)
        return subset_gefs
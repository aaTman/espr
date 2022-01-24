import numpy as np 
import xarray as xr
import xarray.ufuncs as xu
from . import utils as ut
import os
import glob

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

    def __init__(self, stat: str, variable: str, group: bool=False):
        self.variable = self.convert_variable(variable)
        if stat == 'mean':
            stat = 'avg'
        elif stat == 'sprd' or stat == 'std':
            stat = 'spr'
        else:
            raise ValueError('Stat must be mean or sprd/std')
        self.stat = stat
        self.paths = ut.load_paths()
        # if self.stat in self._stat_list():
        #     pass
        # else:
        #     self._convert_stat()
        if group == True:
            self.load_all()

    def convert_variable(self, variable):
        if variable in ['slp','psl','prmsl']:
            self.in_var = 'prmsl'
            self.key_filter = {'typeOfLevel':'meanSea'}
        elif variable in ['precip','pwat']:
            self.in_var = 'pwat'
            self.key_filter = {'typeOfLevel':'unknown', 'level': 0}
        elif variable in ['temp','tmp','tmp850','tmp925']:
            self.short_name = 't'
            if '925' in variable:
                self.key_filter = {'typeOfLevel':'isobaricInhPa','level': 925, 'shortName': 't'}
                self.in_var = 'tmp925'
            elif '850' in variable:
                self.key_filter = {'typeOfLevel':'isobaricInhPa','level': 850, 'shortName': 't'}
                self.in_var = 'tmp850'
            else:
                raise Exception('Temperature level must be indicated (925 or 850)')
        elif variable in ['wnd', 'wind', 'sfc_wind', '10m_wnd', 'u10', 'v10']:
            self.in_var = 'wnd'
            self.key_filter = {'typeOfLevel': 'heightAboveGround', 'level': 10}
        return self.in_var

    # def _convert_stat(self):
    #     if self.stat in {'avg','mu'}:
    #         self.stat = 'mean'
    #     elif self.stat in {'std', 'sigma', 'spread'}:
    #         self.stat = 'sprd'
    
    def _var_list(self):
        return ['prmsl','pwat','tmp','wnd']

    def _map(self, data):
        if self.in_var == 'prmsl':
            data = data.rename({'prmsl':'Pressure'})
        elif self.in_var == 'pwat':
            data = data.rename({'pwat':'Precipitable_water'})
        return data

    def _stat_list(self):
        return ['sprd', 'mean']
    
    def _get_var(self, data):
        if self.in_var == 'wnd':
            subset_variable = xu.sqrt(data[[n for n in data.data_vars][0]]**2+data[[n for n in data.data_vars][1]]**2)
            subset_variable = subset_variable.drop(['heightAboveGround'])
        
        elif self.in_var == 'tmp925':
            subset_variable = data['t'] - 273.15
        elif self.in_var == 'tmp850':
            subset_variable = data['t'] - 273.15
        elif self.in_var == 'pwat':
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
            flist = [n for n in glob.glob(f'{self.paths["data_store"]}*') if self.stat in n and '.idx' not in n]
            try:
                new_gefs = xr.open_mfdataset(flist,
                engine='cfgrib',
                combine='nested',
                concat_dim='time',
                backend_kwargs=dict(filter_by_keys=self.key_filter,indexpath='')
                )
            except OSError:
                raise FileNotFoundError('sprd files likely not in download folder, please check!')
        except KeyError:
            import cfgrib
            new_gefs = cfgrib.open_datasets(f'{self.paths["data_store"]}gefs_mean_000.grib2')
        subset_gefs = self._get_var(new_gefs)
        subset_gefs = self._rename_latlon(new_gefs)
        if subset_lat is not None and subset_lon is not None:
            subset_gefs = self._subset_latlon(subset_gefs, subset_lat, subset_lon)
        else:
            pass
        self.date = str(subset_gefs.time.values).partition('T')[0]
        subset_gefs = self._map(subset_gefs)
        return subset_gefs
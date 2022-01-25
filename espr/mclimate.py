import numpy as np

import xarray as xr
import xarray.ufuncs as xu

import datetime
import os
import utils as ut

import typing
from dask.distributed import Client
import dateutil.parser as dparser

class MClimate:
    """
    Model climatology object.
    This class instantiates metadata for an MClimate xarray 
    object that can be produced with MClimate.generate().

    Note: There must be a folder specified with historical forecasts
    available.
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
    v12 : bool
        Whether or not the data is from the v12 reforecast. The v12
        reforecast is structured with individual files for each date
        as opposed to one large netcdf due to its storage on AWS.
    """
    def __init__(self,
                date, 
                path: str, 
                variable: str, 
                fhour: int = 24, 
                percentage: float = 10,
                period: int = 10,
                v12: bool = False):
        if isinstance(date, datetime.date) or isinstance(date, datetime.datetime):
            self.date = date
        else:
            try:
                self.date = dparser.parse(date,fuzzy=True)
            except:
                raise Exception('Please enter date similar to yyyy-mm-dd')
        self.variable = variable
        if self.variable in self.var_list():
            pass
        else:
            self._convert_variable()
        self.fhour = fhour
        self.percentage = percentage
        self.path = path
        self.period = period
        self.v12 = v12

    def var_list(self) -> list:
        return ['slp','pwat','tmp925','tmp850','wnd', 'tcc', 'dswrf']
    
    def _convert_variable(self):
        if 'slp' or 'psl' in self.variable:
            self.variable = 'slp'
        if 'precip' in self.variable:
            self.variable = 'pwat'
        if 'temp' or 'tmp' in self.variable:
            if '925' in self.variable:
                self.variable = 'tmp925'
            elif '850' in self.variable:
                self.variable = 'tmp850'
            else:
                raise Exception('Temperature level must be indicated (925 or 850 for now)')
        elif 'wind' in self.variable:
            self.variable = 'wnd'
         
    def date_string(self) -> str:
        djf = [1, 2, 12]
        mam = [3, 4, 5]
        jja = [6, 7, 8]
        son = [9, 10, 11]
        if self.date.month in djf:
            return 'djf'
        if self.date.month in mam:
            return 'mam' 
        if self.date.month in jja:
            return 'jja' 
        if self.date.month in son:
            return 'son'

    def subset_time(self):
        '''
        Subsets the date range to within 21 days +- the valid date.
        This method is ~ 3x faster than using an xr.concat method which
        loops through the date_range variable to select valid dates.
        Returns the subset Dataset.
        Parameters
        ---------
        ds : xarray.dataset
            The dataset to subset the time range upon.
        '''
        d64 = np.datetime64(self.date,'D')
        date_range = ut.replace_year(np.arange(d64-self.period,d64+self.period+1), 2012)
        days = date_range - date_range.astype('datetime64[M]') + 1
        months = date_range.astype('datetime64[M]').astype(int) % 12 + 1
        years = np.arange(2000,2020).astype(str)
        centered_date_str = np.char.add(np.array([n.zfill(2) for n in months.astype(int).astype(str)]).T,
                                            np.array([n.zfill(2) for n in days.astype(int).astype(str)]).T)
        if self.v12:
            full_date_list = np.array([n+m for n in years for m in centered_date_str]).T
            stat_list = [n for n in os.listdir(self.path) if self.stat in n]
            subset_stat_list = [n for n in stat_list if any(m in n for m in full_date_list)]
            subset_mean_list_full_path = [f'{self.path}/{n}' for n in subset_stat_list]
            return subset_mean_list_full_path
        else:
            return centered_date_str
    
    def set_data_path(self, stat: str, custom: typing.Optional[str] = None):
        '''
        Generates the path for variables. Default is 
        <variable>_<stat>_<date_string> where <variable> is
        slp, wnd, etc. <stat> is mean or sprd, <date_string> is
        djf, mam, jja, son.
        Parameters
        ---------
        stat : str
            The stat (mean, sprd).
        custom: str
            If custom path desired, enter path here (fstrings included).
        '''
        if self.v12:
            return self.subset_time()
        if custom is not None:
            return custom
        else:
            if self.variable == 'wnd':
                return f'{self.path}/*{self.variable}_{stat}_{self.date_string()}.nc'
            else:
                return f'{self.path}/{self.variable}_{stat}_{self.date_string()}.nc'
    
    def open_xr_dataset(self, data_path: str, arg_dict: dict):
        if self.v12:
            ds = xr.open_mfdataset(data_path, **arg_dict)
            return ds
        else:
            return xr.open_dataset(data_path, chunks='auto')
   
    def retrieve_from_xr(self, stat: str='mean', subset_fhour: bool=False):
        arg_dict = {}
        assert stat in ['mean','sprd'], 'stat must be mean or sprd'
        data_path = self.set_data_path(stat)
        if self.v12:
            arg_dict['combine'] = 'nested'
            arg_dict['concat_dim']='date'
            arg_dict['coords']='minimal'
            arg_dict['compat']='override'
        if self.variable == 'wnd':
            arg_dict['combine'] = 'by_coords'
            ds = self.open_xr_dataset(data_path, arg_dict) 
            ds = np.sqrt(ds[[n for n in ds.data_vars][0]]**2+ds[[n for n in ds.data_vars][1]]**2)
        elif self.variable == 'tmp925':
            self.variable = 'tmp'
            ds = self.open_xr_dataset(data_path, arg_dict) 
            ds = ds.sel(pressure=925)
            ds = ds.drop(['pressure'])
        elif self.variable == 'tmp850':
            self.variable = 'tmp'
            ds = self.open_xr_dataset(data_path, arg_dict) 
            ds = ds.sel(pressure=850)
            ds = ds.drop(['pressure'])
        else:
            ds = self.open_xr_dataset(data_path, arg_dict)
        if subset_fhour:
            try:
                ds = ds.sel(fhour=np.timedelta64(self.fhour,'h'))
            except KeyError:
                ds = ds.sel(fhour=self.fhour)

        # year_values = ds.time.values.astype('datetime64[Y]').astype(int) + 1970
        if self.v12:
            pass
        else:
            date_ = self.subset_time()
            months =  ds.time.values.astype('datetime64[M]').astype(int) % 12 + 1
            days = (ds.time.values.astype('datetime64[D]') - ds.time.values.astype('datetime64[M]')).astype(int) + 1
            ds_timestr = np.char.add(np.array([n.zfill(2) for n in months.astype(int).astype(str)]).T,
                                            np.array([n.zfill(2) for n in days.astype(int).astype(str)]).T)
            ds = ds.assign_coords(timestr=('time', ds_timestr))
            ds = ds.where(ds.timestr.isin(date_),drop=True)
        # if self.fhour:
        #     ds = ds.drop(['intTime', 'intValidTime', 'fhour'])
        # else:
        #     ds = ds.drop(['intTime', 'intValidTime'])
        # if self.variable == 'wnd':
        #     ds = xu.sqrt(ds[[n for n in ds.data_vars][0]]**2+ds[[n for n in ds.data_vars][1]]**2)
        return ds

    def generate(self,stat: str='mean', load: bool=False, subset_fhour: bool=False):
        '''
        Generates the model climatology given the forecast hour specified.
        Parameters
        ---------
        stat : str
            mean or sprd, the two statistics to generate model climatology on.
        dask : bool
            Determines if dask will be used when loading the xarray file.
        load : bool
            If true, will load xarray into memory. Only use if there is 
            sufficient memory to handle the netcdf.
        '''
        self.stat = stat
        xarr = self.retrieve_from_xr(self.stat, subset_fhour)
        if load:
            return xarr.load()
        else:   
            return xarr
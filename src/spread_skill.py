import xarray as xr
import numpy as np

def xarr_std(x):
    return (x - x.mean()) / x.std()

def time_valid_errors(x, x_obs):
    x['date'] = x['date']+x['time'].astype("timedelta64[h]")
    x = x.rename({'date':'valid_time'}).transpose("valid_time","latitude", "longitude") 
    return np.abs(x.msl - x_obs)

class stats:
    def __init__(self, ds, path, default_obs=True):
        self.ds = ds
        var_path = [n for n in ds.data_vars][0]
        self.path = path
        if default_obs:
            self.set_obs_path(f'/home/taylorm/espr/analysis/{var_path}.nc')
        self.swap_time_dim()
        self.swap_obs_time_dim()
        self.obs_subset()

    def swap_time_dim(self,original_dim='step',new_dim='valid_time'):
        self.ds = self.ds.swap_dims({'step':'valid_time'})

    def swap_obs_time_dim(self,original_dim='time',new_dim='valid_time'):
        self.obs = self.obs.rename({'time':'valid_time'})
    
    def set_obs_path(self, path, load=True, dask=True):
        self.obs_path = path
        if load:
            if dask:
                self.obs = xr.open_dataset(self.obs_path,chunks='auto')
            else:
                self.obs = xr.open_dataset(self.obs_path)

    def obs_subset(self):
        self.obs = self.obs.where(self.obs.valid_time.isin([self.ds.valid_time]),drop=True)

    def range(self,dim='number'):
        return self.ds.max(dim=dim) - self.ds.min(dim=dim)

    def valid_sample_space(self,dim='number'):
        return np.logical_and(self.obs<=self.ds.max(dim='number'),self.obs>=self.ds.min(dim='number'))

    
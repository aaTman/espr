import xarray as xr
import numpy as np

def xarr_std(x):
    return (x - x.mean()) / x.std()

def time_valid_errors(x, x_obs):
    x['date'] = x['date']+x['time'].astype("timedelta64[h]")
    x = x.rename({'date':'valid_time'}).transpose("valid_time","latitude", "longitude") 
    return np.abs(x.msl - x_obs)

class stats:
    def __init__(self, ds, path, default_obs=True, save=True,run_all=True):
        self.ds = ds
        self.ds_var = [n for n in ds][0]
        self.path = path
        if default_obs:
            self.set_obs_path(f'/home/taylorm/espr/analysis')
            self.obs_var = [n for n in self.obs][0]
        self.swap_time_dim()
        self.swap_obs_time_dim()
        self.obs_subset()
        if run_all:
            self.valid_sample_space(save=save)

    def swap_time_dim(self,original_dim='step',new_dim='valid_time'):
        self.ds = self.ds.swap_dims({'step':'valid_time'})

    def swap_obs_time_dim(self,original_dim='time',new_dim='valid_time'):
        self.obs = self.obs.rename({'time':'valid_time'})
    
    def set_obs_path(self, path, load=True, dask=True):
        self.obs_path = path
        if load:
            if dask:
                self.obs = xr.open_dataset(f'{self.obs_path}/{self.ds_var}.nc',chunks='auto')
            else:
                self.obs = xr.open_dataset(f'{self.obs_path}/{self.ds_var}.nc')

    def obs_subset(self):
        if np.any(self.ds.longitude.values > 180):
            self.ds['longitude'] = (self.ds['longitude'] + 180) % 360 - 180
        self.obs = self.obs.where(self.obs['valid_time'].isin([self.ds['valid_time']]),drop=True)\
            .where(self.obs['latitude'].isin([self.ds['latitude']]),drop=True)\
                .where(self.obs['longitude'].isin([self.ds['longitude']]),drop=True)


    def range(self,dim='number'):
        return self.ds.max(dim=dim) - self.ds.min(dim=dim)

    def valid_sample_space(self,dim='number',save=True):
        total_gridpoints = self.ds['longitude'].shape[0]*self.ds['latitude'].shape[0]
        valid_grid = np.logical_and(self.obs[self.obs_var]<=self.ds[self.ds_var].max(dim='number'),self.obs[self.obs_var]>=self.ds[self.ds_var].min(dim='number'))
        valid_stat = valid_grid.sum(['latitude','longitude'])/total_gridpoints
        if save:
            valid_stat.to_netcdf(f"{self.obs_path}/stats/{str(self.ds['time'].values.astype('datetime64[D]'))}",compute=False)
        else:
            return valid_stat

    
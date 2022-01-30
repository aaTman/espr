import xarray as xr
import numpy as np
import bottleneck

def hsa(gefs_sprd, subset, debug=False):
    '''Standardizes, sets min and max between -1 and 1, and takes the arctanh to derive
    a "normal" distribution to ascribe more statistical relevance to the zscore values.
    
    Known as historical spread anomaly, or HSA.'''
    try:
        gefs_sprd = gefs_sprd.rename({'time':'fhour'})
    except:
        pass
    try:
        gefs_sprd = gefs_sprd.assign_coords(fhour=subset.fhour)
    except:
        pass
    subset_vals = (gefs_sprd['Pressure'] - subset.mean('time',skipna=True))/subset.std('time',skipna=True)
    new_stacked = xr.concat([subset.drop('timestr').to_dataset(),gefs_sprd.expand_dims('time')],'time')
    percentile = bottleneck.nanrankdata(new_stacked['Pressure'],axis=0)/np.count_nonzero(~np.isnan(new_stacked['Pressure'][:,0,0,0]))
    perc_ds = xr.Dataset(
    data_vars=dict( 
        spread_percentile=(["fhour","lat","lon"],percentile[-1])
        ), coords=dict( 
            lon=new_stacked.lon.values, 
            lat=new_stacked.lat.values, 
            fhour=new_stacked.fhour.values 
            ), 
            attrs=dict(
                description="Spread percentile based on reforecast\
                    of similar mean anomalies by gridpoint."), 
    )
    if debug:
        return subset_vals
    subset_vals = (0.99-(-0.99))*(subset_vals-subset_vals.min(['lat','lon']))/(subset_vals.max(['lat','lon'])-subset_vals.min(['lat','lon'])) + -0.99
    subset_vals = np.arctanh(subset_vals)
    return subset_vals, perc_ds

def xarr_interpolate(original, new):
    new_lat = [i for i in new.coords if 'lat' in i][0]
    new_lon = [i for i in new.coords if 'lon' in i][0]
    old_lat = [i for i in original.coords if 'lat' in i][0]
    old_lon = [i for i in original.coords if 'lon' in i][0]
    new_lat_vals = new[new_lat].values
    new_lon_vals = new[new_lon].values
    interpolated_ds = original.interp({old_lat:new_lat_vals})
    return interpolated_ds

def subset_sprd(percentile, mc_std):
    mask = np.logical_and(percentile >= percentile[-1]-0.05, percentile <= percentile[-1]+0.05)
    try:
        mc_std = mc_std[[n for n in mc_std][0]]
    except:
        pass
    # mc_std.rename({'fhour':'time','time':'fhour'})
    mask_da=xr.DataArray(mask[:-1], coords={
        'fhour':mc_std.fhour.values, 
        'time':mc_std.time.values, 
        'lat':mc_std.lat.values, 
        'lon':mc_std.lon.values 
        }, 
    dims={ 
        'time': len(mc_std.time), 
        'fhour':len(mc_std.fhour), 
        'lat': len(mc_std.lat), 
        'lon': len(mc_std.lon) 
        }
    )
    mc_std = mc_std.where(mask_da)
    return mc_std

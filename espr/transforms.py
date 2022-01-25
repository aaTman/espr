import xarray as xr
import numpy as np
import bottleneck

def hsa(gefs_sprd, subset):
    '''Standardizes, sets min and max between -1 and 1, and takes the arctanh to derive
    a "normal" distribution to ascribe more statistical relevance to the zscore values.
    
    Known as historical spread anomaly, or HSA.'''
    gefs_sprd = gefs_sprd.rename({'time':'fhour'})
    gefs_sprd = gefs_sprd.assign_coords(fhour=subset.fhour)
    subset_vals = (gefs_sprd - subset.mean('time'))/subset.std('time')
    subset_vals = (0.99-(-0.99))*(subset_vals-subset_vals.min(['lat','lon']))/(subset_vals.max(['lat','lon'])-subset_vals.min(['lat','lon'])) + -0.99
    subset_vals = np.arctanh(subset_vals)
    return subset_vals

def percentile(mclimate, forecast):
    try:
        forecast = forecast.where(forecast['step'].isin(mclimate['fhour']), drop=True)
    except ValueError:
        raise ValueError('forecast step value not the same as fhour, test incoming data')
    vars = ['step','meanSea','valid_time','isobaricInhPa', 'pressure', 'heightAboveGround','intTime','intValidTime']
    forecast = forecast.drop({'time'}).rename({'step':'fhour','time':'fhour'}).expand_dims('time')
    drop_vars = list(set(list(mclimate.coords) + list(mclimate)) - set(list(forecast.coords)+list(forecast)))
    try:
        forecast = forecast.drop(drop_vars)
    except ValueError:
        mclimate = mclimate.drop(drop_vars)
    try:
        new_stacked = xr.concat([mclimate[[n for n in mclimate][0]], forecast[[n for n in forecast][0]]],'time')
    except TypeError:
        new_stacked = xr.concat([mclimate, forecast[[n for n in forecast][0]]],'time')
    except ValueError:
        forecast = forecast.drop(['level'])
        new_stacked = xr.concat([mclimate[[n for n in mclimate][0]], forecast[[n for n in forecast][0]]],'time')
    percentile = bottleneck.rankdata(new_stacked,axis=0)/len(new_stacked['time'])
    return percentile

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
    mc_std  = mc_std.where(~np.isnan(mask_da),drop=True)

    return mc_std
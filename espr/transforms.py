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
    new_stacked = new_stacked.compute()
    percentile = bottleneck.rankdata(new_stacked,axis=0)/len(new_stacked['time'])
    return percentile

def xarr_interpolate(original, new):
    new_lat = [i for i in new.coords if 'lat' in i][0]
    new_lon = [i for i in new.coords if 'lon' in i][0]
    old_lat = [i for i in original.coords if 'lat' in i][0]
    old_lon = [i for i in original.coords if 'lon' in i][0]
    interpolated_ds = original.interp({
        old_lat : new[new_lat].values, 
        old_lon : new[new_lon].values
        })
    return interpolated_ds

def subset_sprd(combined_fcst_mcli, mcli_sprd):
    new_perc = combined_fcst_mcli.where(np.logical_and(combined_fcst_mcli >= combined_fcst_mcli.isel(time=-1)-0.05, combined_fcst_mcli <= combined_fcst_mcli.isel(time=-1)+0.05),drop=True)
    try:
        mcli_sprd = mcli_sprd[[n for n in mcli_sprd][0]]
    except:
        pass
    try:
        mcli_sprd = mcli_sprd.where(~np.isnan(new_perc[:-1]),drop=True)
    except ValueError:
        mcli_sprd = mcli_sprd.drop(['isobaricInhPa'])
        mcli_sprd = mcli_sprd.where(~np.isnan(new_perc[:-1]),drop=True)
    return mcli_sprd
from datetime import datetime

import numpy as np
import xarray as xr

from .core import GEFSLivePull, MClimate

"""
Putting methods here that generate these products so far:
1. probability matched mean
2. extreme forecast index
3. mclimate event cdf/quantile
4. shift of tails
5. % of time validating within dataset
6. historical spread anomaly
7. standardized spread anomaly
8. crps of m-climate? only if the context can be provided
9. dprog/dt analysis?
"""


class EnsembleProduct:
    """
    Products that are generated from ensemble forecasts and
    require all ensemble members in the dataset
    """

    def __init__(self, variable):
        self.gefs_live = pull_gefs(ensemble=True)
        self.variable = variable

    def probability_matched_mean(self):
        num_ensemble_members = len(self.gefs_live.ge_ens["number"])
        sorted_ranked_ensemble_mean = np.argsort(
            self.gefs_live.ge_avg[self.variable].flatten()
        )  # this is probably wrong
        sorted_index_ensemble_mean = np.argsort(sorted_ranked_ensemble_mean)
        sorted_ranked_ensemble_members = np.argsort(
            self.gefs_live.ge_spr[self.variable].flatten()
        )[
            0::num_ensemble_members
        ]  # might need to change the order of this to subset first
        probability_matched_mean = sorted_index_ensemble_mean[
            sorted_ranked_ensemble_members
        ][sorted_index_ensemble_mean]

        probability_matched_mean = probability_matched_mean.reshape(
            self.gefs_live.ge_avg[self.variable].shape
        )
        return probability_matched_mean

    ## old code used to pull from, will remove/is not used
    def pmm(ens):
        # Takes the mean of the ensemble
        ensMean = np.mean(ens, axis=1)

        # Sets the sorted index array for each forecast hour's ensemble mean values low to high
        ensMeanSort = np.array(
            [np.argsort(ensMean[i].flatten()) for i in range(0, len(ensMean))]
        )

        # Sets the index array of the sorted ensemble mean index array; in simple
        # terms this can be used to rearrange the data back to its original order
        ensMeanSortSort = np.array(
            [np.argsort(ensMeanSort[i]) for i in range(0, len(ensMeanSort))]
        )

        # Flattens the full ensemble by forecast hour
        ensFlat = [ens[i].flatten() for i in range(0, len(ens))]

        # Takes every 21st value to equal the size of the flattened mean array
        ensFlat = np.array([ensFlat[i][0::21] for i in range(0, len(ens))])

        # Sets the sorted index array from low to high for each forecast hour
        ensSort = np.array([np.argsort(ensFlat[i]) for i in range(0, len(ens))])

        # Replaces the ensemble mean values with the ensemble values, then returns
        # the values to the original index of the ensemble mean.
        enspmm = np.array(
            [ensFlat[i][ensSort[i]][ensMeanSortSort[i]] for i in range(0, len(ensSort))]
        )

        # Catch for errors, haven't had an issue so this might be useless.
        enspmm = enspmm.reshape((len(ensSort), len(ensMean[0]), len(ensMean[0, 0])))
        return enspmm


class MClimateProduct:
    """
    Products which are generated from or use an m-climate dataset
    """

    def __init__(self, date: datetime):
        mc = MClimate()
        gefs_r = mc.generate_mclimate()

    def event_cdf(self):
        pass

    def extreme_forecast_index(self):
        pass

    def shift_of_tails(self):
        pass

    def time_validating_within_dataset(self):
        pass

    def historical_spread_anomaly(self):
        pass

    def standardized_spread_anomaly(self):
        pass

    def crps(self):
        pass


class MiscProduct:
    """
    Products that don't require m-climate or ensembles for generation
    """

    def __init__(self, ds: xr.Dataset):
        self.ds = ds

    def dprog_dt(self):
        pass


def pull_gefs(ensemble: bool = False):
    gefs_object = GEFSLivePull()
    gefs_l = gefs_object.gefs_live(ensemble=ensemble)
    return gefs_l

import xarray as xr


"""Putting methods here that generate these products so far:
1. probability matched mean
2. extreme forecast index
3. mclimate event cdf/quantile
4. shift of tails
5. % of time validating within dataset
6. historical spread anomaly
7. standardized spread anomaly
8. crps of m-climate? only if the context can be provided
9. dprog/dt analysis?"""


class EnsembleProduct:
    """
    Products that are generated from ensemble forecasts and
    require all ensemble members in the dataset
    """

    def __init__(self, ds: xr.Dataset):
        self.ds = ds

    def probability_matched_mean(self):
        pass


class MClimateProduct:
    """
    Products which are generated from or use an m-climate dataset
    """

    def __init__(self, ds: xr.Dataset):
        self.ds = ds

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

import gc
import logging
import os
import sys
from datetime import datetime
from typing import List, Tuple, Union

import bottleneck
import dask
import numpy as np
import pandas as pd
import pytz
import xarray as xr
from dask.distributed import Client
from gefsv12_retro_kerchunk.kerchunk_zarr import RetrospectivePull

import transforms
import utils as ut

"""
steps for slp: 
1. set up the datetime
2. get the current gefs forecast
3. get the full model climatology based on the datetime
4. any cleanup that's needed, do it
5. combine the mclimate MEAN and the forecast MEAN to grab the percentile using bottleneck rankdata
6. use the percentile to subset the spread based on whatever the percentile bounds are
    a. this might need weird masking that i already did in transforms.py/subset_sprd
7. use the forecast spread and subset spread to use the HSA function and get hsa + standardized spread anomaly
"""


class MClimate:
    def __init__(
        self,
        date: Union[pd.Timestamp, datetime] = datetime.now(tz=pytz.UTC),
        variable: str = "pres_msl",
        **kwargs
    ):
        self.date = date
        self.variable = variable
        self.centered_date_range = kwargs.get("centered_date_range", 10)

    def gefs_retrospective(self, fhour: int = 3) -> xr.Dataset:
        gefs_r = RetrospectivePull(
            date=self.date,
            fhour=fhour,
            variable="pres_msl",
            centered_date_range=self.centered_date_range,
        )
        gefs_r.generate_json_files()
        ds = gefs_r.generate_kerchunk(ds=True)
        return ds

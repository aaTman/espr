from dataclasses import dataclass
from datetime import datetime
from tempfile import TemporaryDirectory
from typing import Optional, Tuple, Union

import fsspec
import pandas as pd
import pytz
import utils as ut
import xarray as xr
from gefsv12_retro_kerchunk.kerchunk_zarr import RetrospectivePull
from kerchunk.combine import MultiZarrToZarr

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


@dataclass
class GEFSLive:
    ge_spr: Union[xr.Dataset, None]
    ge_avg: Union[xr.Dataset, None]
    ge_ens: Union[xr.Dataset, None]
    fhour: int


class ModelMetadata:
    def __init__(
        self,
        date: Union[pd.Timestamp, datetime] = datetime.now(tz=pytz.UTC),
        variable: str = "pres_msl",
        directory: Optional[str] = None,
        **kwargs,
    ):
        self.date = date
        self.variable = variable
        self._temp_dir = TemporaryDirectory() if directory is None else None
        self.directory = directory if directory is not None else self._temp_dir.name
        self.so = {"anon": True, "skip_instance_cache": True}
        self.fs_local = fsspec.filesystem(
            "", skip_instance_cache=True, use_listings_cache=False
        )


class MClimate(ModelMetadata):
    def __init__(
        self,
        date: Union[pd.Timestamp, datetime] = datetime.now(tz=pytz.UTC),
        variable: str = "pres_msl",
        directory: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(date, variable, directory, **kwargs)
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

    def generate_mclimate(self, fhour: int = 3):
        gefs_r = self.gefs_retrospective(fhour=fhour)
        return gefs_r


class GEFSLivePull(ModelMetadata):
    def __init__(
        self,
        date: Union[pd.Timestamp, datetime] = datetime.now(tz=pytz.UTC),
        variable: str = "pres_msl",
        directory: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(date, variable, directory, **kwargs)
        if self.date.hour % 6 != 0:
            self.date = self.date.replace(
                hour=(self.date.hour // 6) * 6,
                minute=0,
                second=0,
                microsecond=0,
            )

    def gefs_live(self, ensemble: bool = False, fhour: int = 3) -> GEFSLive:
        self.date, basename_espr, basename_eavg = ut.find_most_recent_gefs(
            self.date, fhour, data_type=["avg", "spr"]
        )

        gespr, geavg, geens = [
            self.generate_gefs_live_ds(basename, ensemble=ensemble)
            for basename in [basename_espr, basename_eavg]
        ]
        gefs_live = GEFSLive(ge_spr=gespr, ge_avg=geavg, ge_ens=geens, fhour=fhour)
        return gefs_live

    def generate_gefs_live_ds(
        self, basename_tuple: Tuple[str, str], ensemble: bool = False
    ) -> xr.Dataset:
        ut.gen_json(
            file_url=basename_tuple[0],
            fs_local=self.fs_local,
            so=self.so,
            json_dir=self.directory,
            statistic=basename_tuple[1],
        )
        reference_jsons = self.fs_local.ls(self.directory)  # get list of file names
        mzarr = MultiZarrToZarr(
            [n for n in reference_jsons if basename_tuple[1] in n],
            concat_dims=["valid_time"],
            identical_dims=["latitude", "longitude", "step"],
        )
        translated_mzarr = mzarr.translate()
        # open dataset as zarr object using fsspec reference file system and xarray
        fs = fsspec.filesystem(
            "reference",
            fo=translated_mzarr,
            remote_protocol="s3",
            remote_options={"anon": True},
        )
        m = fs.get_mapper("")
        zarr_ds = xr.open_dataset(
            m,
            engine="zarr",
            backend_kwargs=dict(consolidated=False),
            chunks={"valid_time": 1},
        )
        return zarr_ds

import os
from typing import Dict, Optional

import fsspec
import numpy as np
import rasterio
import rasterio.shutil
import xarray
from rasterio.io import MemoryFile
from pystac import Asset, MediaType
from pystac.utils import make_absolute_href

from stactools.noaa_climate_normals import gridded_constants

TRANSFORM = [0.04166667, 0.0, -124.70833333, 0.0, -0.04166667, 49.37500127]

GTIFF_PROFILE = {
    "crs": "epsg:4326",
    "width": 1385,
    "height": 596,
    "dtype": "float32",
    "count": 1,
    "transform": rasterio.Affine(*TRANSFORM),
    "driver": "GTiff",
}

COG_PROFILE = {"compress": "deflate", "blocksize": 512, "driver": "COG"}


def create_cogs(
    nc_href: str,
    frequency: gridded_constants.Frequency,
    period: gridded_constants.Period,
    cog_dir: str,
    cogs: Dict[str, str],
    time_index: Optional[int] = None,
) -> None:
    with fsspec.open(nc_href, mode="rb"):
        with xarray.open_dataset(nc_href) as dataset:
            data_vars = list(dataset.data_vars)
            if frequency is not gridded_constants.Frequency.DAILY:
                data_vars = [var for var in data_vars if frequency.name.lower() in var]

            if gridded_constants.TIME_VARS.get(frequency, None):
                kwargs = {gridded_constants.TIME_VARS[frequency]: time_index - 1}
            else:
                kwargs = None

            for data_var in data_vars:
                cog_filename = f"{period.value.replace('-', '_')}-{data_var}.tif"
                if time_index:
                    cog_filename = cog_filename[0:-4] + f"-{time_index}.tif"

                cogs[data_var] = dict()
                cogs[data_var]["href"] = os.path.join(cog_dir, cog_filename)
                cogs[data_var]["description"] = dataset[data_var].long_name
                cogs[data_var]["unit"] = dataset[data_var].units

                nodata = 0 if "flag" in data_var else np.nan

                if kwargs:
                    values = dataset[data_var].isel(**kwargs)
                else:
                    values = dataset[data_var]
                values = np.round(values, 2)  # per comments in NetCDF files

                with MemoryFile() as mem:
                    with mem.open(**GTIFF_PROFILE, nodata=nodata) as temp:
                        temp.write(values, 1)
                        rasterio.shutil.copy(temp, cogs[data_var]["href"], **COG_PROFILE)


def cog_asset(key: str, cog: Dict[str, str]) -> Asset:
    roles = ["metadata"] if "flag" in key else ["data"]
    roles.append("cloud-optimized")

    nodata = 0 if "flag" in key else "nan"

    unit = cog["unit"].replace("_", " ")
    if "number of" in unit:
        unit = unit.replace("number of ", "")

    raster_bands = [
        {
            "data_type": "float32",
            "nodata": nodata,
            "unit": unit,
            "spatial_resolution": 5000
        }
    ]

    return Asset(
        href=make_absolute_href(cog["href"]),
        description=cog["description"],
        media_type=MediaType.COG,
        roles=roles,
        extra_fields={
            "raster:bands": raster_bands
        }
    )

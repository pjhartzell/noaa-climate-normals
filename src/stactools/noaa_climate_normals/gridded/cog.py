import os
from typing import Any, Dict, Optional

import fsspec
import numpy as np
import rasterio
import rasterio.shutil
import xarray
from pystac import Asset, MediaType
from pystac.utils import make_absolute_href
from rasterio.io import MemoryFile

from . import constants

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
    frequency: constants.Frequency,
    period: constants.Period,
    cog_dir: str,
    cogs: Dict[str, Any],
    time_index: Optional[int] = None,
) -> None:
    """Creates COGs for all variables in a NetCDF for a particular Climate
    Normal frequency.

    Args:
        nc_href (str): HREF to to the NetCDF file.
        period (constants.Period): Climate normal time period of CSV data, e.g.,
            '1991-2020'.
        frequency (constants.Frequency): Temporal interval of COGs to be
            created, e.g., 'monthly' or 'daily'.
        cog_dir (str): Directory to store created COGs.
        cogs (Dict[str, Any]): A dictionary for COG metadata to be used in STAC
            Item assets.
        time_index (Optional[int]): 1-based time index into the NetCDF
            timestack, e.g., 'time_index=3' for the month of March for a NetCDF
            holding monthly frequency data.
    """
    with fsspec.open(nc_href, mode="rb"):
        with xarray.open_dataset(nc_href) as dataset:
            data_vars = list(dataset.data_vars)
            if frequency is not constants.Frequency.DAILY:
                data_vars = [var for var in data_vars if frequency.name.lower() in var]

            if time_index:
                kwargs = {constants.TIME_VARS[frequency]: time_index - 1}
            else:
                kwargs = None

            for data_var in data_vars:
                cog_filename = (
                    f"{period.value.replace('-', '_')}-{frequency}-{data_var}.tif"
                )
                if time_index:
                    cog_filename = cog_filename[0:-4] + f"-{time_index}.tif"

                cogs[data_var] = {}
                cogs[data_var]["href"] = os.path.join(cog_dir, cog_filename)
                cogs[data_var]["description"] = dataset[data_var].long_name
                cogs[data_var]["unit"] = dataset[data_var].units

                nodata = 0 if "flag" in data_var else np.nan

                if kwargs:
                    values = dataset[data_var].isel(**kwargs)
                else:
                    values = dataset[data_var]

                # round per comments in NetCDF files
                if "prcp" in data_var:
                    values = np.round(values, 2)
                else:
                    values = np.round(values, 1)

                with MemoryFile() as mem:
                    with mem.open(**GTIFF_PROFILE, nodata=nodata) as temp:
                        temp.write(values, 1)
                        rasterio.shutil.copy(
                            temp, cogs[data_var]["href"], **COG_PROFILE
                        )


def cog_asset(data_var: str, cog: Dict[str, str]) -> Asset:
    """Creates a STAC Asset for a COG.

    Args:
        data_var (str): Name of the NetCDF data variable the COG was created
            from.
        cog (Dict[str, str]): Dictionary of COG metadata.

    Returns:
        Asset: A STAC Asset for the COG.
    """
    nodata = 0 if "flag" in data_var else "nan"

    unit = cog["unit"].replace("_", " ")
    if "number of" in unit:
        unit = unit.replace("number of ", "")

    raster_bands = [
        {
            "data_type": "float32",
            "nodata": nodata,
            "unit": unit,
            "spatial_resolution": 5000,
        }
    ]

    return Asset(
        href=make_absolute_href(cog["href"]),
        description=cog["description"],
        media_type=MediaType.COG,
        roles=["data"],
        extra_fields={"raster:bands": raster_bands},
    )

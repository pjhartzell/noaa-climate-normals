from datetime import datetime, timezone
from typing import Any, Dict

import pystac

from ..constants import KEYWORDS

BBOX = [-124.708333, 24.541666, -66.999995, 49.375001]
NETCDF_MEDIA_TYPE = "application/netcdf"
NETCDF_ROLES = ["data"]

COLLECTION: Dict[str, Any] = {
    "id": "noaa-climate-normals-netcdf",
    "title": "Gridded U.S. Climate Normals - NetCDF Data",
    "description": (
        "NOAA's Gridded U.S. Climate Normals provide a continuous grid of "
        "temperature and precipitation data across the continental United States (CONUS). "
        "The grids are derived from NOAA's NClimGrid dataset and data resolution "
        "(nominal 5x5 kilometer) and spatial extents (CONUS) therefore "
        "match that of NClimGrid. Monthly, seasonal, and annual gridded normals "
        "are computed from simple averages of the NClimGrid data "
        "and are provided for three time periods: 1901-2020, 1991-2020, and "
        "2006-2020. Daily gridded normals are smoothed for a smooth transition "
        "from one day to another and are provided for two time periods: "
        "1991-2020, and 2006-2020. The data in this Collection are the original "
        "NetCDF files provided by NOAA's National Centers for Environmental Information."
    ),
    "license": "proprietary",
    "keywords": KEYWORDS,
    "extent": pystac.Extent(
        pystac.SpatialExtent([BBOX]),
        pystac.TemporalExtent(
            [
                [
                    datetime(1901, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                    datetime(2020, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
                ]
            ]
        ),
    ),
}

FREQUENCIES = ["daily", "monthly"]
PERIODS = ["1901-2000", "1991-2020", "2006-2020"]
VARIABLES = {
    "prcp": "Precipitation",
    "tavg": "Average Temperature",
    "tmax": "Maximum Temperature",
    "tmin": "Minimum Temperature",
    "m2dprcp": "Month-to-Date Precipitation",
    "y2dprcp": "Year-to-Date Precipitation",
}

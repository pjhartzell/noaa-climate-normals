from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict

import pystac

from ..constants import KEYWORDS


class Frequency(str, Enum):
    DAILY = "daily"
    MLY = "monthly"
    SEAS = "seasonal"
    ANN = "annual"


class Period(str, Enum):
    PERIOD_1901_2000 = "1901-2000"
    PERIOD_1991_2020 = "1991-2020"
    PERIOD_2006_2020 = "2006-2020"


TIME_VARS = {
    Frequency.DAILY: "time",
    Frequency.MLY: "time",
    Frequency.SEAS: "seasons",
    Frequency.ANN: None,
}

SEASONS = {
    1: "Winter (Dec-Jan-Feb)",
    2: "Spring (Mar-Apr-May)",
    3: "Summer (Jun-Jul-Aug)",
    4: "Fall (Sep-Oct-Nov)",
}

PREFIXES = ["prcp", "tavg", "tmax", "tmin", "m2dprcp", "y2dprcp"]

RASTER_EXTENSION_V11 = "https://stac-extensions.github.io/raster/v1.1.0/schema.json"

COLLECTION: Dict[str, Any] = {
    "id": "noaa-climate-normals-gridded",
    "title": "Gridded U.S. Climate Normals",
    "description": (
        "NOAA's Gridded U.S. Climate Normals provide a continuous grid of "
        "temperature and precipitation data across the continental United States (CONUS). "
        "The grids are derived from NOAA's NClimGrid dataset, and resolutions "
        "(nominal 5x5 kilometer) and spatial extents (CONUS) therefore "
        "match that of NClimGrid. Monthly, seasonal, and annual gridded normals "
        "are computed from simple averages of the NClimGrid data "
        "and are provided for three time periods: 1901-2020, 1991-2020, and "
        "2006-2020. Daily gridded normals are smoothed for a smooth transition "
        "from one day to another and are provided for two time periods: "
        "1991-2020, and 2006-2020. The data in this Collection have been "
        "converted from the original NetCDF format to Cloud-Optimized GeoTIFFs (COGs)."
    ),
    "license": "proprietary",
    "keywords": KEYWORDS,
    "extent": pystac.Extent(
        pystac.SpatialExtent([[-124.708333, 24.541666, -66.999995, 49.375001]]),
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

README = pystac.Link(
    rel="about",
    target="https://www.ncei.noaa.gov/sites/default/files/2022-04/Readme_Monthly_Gridded_Normals.pdf",  # noqa
    media_type="application/pdf",
    title="Readme for Monthly Gridded Normals",
)
DOCUMENTATION = pystac.Link(
    rel="about",
    target="https://www.ncei.noaa.gov/sites/default/files/2022-04/Documentation_Monthly_Gridded_Normals.pdf",  # noqa
    media_type="application/pdf",
    title="Documentation for Monthly Gridded Normals",
)

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

PREFIXES = {
    "prcp": "Precipitation",
    "tavg": "Average temperature",
    "tmax": "Maximum temperature",
    "tmin": "Minimum temperature",
    "m2dprcp": "Month-to-date precipitation",
    "y2dprcp": "Year-to-date precipitation",
}

RASTER_EXTENSION_V11 = "https://stac-extensions.github.io/raster/v1.1.0/schema.json"

NETCDF_MEDIA_TYPE = "application/netcdf"
NETCDF_ROLES = ["data", "source"]

MONTHLY_FILES = ["prcp", "tavg", "tmin", "tmax"]

COLLECTION: Dict[str, Any] = {
    "id": "noaa-climate-normals-gridded",
    "title": "Gridded U.S. Climate Normals",
    "description": (
        "The gridded version of NOAA's U.S. Climate Normals provide temperature "
        "and precipitation data derived from NOAA's NClimGrid dataset. Grid "
        "resolutions (nominal 5x5 kilometer) and spatial extents (Continental "
        "U.S.) therefore match that of NClimGrid. Monthly, seasonal, and annual "
        "gridded normals are derived from simple averaging of the NClimGrid data. "
        "Daily gridded normals are smoothed for a smooth transition from one day "
        "to another. Monthly, seasonal, and annual gridded normals are provided "
        "for three time periods: 1901-2020, 1991-2020, and 2006-2020."
    ),
    "license": "proprietary",
    "keywords": KEYWORDS,
    "extent": pystac.Extent(
        pystac.SpatialExtent([[-124.708333, 24.541666, -66.999995, 49.375001]]),
        pystac.TemporalExtent(
            [
                [
                    datetime(1901, 1, 1, tzinfo=timezone.utc),
                    datetime(2020, 12, 31, tzinfo=timezone.utc),
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

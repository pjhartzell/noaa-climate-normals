from enum import Enum


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

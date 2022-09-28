from enum import Enum


class Frequency(str, Enum):
    HOURLY = "hourly"
    DAILY = "daily"
    MONTHLY = "monthly"
    ANNUALSEASONAL = "annualseasonal"


class Period(str, Enum):
    ONE = "1981-2010"
    TWO = "1991-2010"
    THREE = "2006-2020"


PARQUET_MEDIA_TYPE = "application/x-parquet"
PARQUET_GEOMETRY_COL = "geometry"
PARQUET_ASSET_TITLE = "GeoParquet for all stations"

CRS = "EPSG:4269"

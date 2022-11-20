#!/usr/bin/env python3

"""Creates the example STAC metadata, COGS, and GeoParquet.

"""
import logging
from pathlib import Path

from stactools.noaa_climate_normals.tabular.constants import Frequency, Period
from stactools.noaa_climate_normals.tabular.parquet import create_parquet

logging.basicConfig(format="%(message)s")
logger = logging.getLogger()
logger.setLevel("INFO")

csv_base_directory = "/Volumes/Samsung_T5/data/ncn/tabular/"
prefixes = [
    "normals-daily/1981-2010/access/",
    "normals-daily/1991-2020/access/",
    "normals-daily/2006-2020/access/",
    "normals-hourly/1981-2010/access/",
    "normals-hourly/1991-2020/access/",
    "normals-hourly/2006-2020/access/",
    "normals-monthly/1981-2010/access/",
    "normals-monthly/1991-2020/access/",
    "normals-monthly/2006-2020/access/",
    "normals-annualseasonal/1981-2010/access/",
    "normals-annualseasonal/1991-2020/access/",
    "normals-annualseasonal/2006-2020/access/",
]

for prefix in prefixes:
    logger.info(f"Working on CSVs in '{prefix}'")
    csv_paths = list(Path(csv_base_directory, prefix).glob("*.csv"))
    csv_hrefs = [path.as_posix() for path in csv_paths]

    period = Period(prefix.split("/")[1])
    frequency = Frequency(prefix.split("/")[0].split("-")[1])

    geoparquet_href = create_parquet(
        csv_hrefs=csv_hrefs,
        frequency=frequency,
        period=period,
        parquet_dir="geoparquet",
    )

#!/usr/bin/env python3

"""Creates the example STAC metadata, COGS, and GeoParquet.

Assumptions:
- The test suite has been run, so all external data have been downloaded.
"""

from azure.storage.blob import ContainerClient

from stactools.noaa_climate_normals.tabular.constants import Frequency, Period
from stactools.noaa_climate_normals.tabular.parquet import create_parquet

blob_container = "https://noaanormals.blob.core.windows.net/climate-normals/"
prefixes = [
    # "normals-daily/1981-2010/access/",
    # "normals-daily/1991-2020/access/",
    # "normals-daily/2006-2020/access/",
    # "normals-hourly/1981-2010/access/",
    # "normals-hourly/1991-2020/access/",
    # "normals-hourly/2006-2020/access/",
    # "normals-monthly/1981-2010/access/",
    # "normals-monthly/1991-2020/access/",
    "normals-monthly/2006-2020/access/",
    "normals-annualseasonal/1981-2010/access/",
    "normals-annualseasonal/1991-2020/access/",
    "normals-annualseasonal/2006-2020/access/",
]

for prefix in prefixes:
    print(f"Working on CSVs in '{prefix}'.")
    container = ContainerClient.from_container_url(blob_container)
    blob_list = list(container.list_blobs(name_starts_with=prefix))
    href_list = [f"{blob_container}{blob['name']}" for blob in blob_list]

    period = Period(prefix.split("/")[1])
    frequency = Frequency(prefix.split("/")[0].split("-")[1])

    geoparquet_href = create_parquet(
        csv_hrefs=href_list, frequency=frequency, period=period, parquet_dir="."
    )

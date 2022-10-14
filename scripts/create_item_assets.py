#!/usr/bin/env python3

"""Creates a JSON file containing AssetDefinition dictionaries for use in
item_assets in the gridded collection.
"""

import json
from tempfile import TemporaryDirectory
from typing import Any, Dict, ValuesView

from stactools.noaa_climate_normals.gridded.cog import cog_asset, create_cogs
from stactools.noaa_climate_normals.gridded.constants import Frequency, Period
from stactools.noaa_climate_normals.gridded.utils import nc_href_dict

paths = [
    "/Users/pjh/data/noaa-climate-normals/gridded/daily/2006-2020/prcp-2006_2020-daily-normals-v1.0.nc",  # noqa
    "/Users/pjh/data/noaa-climate-normals/gridded/daily/1991-2020/prcp-1991_2020-daily-normals-v1.0.nc",  # noqa
    "/Users/pjh/data/noaa-climate-normals/gridded/monthly/1901-2000/prcp-1901_2000-monthly-normals-v1.0.nc",  # noqa
    "/Users/pjh/data/noaa-climate-normals/gridded/monthly/2006-2020/prcp-2006_2020-monthly-normals-v1.0.nc",  # noqa
    "/Users/pjh/data/noaa-climate-normals/gridded/monthly/1991-2020/prcp-1991_2020-monthly-normals-v1.0.nc",  # noqa
]
time_index = 1


def get_cog_asset_info(
    path: str, frequency: Frequency, period: Period
) -> Dict[str, Any]:
    with TemporaryDirectory() as cog_dir:
        cogs: Dict[str, Any] = {}
        nc_hrefs = nc_href_dict(path, frequency)
        for _, nc_href in nc_hrefs.items():
            time_index = None if frequency is Frequency.ANN else 1
            create_cogs(nc_href, frequency, period, cog_dir, cogs, time_index)

        cog_assets = {}
        for key, value in cogs.items():
            cog_asset_dict = cog_asset(key, value).to_dict()
            cog_asset_dict.pop("href")
            cog_assets[key] = cog_asset_dict

        return cog_assets


def update_item_assets(cog_assets: Dict[str, Any], frequency: str, period: str) -> None:
    for key, value in cog_assets.items():
        if item_assets.get(key, None) is None:
            item_assets[key] = value
        else:
            if value != item_assets[key]:
                print("COG asset key exists but value does not match")
                new_key = f"{key}_{frequency}_{period}"
                item_assets[new_key] = ValuesView


item_assets: Dict[str, Any] = {}

for path in paths:
    path_parts = path.split("/")
    frequency = path_parts[6]
    period = path_parts[7]
    if frequency == "daily":
        print(f"Working on '{frequency}' '{period}'")
        cog_assets = get_cog_asset_info(path, Frequency(frequency), Period(period))
        update_item_assets(cog_assets, frequency, period)
    elif frequency == "monthly":
        for _frequency in ["monthly", "seasonal", "annual"]:
            print(f"Working on '{_frequency}' '{period}'")
            cog_assets = get_cog_asset_info(path, Frequency(_frequency), Period(period))
            update_item_assets(cog_assets, _frequency, period)
    else:
        raise ValueError("Stop. Something is wrong.")

with open("item_assets.json", "w") as fout:
    json.dump(item_assets, fout)

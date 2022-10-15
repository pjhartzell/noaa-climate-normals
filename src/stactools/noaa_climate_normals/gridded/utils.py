import calendar
import datetime
import json
import os
from typing import Any, Dict, Optional

import pkg_resources
from pystac import Asset
from pystac.utils import make_absolute_href

from . import constants


def nc_href_dict(nc_href: str, frequency: constants.Frequency) -> Dict[str, str]:
    """Creates a dictionary of NetCDF file names that contain data required to
    create an Item.

    Args:
        nc_href (str): HREF to one of the NetCDFs necessary to create an Item.
        frequency (Frequency): Temporal interval of Item to be created, e.g.,
            'monthly' or 'daily'.

    Returns:
        Dict[str, str]: Mapping of weather variable type to NetCDF filename.
    """
    base, filename = os.path.split(nc_href)
    suffix = "-".join(filename.split("-")[1:])
    filenames = {prefix: f"{prefix}-{suffix}" for prefix in constants.PREFIXES}
    href_dict = {prefix: os.path.join(base, name) for prefix, name in filenames.items()}
    if frequency is not constants.Frequency.DAILY:
        href_dict.pop("m2dprcp")
        href_dict.pop("y2dprcp")
    return href_dict


def nc_asset(prefix: str, nc_href: str) -> Asset:
    """Creates a STAC Asset for a NetCDF file.

    Args:
        prefix (str): Weather variable type from constants.PREFIXES
        nc_href (str): HREF to the NetCDF file for which to create an Asset.

    Returns:
        Asset: A STAC Asset for the NetCDF file.
    """
    return Asset(
        href=make_absolute_href(nc_href),
        description=f"{constants.PREFIXES[prefix]} source data",
        media_type=constants.NETCDF_MEDIA_TYPE,
        roles=constants.NETCDF_ROLES,
    )


def item_title(
    frequency: constants.Frequency,
    period: constants.Period,
    time_index: Optional[int] = None,
) -> str:
    """Generates a formatted title for an Item.

    Args:
        frequency (Frequency): Temporal interval of Item being created, e.g.,
            'monthly' or 'daily'.
        period (constants.Period): Climate normal time period, e.g., '1991-2020'.
        time_index (Optional[int]): 1-based time index relating to the
            frequency, e.g., 'time_index=3' for the month of March for monthly
            frequency data.

    Returns:
        str: Formatted Item title.
    """
    if time_index:
        if frequency is constants.Frequency.DAILY:
            leap_year = 2020  # use a leap year for date since daily data has 366 days
            date = datetime.datetime.strptime(f"{time_index} {leap_year}", "%j %Y")
            title = f"{period} Daily Climate Normals for {date.strftime('%B %-d')}"
        if frequency is constants.Frequency.MLY:
            title = f"{period} Monthly Climate Normals for {calendar.month_name[time_index]}"
        if frequency is constants.Frequency.SEAS:
            title = (
                f"{period} Seasonal Climate Normals for {constants.SEASONS[time_index]}"
            )
    else:
        title = f"{period} Annual Climate Normals"

    return title


def load_item_assets() -> Any:
    """Loads a dictionary of item_assets entries for the collection.

    Returns:
        Any: A dictionary of item_asset dictionaries
    """
    try:
        with pkg_resources.resource_stream(
            "stactools.noaa_climate_normals.gridded.utils",
            "item_assets/item_assets.json",
        ) as stream:
            return json.load(stream)
    except FileNotFoundError as e:
        raise e

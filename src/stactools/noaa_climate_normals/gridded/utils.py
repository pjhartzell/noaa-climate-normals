import calendar
import datetime
import os
from typing import Dict, Optional

from pystac import Asset
from pystac.utils import make_absolute_href

from . import constants


def nc_href_dict(nc_href: str, frequency: constants.Frequency) -> Dict[str, str]:
    """Creates a dictionary of NetCDF file names that contain data required to
    create an Item.

    Args:
        nc_href (str): HREF to one of the NetCDFs necessary to create an Item.
        frequency (Frequency): Temporal interval specified in the NetCDF filename.

    Returns:
        List[str]: ...
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
    if time_index:
        if frequency is constants.Frequency.DAILY:
            not_leap = 2021
            date = datetime.datetime.strptime(f"{time_index} {not_leap}", "%j %Y")
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

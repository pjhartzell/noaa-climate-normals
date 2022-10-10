import os
from typing import Dict

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

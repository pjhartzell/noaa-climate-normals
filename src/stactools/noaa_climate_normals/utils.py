import os
from typing import Dict, Optional

from pystac import Asset
from pystac.utils import make_absolute_href
from stactools.core.io import ReadHrefModifier

from . import gridded_constants


def modify_href(
    href: str, read_href_modifier: Optional[ReadHrefModifier] = None
) -> str:
    """Modify an HREF with, for example, a token signature.

    Args:
        href (str): The HREF to be modified
        read_href_modifier (Optional[ReadHrefModifier]): An optional function
            to modify an href (e.g., to add a token to a url).

    Returns:
        str: The modified HREF.
    """
    if read_href_modifier:
        return read_href_modifier(href)
    else:
        return href


def nc_href_dict(
    nc_href: str, frequency: gridded_constants.Frequency
) -> Dict[str, str]:
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
    filenames = {prefix: f"{prefix}-{suffix}" for prefix in gridded_constants.PREFIXES}
    href_dict = {prefix: os.path.join(base, name) for prefix, name in filenames.items()}
    if frequency is not gridded_constants.Frequency.DAILY:
        href_dict.pop("m2dprcp")
        href_dict.pop("y2dprcp")
    return href_dict


def nc_asset(prefix: str, nc_href: str) -> Asset:
    return Asset(
        href=make_absolute_href(nc_href),
        description=f"{gridded_constants.PREFIXES[prefix]} source data",
        media_type=gridded_constants.NETCDF_MEDIA_TYPE,
        roles=gridded_constants.NETCDF_ROLES,
    )

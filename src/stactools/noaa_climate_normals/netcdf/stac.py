from datetime import datetime, timezone

from pystac import Asset, Collection, Item, Summaries
from pystac.extensions.item_assets import AssetDefinition, ItemAssetsExtension
from pystac.extensions.projection import ItemProjectionExtension
from pystac.utils import datetime_to_str, make_absolute_href
from shapely.geometry import box, mapping

from ..constants import LANDING_PAGE_LINK, LICENSE_LINK, PROVIDERS
from ..gridded.constants import DOCUMENTATION, README
from .constants import (
    BBOX,
    COLLECTION,
    FREQUENCIES,
    NETCDF_MEDIA_TYPE,
    NETCDF_ROLES,
    PERIODS,
    VARIABLES,
)
from .utils import netcdf_item_id


def create_item(nc_href: str) -> Item:
    """Creates a STAC Item for a NetCDF file containing gridded Climate Normal
    data.

    Args:
        nc_href (str): HREF to a NetCDF file.

    Returns:
        Item: A STAC Item describing a NetCDF file.
    """
    id = netcdf_item_id(nc_href)
    filename_parts = id.split("-")
    variable = filename_parts[0]
    period = filename_parts[1].replace("_", "-")
    frequency = filename_parts[2]
    asset_title = (
        f"{period} {frequency.capitalize()} Gridded Climate Normals for "
        f"{VARIABLES[variable]}"
    )

    item = Item(
        id=id,
        geometry=mapping(box(*BBOX)),
        bbox=BBOX,
        datetime=None,
        properties={
            "noaa-climate-normals:frequency": frequency,
            "noaa-climate-normals:period": period,
            "start_datetime": datetime_to_str(
                datetime(int(period[0:4]), 1, 1, 0, 0, 0)
            ),
            "end_datetime": datetime_to_str(
                datetime(int(period[5:]), 12, 31, 23, 59, 59)
            ),
            "created": datetime_to_str(datetime.now(tz=timezone.utc)),
        },
    )

    projection = ItemProjectionExtension.ext(item, add_if_missing=True)
    projection.epsg = 4326

    item.add_asset(
        "netcdf",
        Asset(
            href=make_absolute_href(nc_href),
            title=asset_title,
            media_type=NETCDF_MEDIA_TYPE,
            roles=NETCDF_ROLES,
        ),
    )

    return item


def create_collection() -> Collection:
    """Creates a STAC Collection for NetCDF data.

    Returns:
        Collection: A STAC Collection for NetCDF Items.
    """
    collection = Collection(**COLLECTION)

    collection.providers = PROVIDERS

    collection.add_links([LANDING_PAGE_LINK, LICENSE_LINK, README, DOCUMENTATION])

    collection.summaries = Summaries(
        {
            "frequency": FREQUENCIES,
            "period": PERIODS,
        }
    )

    item_assets = ItemAssetsExtension.ext(collection, add_if_missing=True)
    item_assets.item_assets = {
        "netcdf": AssetDefinition({"type": NETCDF_MEDIA_TYPE, "roles": NETCDF_ROLES})
    }

    return collection

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import stactools.core.create
from pystac import Collection, Item, Summaries
from pystac.extensions.item_assets import AssetDefinition, ItemAssetsExtension
from stactools.core.io import ReadHrefModifier

from ..constants import LANDING_PAGE_LINK, LICENSE_LINK, PROVIDERS
from ..utils import modify_href
from . import constants
from .cog import cog_asset, create_cogs
from .utils import item_title, load_item_assets, nc_asset, nc_href_dict

logger = logging.getLogger(__name__)


def create_item(
    nc_href: str,
    frequency: constants.Frequency,
    cog_dir: str,
    *,
    time_index: Optional[int] = None,
    netcdf_assets: bool = False,
    read_href_modifier: Optional[ReadHrefModifier] = None,
) -> Item:
    """Creates a STAC Item and COGs for a single timestep of Climate Normal
    data, e.g., an Item for the month of March for the monthly Climate Normals.

    Args:
        nc_href (str): HREF to one of the NetCDF files containing data required
            for Item creation. Any additional required NetCDF files are assumed
            to exist in the same location.
        frequency (Frequency): Temporal interval of Item to be created, e.g.,
            'monthly' or 'daily'.
        cog_dir (str): Directory to store created COGs.
        time_index (Optional[int]): 1-based time index into the NetCDF
            timestack, e.g., 'time_index=3' for the month of March for a NetCDF
            holding monthly frequency data.
        no_netcdf_assets (bool, optional): Flag to include the NetCDF files as
            Assets on the Item.
        read_href_modifier (Optional[ReadHrefModifier]): An optional function

    Returns:
        Item: A STAC Item for a single timestep of Climate Normal data.
    """
    if not time_index and frequency is not constants.Frequency.ANN:
        raise ValueError(f"A time_index value is required for {frequency.value} data.")
    if time_index and frequency is constants.Frequency.ANN:
        logger.info("time_index value is not used for Annual frequency data")
        time_index = None

    period = constants.Period(os.path.basename(nc_href).split("-")[1].replace("_", "-"))
    id = f"{period.value.replace('-', '_')}-{frequency}"
    if time_index:
        id += f"-{time_index}"
    title = item_title(frequency, period, time_index)

    cogs: Dict[str, Any] = {}
    nc_hrefs = nc_href_dict(nc_href, frequency)
    for _, nc_href in nc_hrefs.items():
        modified_href = modify_href(nc_href, read_href_modifier)
        create_cogs(modified_href, frequency, period, cog_dir, cogs, time_index)

    item = stactools.core.create.item(next(iter(cogs.values()))["href"])
    item.id = id
    item.datetime = None
    item.common_metadata.start_datetime = datetime(
        int(period.value[0:4]), 1, 1, 0, 0, 0
    )
    item.common_metadata.end_datetime = datetime(int(period.value[5:]), 12, 31, 0, 0, 0)
    item.common_metadata.created = datetime.now(tz=timezone.utc)
    item.properties["noaa-climate-normals:frequency"] = frequency
    item.properties["noaa-climate-normals:period"] = period
    if time_index:
        item.properties["noaa-climate-normals:time_index"] = time_index
    item.properties["title"] = title

    item.assets.pop("data")
    for key, value in cogs.items():
        item.add_asset(key, cog_asset(key, value))

    if netcdf_assets:
        for prefix, href in nc_hrefs.items():
            item.add_asset(f"{prefix}_source", nc_asset(prefix, href))

    item.stac_extensions.append(constants.RASTER_EXTENSION_V11)

    return item


def create_collection() -> Collection:
    collection = Collection(**constants.COLLECTION)

    collection.providers = PROVIDERS

    collection.add_links(
        [LANDING_PAGE_LINK, LICENSE_LINK, constants.README, constants.DOCUMENTATION]
    )

    collection.summaries = Summaries(
        {
            "frequency": [f.value for f in constants.Frequency],
            "period": [p.value for p in constants.Period],
        }
    )

    item_asset_dicts = load_item_assets()
    for key, value in item_asset_dicts.items():
        item_asset_dicts[key] = AssetDefinition(value)
    item_assets = ItemAssetsExtension.ext(collection, add_if_missing=True)
    item_assets.item_assets = item_asset_dicts

    collection.stac_extensions.append(constants.RASTER_EXTENSION_V11)

    return collection

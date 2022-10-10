import logging
import os
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional

import stactools.core.create
from pystac import Asset, Item
from pystac.extensions.projection import ProjectionExtension
from pystac.extensions.scientific import ScientificExtension
from pystac.extensions.table import TableExtension
from pystac.utils import datetime_to_str, make_absolute_href
from stactools.core.io import ReadHrefModifier

from stactools.noaa_climate_normals import constants, gridded_constants, utils
from stactools.noaa_climate_normals.cog import cog_asset, create_cogs
from stactools.noaa_climate_normals.parquet import create_parquet

logger = logging.getLogger(__name__)


def create_gridded_item(
    nc_href: str,
    frequency: gridded_constants.Frequency,
    cog_dir: str,
    *,
    time_index: Optional[int] = None,
    no_netcdf_assets: bool = False,
    read_href_modifier: Optional[ReadHrefModifier] = None,
) -> Item:

    if not time_index and frequency is not gridded_constants.Frequency.ANN:
        raise ValueError(f"A time_index value is required for {frequency.value} data.")
    if time_index and frequency is gridded_constants.Frequency.ANN:
        logger.info("time_index value is not used for Annual frequency data")
        time_index = None

    period = gridded_constants.Period(
        os.path.basename(nc_href).split("-")[1].replace("_", "-")
    )
    id = f"{period.value.replace('-', '_')}-{frequency}"
    if time_index:
        id += f"-{time_index}"
    title = f"{frequency.value.capitalize()} Climate Normals for Period {period}"

    cogs: Dict[str, Dict[str, str]] = defaultdict(dict)
    nc_hrefs = utils.nc_href_dict(nc_href, frequency)
    for _, nc_href in nc_hrefs.items():
        modified_href = utils.modify_href(nc_href, read_href_modifier)
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
    item.properties["title"] = title

    item.assets.pop("data")
    for key, value in cogs.items():
        item.add_asset(key, cog_asset(key, value))

    if not no_netcdf_assets:
        for prefix, href in nc_hrefs.items():
            item.add_asset(f"{prefix}_source", utils.nc_asset(prefix, href))

    item.stac_extensions.append(gridded_constants.RASTER_EXTENSION_V11)

    return item


def create_tabular_item(
    csv_hrefs: List[str],
    frequency: constants.Frequency,
    period: constants.Period,
    parquet_dir: str,
) -> Item:

    start_year = int(period.value[0:4])
    end_year = int(period.value[5:])
    id = f"{frequency}_{period}"

    if frequency is constants.Frequency.ANNUALSEASONAL:
        formatted_frequency = "Annual/Seasonal"
    else:
        formatted_frequency = frequency.value.capitalize()
    title = f"{formatted_frequency} Climate Normals for Period {period}"

    parquet_dict = create_parquet(
        csv_hrefs, frequency, period, os.path.join(parquet_dir, f"{id}.parquet")
    )
    geometry = parquet_dict.pop("geometry")
    bbox = parquet_dict.pop("bbox")

    item = Item(
        id=id,
        geometry=geometry,
        bbox=bbox,
        datetime=None,
        properties={
            "noaa-climate-normals:frequency": frequency,
            "noaa-climate-normals:period": period,
            "start_datetime": datetime_to_str(datetime(start_year, 1, 1, 0, 0, 0)),
            "end_datetime": datetime_to_str(datetime(end_year, 12, 31, 23, 59, 59)),
            "created": datetime_to_str(datetime.now(tz=timezone.utc)),
            "title": title,
        },
    )

    parquet_dict["href"] = make_absolute_href(parquet_dict["href"])
    item.add_asset("parquet", Asset.from_dict(parquet_dict))
    TableExtension.ext(item.assets["parquet"], add_if_missing=True)

    projection = ProjectionExtension.ext(item, add_if_missing=True)
    projection.epsg = int(constants.CRS[5:])

    scientific = ScientificExtension.ext(item, add_if_missing=True)
    if period is constants.Period.PERIOD_1981_2010:
        scientific.doi = constants.DATA_1981_2010["doi"]
        citation = constants.DATA_1981_2010["citation"]
        scientific.citation = citation.replace("FREQUENCY", formatted_frequency)
    if frequency is constants.Frequency.HOURLY:
        scientific.publications = [constants.PUBLICATION_HOURLY]
    else:
        scientific.publications = [constants.PUBLICATION_DAILY_MONTHLY_ANNUALSEASONAL]

    item.add_links(
        [
            constants.HOMEPAGE[period][frequency],
            constants.DOCUMENTATION[period][frequency],
        ]
    )

    return item

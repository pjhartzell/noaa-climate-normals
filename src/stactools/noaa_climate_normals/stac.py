import logging
import os
from datetime import datetime, timezone
from typing import List

from pystac import Asset, Item
from pystac.extensions.projection import ProjectionExtension
from pystac.extensions.scientific import ScientificExtension
from pystac.extensions.table import TableExtension
from pystac.utils import datetime_to_str, make_absolute_href

from stactools.noaa_climate_normals import constants
from stactools.noaa_climate_normals.parquet import create_parquet

logger = logging.getLogger(__name__)


def create_tabular_item(
    csv_hrefs: List[str],
    frequency: constants.Frequency,
    period: constants.Period,
    parquet_dir: str,
) -> Item:

    # Add documentation links

    start_year = int(period.value[0:4])
    end_year = int(period.value[5:])
    id = f"{frequency}_{period}"
    formatted_frequency = "/".join([f.capitalize() for f in frequency.split("/")])
    title = f"{formatted_frequency} NOAA US Climate Normals for Period {period}"

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

    item.add_links(
        [
            constants.HOMEPAGE[period][frequency],
            constants.DOCUMENTATION[period][frequency],
        ]
    )

    scientific = ScientificExtension.ext(item, add_if_missing=True)
    scientific.doi = constants.CITE_AS[period]["doi"]
    citation = constants.CITE_AS[period]["citation"]
    scientific.citation = citation.replace("FREQUENCY", formatted_frequency)

    return item


def create_gridded_item() -> None:
    return None

import os
from datetime import datetime, timezone
from typing import List, Optional

from pystac import Asset, Item
from pystac.extensions.projection import ProjectionExtension
from pystac.extensions.scientific import ScientificExtension
from pystac.extensions.table import TableExtension
from pystac.utils import datetime_to_str, make_absolute_href
from stactools.core.io import ReadHrefModifier

from ..utils import modify_href
from . import constants
from .parquet import create_parquet


def create_item(
    csv_hrefs: List[str],
    frequency: constants.Frequency,
    period: constants.Period,
    parquet_dir: str,
    *,
    read_href_modifier: Optional[ReadHrefModifier] = None,
) -> Item:

    start_year = int(period.value[0:4])
    end_year = int(period.value[5:])
    id = f"{frequency}_{period}"

    if frequency is constants.Frequency.ANNUALSEASONAL:
        formatted_frequency = "Annual/Seasonal"
    else:
        formatted_frequency = frequency.value.capitalize()
    title = f"{formatted_frequency} Climate Normals for Period {period}"

    read_csv_hrefs = [
        modify_href(csv_href, read_href_modifier) for csv_href in csv_hrefs
    ]
    parquet_dict = create_parquet(
        read_csv_hrefs, frequency, period, os.path.join(parquet_dir, f"{id}.parquet")
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
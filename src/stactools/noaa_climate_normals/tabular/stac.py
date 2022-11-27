from datetime import datetime, timezone
from typing import List, Optional

import stac_table
from pystac import Collection, Item, Summaries
from pystac.extensions.item_assets import ItemAssetsExtension
from pystac.extensions.scientific import ScientificExtension
from pystac.extensions.table import TableExtension
from pystac.utils import datetime_to_str, make_absolute_href
from stactools.core.io import ReadHrefModifier

from ..constants import LANDING_PAGE_LINK, LICENSE_LINK, PROVIDERS
from . import constants
from .parquet import create_parquet, get_collection_tables, update_table_columns
from .utils import formatted_frequency, id_string


def create_item(
    csv_hrefs: List[str],
    frequency: constants.Frequency,
    period: constants.Period,
    geoparquet_dir: str,
    *,
    num_partitions: int = 5,
    geoparquet_href: Optional[str] = None,
    read_href_modifier: Optional[ReadHrefModifier] = None,
) -> Item:
    """Creates a STAC Item for GeoParquet data created from NOAA Climate
    Normal CSV files.

    Args:
        csv_hrefs (List[str]): List of HREFs to CSV files that will be
            converted to a single parquet file.
        frequency (Frequency): Temporal interval of CSV data, e.g., 'monthly'
            'hourly'.
        period (Period): Climate normal time period of CSV data, e.g.,
            '1991-2020'.
        geoparquet_dir (str): Directory to store created GeoParquet data.
        num_partitions (int): Number of parquet files to create.
        geoparquet_href (Optional[str]): HREF to an existing GeoParquet
            file or directory. New GeoParquet data will not be created from the
            `csv_hrefs` list if this HREF is supplied.
        read_href_modifier (Optional[ReadHrefModifier]): An optional function
            to modify an HREF, e.g., to add a token to a URL.

    Returns:
        Item: A STAC Item for a GeoParquet file containing weather stataion data.
    """
    start_year = int(period.value[0:4])
    end_year = int(period.value[5:])
    id = id_string(frequency, period)
    title = f"{formatted_frequency(frequency)} Climate Normals for Period {period}"

    if not geoparquet_href:
        geoparquet_href = create_parquet(
            csv_hrefs,
            frequency,
            period,
            geoparquet_dir,
            num_partitions=num_partitions,
            read_href_modifier=read_href_modifier,
        )

    item = Item(
        id=id,
        datetime=None,
        geometry=None,
        bbox=None,
        properties={
            "noaa-climate-normals:frequency": frequency,
            "noaa-climate-normals:period": period,
            "start_datetime": datetime_to_str(datetime(start_year, 1, 1, 0, 0, 0)),
            "end_datetime": datetime_to_str(datetime(end_year, 12, 31, 23, 59, 59)),
            "created": datetime_to_str(datetime.now(tz=timezone.utc)),
            "title": title,
        },
    )

    item = stac_table.generate(
        uri=make_absolute_href(geoparquet_href),
        template=item,
        infer_bbox="geometry",
        infer_geometry=False,
        asset_key="geoparquet",
    )
    item.properties.pop("proj:bbox")

    item = update_table_columns(item, frequency, period)

    scientific = ScientificExtension.ext(item, add_if_missing=True)
    if period is constants.Period.PERIOD_1981_2010:
        scientific.doi = constants.DATA_1981_2010["doi"]
        citation = constants.DATA_1981_2010["citation"]
        scientific.citation = citation.replace(
            "FREQUENCY", formatted_frequency(frequency)
        )
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


def create_collection() -> Collection:
    """Creates a STAC Collection for tabular data.

    Returns:
        Collection: A STAC Collection for tabular Items.
    """
    collection = Collection(**constants.COLLECTION)

    scientific = ScientificExtension.ext(collection, add_if_missing=True)
    scientific.publications = [
        constants.PUBLICATION_DAILY_MONTHLY_ANNUALSEASONAL,
        constants.PUBLICATION_HOURLY,
    ]

    collection.providers = PROVIDERS

    item_assets = ItemAssetsExtension.ext(collection, add_if_missing=True)
    item_assets.item_assets = constants.ITEM_ASSETS

    TableExtension.ext(collection, add_if_missing=True)
    collection.extra_fields["table:tables"] = get_collection_tables()

    collection.add_links([LANDING_PAGE_LINK, LICENSE_LINK])

    collection.summaries = Summaries(
        {
            "frequency": [f.value for f in constants.Frequency],
            "period": [p.value for p in constants.Period],
        }
    )

    return collection

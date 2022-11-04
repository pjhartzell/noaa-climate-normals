import logging
import os

import click
from click import Command, Group
from pystac import CatalogType

from .constants import Frequency, Period
from .parquet import create_parquet
from .stac import create_collection, create_item

logger = logging.getLogger(__name__)


def create_command(noaa_climate_normals: Group) -> Command:
    @noaa_climate_normals.group(
        "tabular",
        short_help=("Commands for working with tabular Climate Normals"),
    )
    def tabular() -> None:
        pass

    @tabular.command("create-geoparquet", short_help="Creates a GeoParquet file")
    @click.argument("file-list")
    @click.argument("frequency", type=click.Choice([f.value for f in Frequency]))
    @click.argument("period", type=click.Choice([p.value for p in Period]))
    @click.argument("destination")
    def create_geoparquet_command(
        file_list: str,
        frequency: str,
        period: str,
        destination: str,
    ) -> None:
        """
        Creates a GeoParquet file from CSV files listed in a text file.

        \b
        Args:
            file_list (str): Path to a text file containing HREFs to CSV files,
                one HREF per line.
            frequency (Frequency): Choice of 'hourly', 'daily', 'monthly', or
                'annualseasonal'.
            period (Period): Choice of '1981-2010', '1991-2020', or
                '2006-2020'.
            destination (str): Directory for the created GeoParquet file.
        """
        with open(file_list) as f:
            hrefs = [line.strip() for line in f.readlines()]
        if not os.path.exists(destination):
            os.mkdir(destination)
        parquet_path = create_parquet(
            csv_hrefs=hrefs,
            frequency=Frequency(frequency),
            period=Period(period),
            parquet_dir=destination,
        )
        click.echo(f"GeoParquet file created at {parquet_path}")

    @tabular.command("create-item", short_help="Creates a STAC Item")
    @click.argument("geoparquet-href")
    @click.argument("destination")
    def create_item_command(
        geoparquet_href: str,
        destination: str,
    ) -> None:
        """Creates a STAC Item for a GeoParquet file.

        \b
        Args:
            geoparquet_href (str): HREF to GeoParquet file.
            destination (str): Directory for the created Item JSON file.
        """
        item = create_item(geoparquet_href=geoparquet_href)
        item.set_self_href(os.path.join(destination, item.id + ".json"))
        item.make_asset_hrefs_relative()
        item.validate()
        item.save_object(include_self_link=False)

        return None

    @tabular.command("create-collection", short_help="Creates a STAC Collection")
    @click.argument("destination")
    def create_collection_command(
        destination: str,
    ) -> None:
        """Creates a STAC Collection for tabular Items.

        \b
        Args:
            destination (str): Path for the Collection JSON file.
        """
        collection = create_collection()
        collection.set_self_href(destination)
        collection.catalog_type = CatalogType.SELF_CONTAINED
        collection.validate()
        collection.save()

    return tabular

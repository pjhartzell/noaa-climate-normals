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

    @tabular.command("create-geoparquet", short_help="Creates GeoParquet data")
    @click.argument("file-list")
    @click.argument("frequency", type=click.Choice([f.value for f in Frequency]))
    @click.argument("period", type=click.Choice([p.value for p in Period]))
    @click.argument("destination")
    @click.option(
        "-n",
        "--num-partitions",
        type=int,
        help="number of geoparquet files to create",
        default=5,
        show_default=True,
    )
    def create_geoparquet_command(
        file_list: str,
        frequency: str,
        period: str,
        destination: str,
        num_partitions: int,
    ) -> None:
        """
        Creates GeoParquet data from CSV files listed in a text file.

        \b
        Args:
            file_list (str): Path to a text file containing HREFs to CSV files,
                one HREF per line.
            frequency (Frequency): Choice of 'hourly', 'daily', 'monthly', or
                'annualseasonal'.
            period (Period): Choice of '1981-2010', '1991-2020', or
                '2006-2020'.
            destination (str): Directory for the created GeoParquet data.
            num_partitions (int): Number of GeoParquet files to create.
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
            num_partitions=num_partitions,
        )
        click.echo(f"GeoParquet file created at {parquet_path}")

        return None

    @tabular.command("create-item", short_help="Creates a STAC Item")
    @click.argument("file-list")
    @click.argument("frequency", type=click.Choice([f.value for f in Frequency]))
    @click.argument("period", type=click.Choice([p.value for p in Period]))
    @click.option(
        "-n",
        "--num-partitions",
        type=int,
        help="number of geoparquet files to create",
        default=5,
        show_default=True,
    )
    @click.argument("destination")
    def create_item_command(
        file_list: str,
        frequency: str,
        period: str,
        destination: str,
        num_partitions: int,
    ) -> None:
        """Creates a STAC Item for tabular data from a single temporal frequency
        and normal period.

        The Item will contain GeoParquet asset created from CSV files listed in
        a text file. The Item and GeoParquet data are saved to the specified
        `destination`.

        \b
        Args:
            file_list (str): Path to a text file containing HREFs to CSV files,
                one HREF per line.
            frequency (Frequency): Choice of 'hourly', 'daily', 'monthly', or
                'annualseasonal'.
            period (Period): Choice of '1981-2010', '1991-2020', or
                '2006-2020'.
            destination (str): Directory for GeoParquet data and STAC Item.
            num_partitions (int): Number of GeoParquet files to create.
        """
        with open(file_list) as f:
            hrefs = [line.strip() for line in f.readlines()]

        if not os.path.exists(destination):
            os.mkdir(destination)

        item = create_item(
            hrefs,
            Frequency(frequency),
            Period(period),
            destination,
            num_partitions=num_partitions,
        )
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

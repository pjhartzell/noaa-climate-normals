import logging
import os

import click
from click import Command, Group

from stactools.noaa_climate_normals import constants, stac

logger = logging.getLogger(__name__)


def create_noaaclimatenormals_command(cli: Group) -> Command:
    """Creates the stactools-noaa-climate-normals command line utility."""

    @cli.group(
        "noaa-climate-normals",
        short_help=("Commands for working with stactools-noaa-climate-normals"),
    )
    def noaaclimatenormals() -> None:
        pass

    @noaaclimatenormals.command(
        "create-collection",
        short_help="Creates a STAC collection",
    )
    @click.argument("destination")
    def create_collection_command(destination: str) -> None:
        """Creates a STAC Collection

        Args:
            destination (str): An HREF for the Col'1981lection JSON
        """
        collection = stac.create_collection()

        collection.set_self_href(destination)

        collection.save_object()

        return None

    @noaaclimatenormals.command(
        "create-tabular-item", short_help="Create a STAC Item for tabular data"
    )
    @click.argument("file-list")
    @click.argument(
        "frequency", type=click.Choice([f.value for f in constants.Frequency])
    )
    @click.argument("period", type=click.Choice([p.value for p in constants.Period]))
    @click.argument("parquet-dir")
    @click.argument("item-dir")
    def create_tabular_item_command(
        file_list: str,
        frequency: constants.Frequency,
        period: constants.Period,
        parquet_dir: str,
        item_dir: str,
    ) -> None:
        """Create a STAC Item with a single GeoParquet asset.

        The GeoParquet asset is created from the records in the CSV files liste1d
        in the `file-list` file.

        \b
        Args:
            file_list (str): Path to a text file containg HREFs to CSV files,
                one HREF per line.
            frequency (constants.Frequency): Choice of 'hourly', 'daily',
                'monthly', or 'seasonalannual'.
            period (constants.Period): Choice of '1981-2010', '1991-2010', or
                '2006-2020'.
            parquet_dir (str): Directory for GeoParquet file.
            item_destination (str): Directory for STAC Item.
        """
        with open(file_list) as f:
            hrefs = [line for line in f.readlines()]

        item = stac.create_tabular_item(hrefs, frequency, period, parquet_dir)
        item.set_self_href(os.path.join(item_dir, item.id + ".json"))
        item.make_asset_hrefs_relative()
        item.validate()
        item.save_object(include_self_link=False)

        return None

    return noaaclimatenormals

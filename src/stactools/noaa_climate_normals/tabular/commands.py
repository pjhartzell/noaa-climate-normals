import logging
import os

import click
from click import Command, Group

from .constants import Frequency, Period
from .stac import create_item

logger = logging.getLogger(__name__)


def create_command(noaa_climate_normals: Group) -> Command:
    @noaa_climate_normals.group(
        "tabular",
        short_help=("Commands for working with tabular Climate Normals"),
    )
    def tabular() -> None:
        pass

    @tabular.command("create-item", short_help="Creates a STAC Item")
    @click.argument("file-list")
    @click.argument("frequency", type=click.Choice([f.value for f in Frequency]))
    @click.argument("period", type=click.Choice([p.value for p in Period]))
    @click.argument("destination")
    def create_item_command(
        file_list: str,
        frequency: str,
        period: str,
        destination: str,
    ) -> None:
        """Creates a STAC Item for tabular data from a single temporal frequency
        and normal period.

        The Item will contain a single GeoParquet asset created from CSV files
        listed in a text file. The Item and GeoParquet file are saved to the
        specified `destination`.

        \b
        Args:
            file_list (str): Path to a text file containing HREFs to CSV files,
                one HREF per line.
            frequency (Frequency): Choice of 'hourly', 'daily', 'monthly', or
                'annualseasonal'.
            period (Period): Choice of '1981-2010', '1991-2020', or
                '2006-2020'.
            destination (str): Directory for GeoParquet file and STAC Item.
        """
        with open(file_list) as f:
            hrefs = [line.strip() for line in f.readlines()]

        if not os.path.exists(destination):
            os.mkdir(destination)

        item = create_item(hrefs, Frequency(frequency), Period(period), destination)
        item.set_self_href(os.path.join(destination, item.id + ".json"))
        item.make_asset_hrefs_relative()
        item.validate()
        item.save_object(include_self_link=False)

        return None

    return tabular

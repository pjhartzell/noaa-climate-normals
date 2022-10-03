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

    # @noaaclimatenormals.command(
    #     "create-collection",
    #     short_help="Creates a STAC collection",
    # )
    # @click.argument("destination")
    # def create_collection_command(destination: str) -> None:
    #     """Creates a STAC Collection

    #     Args:
    #         destination (str): An HREF for the Col'1981lection JSON
    #     """
    #     collection = stac.create_collection()

    #     collection.set_self_href(destination)

    #     collection.save_object()

    #     return None

    @noaaclimatenormals.command(
        "create-tabular-item", short_help="Create a STAC Item for tabular data"
    )
    @click.argument("file-list")
    @click.argument(
        "frequency", type=click.Choice([f.value for f in constants.Frequency])
    )
    @click.argument("period", type=click.Choice([p.value for p in constants.Period]))
    @click.argument("output-dir")
    def create_tabular_item_command(
        file_list: str,
        frequency: str,
        period: str,
        output_dir: str,
    ) -> None:
        """Create a STAC Item for single frequency and normal period.

        The Item will contain a single GeoParquet asset created from a text file
        containing HREFs to source data CSV files. The Item and GeoParquet file
        are saved to the specified `output_dir`.

        \b
        Args:
            file_list (str): Path to a text file containing HREFs to CSV files,
                one HREF per line.
            frequency (constants.Frequency): Choice of 'hourly', 'daily',
                'monthly', or 'seasonalannual'.
            period (constants.Period): Choice of '1981-2010', '1991-2020', or
                '2006-2020'.
            output_dir (str): Directory for GeoParquet file and STAC Item.
        """
        with open(file_list) as f:
            hrefs = [line.strip() for line in f.readlines()]

        _frequency = constants.Frequency(frequency)
        _period = constants.Period(period)

        if not os.path.exists(output_dir):
            os.mkdir(output_dir)

        item = stac.create_tabular_item(hrefs, _frequency, _period, output_dir)
        item.set_self_href(os.path.join(output_dir, item.id + ".json"))
        item.make_asset_hrefs_relative()
        item.validate()
        item.save_object(include_self_link=False)

        return None

    return noaaclimatenormals

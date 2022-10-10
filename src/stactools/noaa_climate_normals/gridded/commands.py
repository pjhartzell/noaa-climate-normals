import logging
import os

import click
from click import Command, Group

from .constants import Frequency
from .stac import create_item

logger = logging.getLogger(__name__)


def create_command(noaa_climate_normals: Group) -> Command:
    @noaa_climate_normals.group(
        "gridded",
        short_help=("Commands for working with gridded Climate Normals"),
    )
    def gridded() -> None:
        pass

    @gridded.command("create-item", short_help="Creates a STAC Item")
    @click.argument("infile")
    @click.argument("frequency", type=click.Choice([f.value for f in Frequency]))
    @click.argument("destination")
    @click.option(
        "-t",
        "--time-index",
        help="Time index, e.g., month of year (1-12)",
        type=int,
        default=1,
    )
    @click.option(
        "-n",
        "--no-netcdf",
        is_flag=True,
        default=False,
        show_default=True,
        help="Exclude NetCDF source data assets from Item",
    )
    def create_item_command(
        infile: str, frequency: str, destination: str, time_index: int, no_netcdf: bool
    ) -> None:
        """Creates a STAC Item for a single timestep of gridded data.

        The Item will contain multiple COG assets, which will be saved alongside
        the Item to the specified `destination`.

        \b
        Args:
            infile (str): HREF to one of the source NetCDF files.
            frequency (str): Choice of 'daily', 'monthly', 'seasonal', or
                'annual'.
            destination (str): An HREF for the created Item and COGs.
            time_index (int): An integer index into the timestack of the
                chosen frequency. Valid values are 1-366 for daily data, 1-12
                for monthly data, and 1-4 for seasonal data. Not required for
                annual data.
            no_netcdf (bool): Flag to exclude NetCDF source data assets from
                Item. Default is False.
        """
        item = create_item(
            infile,
            Frequency(frequency),
            destination,
            time_index=time_index,
            no_netcdf_assets=no_netcdf,
        )
        item.set_self_href(os.path.join(destination, f"{item.id}.json"))
        item.make_asset_hrefs_relative()
        item.validate()
        item.save_object(include_self_link=False)

        return None

    return gridded

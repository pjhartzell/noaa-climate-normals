import os

import click
from click import Command, Group
from pystac import CatalogType

from .stac import create_collection, create_item


def create_command(noaa_climate_normals: Group) -> Command:
    @noaa_climate_normals.group(
        "netcdf",
        short_help=("Commands for working with gridded Climate Normals NetCDF Data"),
    )
    def netcdf() -> None:
        pass

    @netcdf.command("create-item", short_help="Creates a STAC Item")
    @click.argument("infile")
    @click.argument("destination")
    def create_item_command(infile: str, destination: str) -> None:
        """Creates a STAC Item for a NetCDF file containing gridded climate
        normal data.

        \b
        Args:
            infile (str): HREF to a NetCDF file.
            destination (str): Directory to store the created Item.
        """
        item = create_item(infile)
        item.set_self_href(os.path.join(destination, f"{item.id}.json"))
        item.make_asset_hrefs_relative()
        item.validate()
        item.save_object(include_self_link=False)

        return None

    @netcdf.command("create-collection", short_help="Creates a STAC Collection")
    @click.argument("destination")
    def create_collection_command(
        destination: str,
    ) -> None:
        """Creates a STAC Collection for NetCDF files containing gridded climate
        data.

        \b
        Args:
            destination (str): Path to the created collection JSON file.
        """
        collection = create_collection()
        collection.set_self_href(destination)
        collection.catalog_type = CatalogType.SELF_CONTAINED
        collection.validate()
        collection.save()

    return netcdf

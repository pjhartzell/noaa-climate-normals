from click import Command, Group

from .gridded.commands import create_command as create_gridded_command
from .tabular.commands import create_command as create_tabular_command


def create_noaa_climate_normals_command(cli: Group) -> Command:
    """Creates the stactools-noaa-climate-normals command line utility."""

    @cli.group(
        "noaa-climate-normals",
        short_help=("Commands for working with NOAA Climate Normal Data"),
    )
    def noaa_climate_normals() -> None:
        pass

    create_tabular_command(noaa_climate_normals)
    create_gridded_command(noaa_climate_normals)

    return noaa_climate_normals

import stactools.core
from stactools.cli.registry import Registry

from stactools.noaa_climate_normals.stac import create_tabular_item

__all__ = ["create_tabular_item"]

stactools.core.use_fsspec()


def register_plugin(registry: Registry) -> None:
    from stactools.noaa_climate_normals import commands

    registry.register_subcommand(commands.create_noaaclimatenormals_command)


__version__ = "0.1.0"

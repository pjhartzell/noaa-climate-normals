import stactools.core
from stactools.cli.registry import Registry

stactools.core.use_fsspec()


def register_plugin(registry: Registry) -> None:
    from . import commands

    registry.register_subcommand(commands.create_noaa_climate_normals_command)


__version__ = "0.1.0"

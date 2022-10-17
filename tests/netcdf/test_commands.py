import os
from tempfile import TemporaryDirectory
from typing import Callable, List

import pystac
from click import Command, Group
from stactools.testing.cli_test import CliTestCase

from stactools.noaa_climate_normals.commands import create_noaa_climate_normals_command
from tests import test_data


class CommandsTest(CliTestCase):
    def create_subcommand_functions(self) -> List[Callable[[Group], Command]]:
        return [create_noaa_climate_normals_command]

    def test_create_netcdf_item(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            path = test_data.get_path(
                "data-files/gridded/daily/prcp-2006_2020-daily-normals-v1.0.nc"
            )

            result = self.run_command(
                f"noaa-climate-normals netcdf create-item {path} {tmp_dir}"
            )
            assert result.exit_code == 0, "\n{}".format(result.output)

            item = pystac.read_file(
                os.path.join(tmp_dir, "prcp-2006_2020-daily-normals-v1.0.json")
            )
            item_dict = item.to_dict()
            assert item_dict["id"] == "prcp-2006_2020-daily-normals-v1.0"
            assert item_dict["assets"].get("netcdf", None) is not None

            item.validate()

    def test_create_netcdf_collection(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            result = self.run_command(
                f"noaa-climate-normals netcdf create-collection "
                f"{os.path.join(tmp_dir, 'collection.json')}"
            )
            assert result.exit_code == 0, "\n{}".format(result.output)

            collection = pystac.read_file(os.path.join(tmp_dir, "collection.json"))
            assert collection.id == "noaa-climate-normals-netcdf"

            collection.validate()

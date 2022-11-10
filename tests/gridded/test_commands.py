import glob
import os.path
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

    def test_create_gridded_item(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            nc_href = test_data.get_path(
                "data-files/gridded/daily/prcp-2006_2020-daily-normals-v1.0.nc"
            )

            result = self.run_command(
                f"noaa-climate-normals gridded create-item {nc_href} daily 1 "
                f"{tmp_dir}"
            )
            assert result.exit_code == 0, "\n{}".format(result.output)

            jsons = [p for p in os.listdir(tmp_dir) if p.endswith(".json")]
            assert len(jsons) == 1
            item = pystac.read_file(os.path.join(tmp_dir, "2006_2020-daily-001.json"))
            assert len(glob.glob(f"{tmp_dir}/*.tif")) == 6

            item.validate()

    def test_create_gridded_collection(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            result = self.run_command(
                f"noaa-climate-normals gridded create-collection "
                f"{os.path.join(tmp_dir, 'collection.json')}"
            )
            assert result.exit_code == 0, "\n{}".format(result.output)

            collection = pystac.read_file(os.path.join(tmp_dir, "collection.json"))
            assert collection.id == "noaa-climate-normals-gridded"

            collection.validate()

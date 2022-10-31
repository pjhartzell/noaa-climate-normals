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

    def test_create_geoparquet_and_tabular_item(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            file_list_path = f"{tmp_dir}/test_monthly_1981-2010.txt"
            with open(file_list_path, "w") as f:
                f.write(
                    test_data.get_path(
                        "data-files/tabular/monthly/1981-2010/USW00013740.csv\n"
                    )
                )
                f.write(
                    test_data.get_path(
                        "data-files/tabular/monthly/1981-2010/USW00094765.csv"
                    )
                )

            result = self.run_command(
                f"noaa-climate-normals tabular create-geoparquet {file_list_path} "
                f"monthly 1981-2010 {tmp_dir}"
            )
            assert result.exit_code == 0, "\n{}".format(result.output)
            geoparquet_path = os.path.join(tmp_dir, "1981_2010-monthly.parquet")
            assert os.path.isfile(geoparquet_path)

            result = self.run_command(
                f"noaa-climate-normals tabular create-item {geoparquet_path} {tmp_dir}"
            )
            assert result.exit_code == 0, "\n{}".format(result.output)

            jsons = [p for p in os.listdir(tmp_dir) if p.endswith(".json")]
            assert len(jsons) == 1
            item = pystac.read_file(os.path.join(tmp_dir, "1981_2010-monthly.json"))
            assert os.path.isfile(os.path.join(tmp_dir, f"{item.id}.parquet"))

            item.validate()

    def test_create_tabular_collection(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            result = self.run_command(
                f"noaa-climate-normals tabular create-collection "
                f"{os.path.join(tmp_dir, 'collection.json')}"
            )
            assert result.exit_code == 0, "\n{}".format(result.output)

            collection = pystac.read_file(os.path.join(tmp_dir, "collection.json"))
            assert collection.id == "noaa-climate-normals-tabular"

            collection.validate()

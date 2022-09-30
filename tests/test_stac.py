import os
from tempfile import TemporaryDirectory

from stactools.noaa_climate_normals import stac
from stactools.noaa_climate_normals.constants import Frequency, Period
from tests import test_data


def test_create_item() -> None:
    href1 = test_data.get_path("data-files/monthly/1981-2010/USW00013740.csv")
    href2 = test_data.get_path("data-files/monthly/1981-2010/USW00094765.csv")

    with TemporaryDirectory() as temp_dir:
        item = stac.create_tabular_item(
            [href1, href2],
            Frequency("monthly"),
            Period("1981-2010"),
            os.path.join(temp_dir, "temp.parquet"),
        )

    # Validate
    item.validate()

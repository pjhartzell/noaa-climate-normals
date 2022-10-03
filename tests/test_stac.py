import os
from tempfile import TemporaryDirectory

from stactools.noaa_climate_normals import stac
from stactools.noaa_climate_normals.constants import Frequency, Period
from tests import test_data


def test_create_tabular_item() -> None:
    href1 = test_data.get_path("data-files/monthly/1981-2010/USW00013740.csv")
    href2 = test_data.get_path("data-files/monthly/1981-2010/USW00094765.csv")

    with TemporaryDirectory() as tmp_dir:
        item = stac.create_tabular_item(
            [href1, href2],
            Frequency("monthly"),
            Period("1981-2010"),
            tmp_dir,
        )

        assert item.id == "monthly_1981-2010"
        assert os.path.isfile(os.path.join(tmp_dir, f"{item.id}.parquet"))

    # Validate
    item.validate()

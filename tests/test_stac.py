import os
from tempfile import TemporaryDirectory
from typing import List

import pytest

from stactools.noaa_climate_normals import stac
from stactools.noaa_climate_normals.constants import Frequency, Period
from tests import test_data

file_lists = [
    ["data-files/annualseasonal/1981-2010/USW00094765.csv"],
    ["data-files/annualseasonal/1991-2020/USW00094765.csv"],
    ["data-files/annualseasonal/2006-2020/USW00094745.csv"],
    ["data-files/daily/1981-2010/USW00094765.csv"],
    ["data-files/daily/1991-2020/USW00094765.csv"],
    ["data-files/daily/2006-2020/USW00094745.csv"],
    ["data-files/hourly/1981-2010/USW00094746.csv"],
    ["data-files/hourly/1991-2020/USW00094745.csv"],
    ["data-files/hourly/2006-2020/USW00094745.csv"],
    [
        "data-files/monthly/1981-2010/USW00013740.csv",
        "data-files/monthly/1981-2010/USW00094765.csv",
    ],
    ["data-files/monthly/1991-2020/USW00094765.csv"],
    ["data-files/monthly/2006-2020/USW00094765.csv"],
]


@pytest.mark.parametrize("file_list", file_lists)
def test_create_tabular_item(file_list: List[str]) -> None:
    frequency = file_list[0].split("/")[1]
    period = file_list[0].split("/")[2]

    hrefs = []
    for file in file_list:
        hrefs.append(test_data.get_path(file))

    with TemporaryDirectory() as tmp_dir:
        item = stac.create_tabular_item(
            hrefs,
            Frequency(frequency),
            Period(period),
            tmp_dir,
        )

        assert item.id == f"{frequency}_{period}"
        assert os.path.isfile(os.path.join(tmp_dir, f"{item.id}.parquet"))

    # Validate
    item.validate()

import os
from tempfile import TemporaryDirectory
from typing import List

import pytest

from stactools.noaa_climate_normals.tabular.constants import Frequency, Period
from stactools.noaa_climate_normals.tabular.stac import create_item
from tests import test_data

tabular_files = [
    ["data-files/tabular/annualseasonal/1981-2010/USW00094765.csv"],
    ["data-files/tabular/annualseasonal/1991-2020/USW00094765.csv"],
    ["data-files/tabular/annualseasonal/2006-2020/USW00094745.csv"],
    ["data-files/tabular/daily/1981-2010/USW00094765.csv"],
    ["data-files/tabular/daily/1991-2020/USW00094765.csv"],
    ["data-files/tabular/daily/2006-2020/USW00094745.csv"],
    ["data-files/tabular/hourly/1981-2010/USW00094746.csv"],
    ["data-files/tabular/hourly/1991-2020/USW00094745.csv"],
    ["data-files/tabular/hourly/2006-2020/USW00094745.csv"],
    [
        "data-files/tabular/monthly/1981-2010/USW00013740.csv",
        "data-files/tabular/monthly/1981-2010/USW00094765.csv",
    ],
    ["data-files/tabular/monthly/1991-2020/USW00094765.csv"],
    ["data-files/tabular/monthly/2006-2020/USW00094765.csv"],
]


@pytest.mark.parametrize("file_list", tabular_files)
def test_create_tabular_item(file_list: List[str]) -> None:
    frequency = file_list[0].split("/")[2]
    period = file_list[0].split("/")[3]

    hrefs = []
    for file in file_list:
        hrefs.append(test_data.get_path(file))

    with TemporaryDirectory() as tmp_dir:
        item = create_item(
            csv_hrefs=hrefs,
            frequency=Frequency(frequency),
            period=Period(period),
            geoparquet_dir=tmp_dir,
        )
        assert item.id == f"{period.replace('-', '_')}-{frequency}"
        assert os.path.isfile(os.path.join(tmp_dir, f"{item.id}.parquet"))

    item.validate()


def test_existing_parquet() -> None:
    with TemporaryDirectory() as tmp_dir:
        href = test_data.get_path(
            "data-files/tabular/monthly/1981-2010/1981_2010-monthly.parquet"
        )
        item = create_item(
            csv_hrefs=[],
            frequency=Frequency("monthly"),
            period=Period("1981-2010"),
            geoparquet_dir=tmp_dir,
            geoparquet_href=href,
        )
        assert item.id == "1981_2010-monthly"
        parquets = [p for p in os.listdir(tmp_dir) if p.endswith(".parquet")]
        assert not parquets

    item.validate()

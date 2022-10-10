import glob
import os
from tempfile import TemporaryDirectory
from typing import List

import pytest

from stactools.noaa_climate_normals import gridded_constants, stac
from stactools.noaa_climate_normals.constants import Frequency, Period
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
        item = stac.create_tabular_item(
            hrefs,
            Frequency(frequency),
            Period(period),
            tmp_dir,
        )
        assert item.id == f"{frequency}_{period}"
        assert os.path.isfile(os.path.join(tmp_dir, f"{item.id}.parquet"))

    item.validate()


def test_create_daily_gridded_item() -> None:
    path = test_data.get_path(
        "data-files/gridded/daily/prcp-2006_2020-daily-normals-v1.0.nc"
    )

    with TemporaryDirectory() as tmp_dir:
        item = stac.create_gridded_item(
            path, gridded_constants.Frequency("daily"), tmp_dir, time_index=1
        )
        assert item.id == "2006_2020-daily-1"
        assert len(glob.glob(f"{tmp_dir}/*.tif")) == 6

    item.validate()


def get_external_monthly_data() -> None:
    for var in gridded_constants.MONTHLY_FILES:
        filename = f"{var}-1991_2020-monthly-normals-v1.0.nc"
        test_data.get_external_data(filename)


def test_create_monthly_gridded_item() -> None:
    get_external_monthly_data()
    nc_href = test_data.get_external_data("prcp-1991_2020-monthly-normals-v1.0.nc")

    with TemporaryDirectory() as tmp_dir:
        item = stac.create_gridded_item(
            nc_href, gridded_constants.Frequency.MLY, tmp_dir, time_index=1
        )
        assert item.id == "1991_2020-monthly-1"
        assert len(glob.glob(f"{tmp_dir}/*.tif")) == 20

    item.validate()


def test_create_seasonal_gridded_item() -> None:
    get_external_monthly_data()
    nc_href = test_data.get_external_data("prcp-1991_2020-monthly-normals-v1.0.nc")

    with TemporaryDirectory() as tmp_dir:
        item = stac.create_gridded_item(
            nc_href, gridded_constants.Frequency.SEAS, tmp_dir, time_index=1
        )
        assert item.id == "1991_2020-seasonal-1"
        assert len(glob.glob(f"{tmp_dir}/*.tif")) == 20

    item.validate()


def test_create_annual_gridded_item() -> None:
    get_external_monthly_data()
    nc_href = test_data.get_external_data("prcp-1991_2020-monthly-normals-v1.0.nc")

    with TemporaryDirectory() as tmp_dir:
        item = stac.create_gridded_item(
            nc_href, gridded_constants.Frequency.ANN, tmp_dir, time_index=1
        )
        assert item.id == "1991_2020-annual"
        assert len(glob.glob(f"{tmp_dir}/*.tif")) == 20

    item.validate()

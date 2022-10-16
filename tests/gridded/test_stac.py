import glob
from tempfile import TemporaryDirectory

from stactools.noaa_climate_normals.gridded.constants import Frequency
from stactools.noaa_climate_normals.gridded.stac import create_item
from tests import test_data

from .. import MONTHLY_FILES


def test_create_daily_gridded_item() -> None:
    path = test_data.get_path(
        "data-files/gridded/daily/prcp-2006_2020-daily-normals-v1.0.nc"
    )

    with TemporaryDirectory() as tmp_dir:
        item = create_item(path, Frequency("daily"), tmp_dir, time_index=1)
        assert item.id == "2006_2020-daily-1"
        assert len(glob.glob(f"{tmp_dir}/*.tif")) == 6

    item.validate()


def get_external_monthly_data() -> None:
    for var in MONTHLY_FILES:
        filename = f"{var}-1991_2020-monthly-normals-v1.0.nc"
        test_data.get_external_data(filename)


def test_create_monthly_gridded_item() -> None:
    get_external_monthly_data()
    nc_href = test_data.get_external_data("prcp-1991_2020-monthly-normals-v1.0.nc")

    with TemporaryDirectory() as tmp_dir:
        item = create_item(nc_href, Frequency.MLY, tmp_dir, time_index=1)
        assert item.id == "1991_2020-monthly-1"
        assert len(glob.glob(f"{tmp_dir}/*.tif")) == 20

    item.validate()


def test_create_seasonal_gridded_item() -> None:
    get_external_monthly_data()
    nc_href = test_data.get_external_data("prcp-1991_2020-monthly-normals-v1.0.nc")

    with TemporaryDirectory() as tmp_dir:
        item = create_item(nc_href, Frequency.SEAS, tmp_dir, time_index=1)
        assert item.id == "1991_2020-seasonal-1"
        assert len(glob.glob(f"{tmp_dir}/*.tif")) == 20

    item.validate()


def test_create_annual_gridded_item() -> None:
    get_external_monthly_data()
    nc_href = test_data.get_external_data("prcp-1991_2020-monthly-normals-v1.0.nc")

    with TemporaryDirectory() as tmp_dir:
        item = create_item(nc_href, Frequency.ANN, tmp_dir, time_index=1)
        assert item.id == "1991_2020-annual"
        assert len(glob.glob(f"{tmp_dir}/*.tif")) == 20

    item.validate()

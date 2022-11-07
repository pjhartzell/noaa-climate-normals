import glob
import os
from tempfile import TemporaryDirectory

from pystac import Item

from stactools.noaa_climate_normals.gridded.constants import Frequency
from stactools.noaa_climate_normals.gridded.stac import create_item
from tests import test_data

from .. import MONTHLY_FILES


def get_external_monthly_data() -> None:
    for var in MONTHLY_FILES:
        filename = f"{var}-1991_2020-monthly-normals-v1.0.nc"
        test_data.get_external_data(filename)


def test_create_daily_gridded_item() -> None:
    path = test_data.get_path(
        "data-files/gridded/daily/prcp-2006_2020-daily-normals-v1.0.nc"
    )

    with TemporaryDirectory() as tmp_dir:
        item = create_item(path, Frequency("daily"), 1, tmp_dir)
        assert item.id == "2006_2020-daily-1"
        assert len(glob.glob(f"{tmp_dir}/*.tif")) == 6

    item.validate()


def test_create_monthly_gridded_item() -> None:
    get_external_monthly_data()
    nc_href = test_data.get_external_data("prcp-1991_2020-monthly-normals-v1.0.nc")

    with TemporaryDirectory() as tmp_dir:
        item = create_item(nc_href, Frequency.MLY, 1, tmp_dir)
        assert item.id == "1991_2020-monthly-1"
        assert len(glob.glob(f"{tmp_dir}/*.tif")) == 20

    item.validate()


def test_create_seasonal_gridded_item() -> None:
    get_external_monthly_data()
    nc_href = test_data.get_external_data("prcp-1991_2020-monthly-normals-v1.0.nc")

    with TemporaryDirectory() as tmp_dir:
        item = create_item(nc_href, Frequency.SEAS, 1, tmp_dir)
        assert item.id == "1991_2020-seasonal-1"
        assert len(glob.glob(f"{tmp_dir}/*.tif")) == 20

    item.validate()


def test_create_annual_gridded_item() -> None:
    get_external_monthly_data()
    nc_href = test_data.get_external_data("prcp-1991_2020-monthly-normals-v1.0.nc")

    with TemporaryDirectory() as tmp_dir:
        item = create_item(nc_href, Frequency.ANN, 1, tmp_dir)
        assert item.id == "1991_2020-annual"
        assert len(glob.glob(f"{tmp_dir}/*.tif")) == 20

    item.validate()


def test_derived_from_links() -> None:
    get_external_monthly_data()
    monthly_nc_href = test_data.get_external_data(
        "prcp-1991_2020-monthly-normals-v1.0.nc"
    )
    daily_nc_href = test_data.get_path(
        "data-files/gridded/daily/prcp-2006_2020-daily-normals-v1.0.nc"
    )

    def count_links(_item: Item) -> int:
        count = 0
        for link in _item.links:
            if link.rel == "derived_from":
                count += 1
        return count

    with TemporaryDirectory() as tmp_dir:
        item = create_item(
            monthly_nc_href,
            Frequency.MLY,
            1,
            tmp_dir,
            api_url_netcdf=(
                "https://planetarycomputer.microsoft.com/api/stac/v1/"
                "collections/noaa-climate-normals-netcdf/items/"
            ),
        )
        assert count_links(item) == 4

        item = create_item(
            daily_nc_href,
            Frequency.DAILY,
            1,
            tmp_dir,
            api_url_netcdf=(
                "https://planetarycomputer.microsoft.com/api/stac/v1/"
                "collections/noaa-climate-normals-netcdf/items/"
            ),
        )
        assert count_links(item) == 6

    item.validate()


def test_existing_cogs() -> None:
    nc_href = test_data.get_path(
        "data-files/gridded/daily/prcp-2006_2020-daily-normals-v1.0.nc"
    )
    cog_hrefs = [
        test_data.get_path(
            "data-files/gridded/daily/2006_2020-daily-dlyprcp_norm-1.tif"
        ),
        test_data.get_path(
            "data-files/gridded/daily/2006_2020-daily-dlytavg_norm-1.tif"
        ),
    ]

    with TemporaryDirectory() as tmp_dir:
        item = create_item(nc_href, Frequency.DAILY, 1, tmp_dir, cog_hrefs=cog_hrefs)
        assert item.id == "2006_2020-daily-1"
        cogs = [p for p in os.listdir(tmp_dir) if p.endswith(".tif")]
        assert len(cogs) == 4

    item.validate()

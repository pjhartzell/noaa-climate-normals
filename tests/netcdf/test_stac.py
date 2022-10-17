from stactools.noaa_climate_normals.netcdf.stac import create_item
from tests import test_data


def test_create_item() -> None:
    path = test_data.get_path(
        "data-files/gridded/daily/prcp-2006_2020-daily-normals-v1.0.nc"
    )

    item = create_item(path)
    assert item.id == "prcp-2006_2020-daily-normals-v1.0"
    assert item.assets.get("netcdf", None) is not None

    item.validate()

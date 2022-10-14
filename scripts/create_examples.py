#!/usr/bin/env python3

"""Creates the example STAC metadata, COGS, and GeoParquet.

Assumptions:
- The test suite has been run, so all external data have been downloaded.
"""

import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

import stactools.core.copy
from pystac import Catalog, CatalogType

from stactools.noaa_climate_normals.gridded import constants as gridded_constants
from stactools.noaa_climate_normals.gridded import stac as gridded_stac
from stactools.noaa_climate_normals.tabular import constants as tabular_constants
from stactools.noaa_climate_normals.tabular import stac as tabular_stac

root = Path(__file__).parent.parent
examples = root / "examples"
data_files = root / "tests" / "data-files"
external_data = root / "tests" / "data-files" / "external"

description = (
    "The U.S. Climate Normals are a large suite of data products that provide "
    "information about typical climate conditions for thousands of locations "
    "across the United States. Normals act both as a ruler to compare today's "
    "weather and tomorrow's forecast, and as a predictor of conditions in the "
    "near future. The official normals are calculated for a uniform 30 year "
    "period, and consist of annual/seasonal, monthly, daily, and hourly averages "
    "and statistics of temperature, precipitation, and other climatological "
    "variables from almost 15,000 U.S. weather stations."
)

with TemporaryDirectory() as tmp_dir:
    catalog = Catalog("noaa-climate-normals", description, "NOAA U.S. Climate Normals")

    print("Creating tabular collection...")
    tabular = tabular_stac.create_collection()
    csv_file_lists = {
        "annualseasonal": [
            str(data_files / "tabular/annualseasonal/1981-2010/USW00094765.csv")
        ],
        "daily": [str(data_files / "tabular/daily/1981-2010/USW00094765.csv")],
        "hourly": [str(data_files / "tabular/hourly/1981-2010/USW00094746.csv")],
        "monthly": [
            str(data_files / "tabular/monthly/1981-2010/USW00013740.csv"),
            str(data_files / "tabular/monthly/1981-2010/USW00094765.csv"),
        ],
    }
    for key, value in csv_file_lists.items():
        tabular_item = tabular_stac.create_item(
            csv_hrefs=value,
            frequency=tabular_constants.Frequency(key),
            period=tabular_constants.Period("1981-2010"),
            parquet_dir=tmp_dir,
        )
        tabular.add_item(tabular_item)
        tabular.update_extent_from_items()
    catalog.add_child(tabular)

    print("Creating gridded collection...")
    gridded = gridded_stac.create_collection()
    gridded_item = gridded_stac.create_item(
        nc_href=str(
            data_files / "gridded" / "daily" / "prcp-2006_2020-daily-normals-v1.0.nc"
        ),
        frequency=gridded_constants.Frequency("daily"),
        cog_dir=tmp_dir,
        time_index=1,
        netcdf_assets=False,
    )
    gridded.add_item(gridded_item)
    for frequency in ["monthly", "seasonal", "annual"]:
        gridded_item = gridded_stac.create_item(
            nc_href=str(external_data / "prcp-1991_2020-monthly-normals-v1.0.nc"),
            frequency=gridded_constants.Frequency(frequency),
            cog_dir=tmp_dir,
            time_index=1,
            netcdf_assets=False,
        )
        gridded.add_item(gridded_item)
        gridded.update_extent_from_items()
    catalog.add_child(gridded)

    print("Saving catalog")
    catalog.normalize_hrefs(str(examples))
    shutil.rmtree(examples)
    for item in catalog.get_all_items():
        for asset in item.assets.values():
            if asset.href.startswith(tmp_dir):
                href = stactools.core.copy.move_asset_file_to_item(
                    item, asset.href, copy=False
                )
                asset.href = href
        item.make_asset_hrefs_relative()
    catalog.save(catalog_type=CatalogType.SELF_CONTAINED)

    print("Done!")

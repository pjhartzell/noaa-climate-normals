import logging
import os
from datetime import datetime, timezone
from typing import List

from pystac import Asset, Item
from pystac.extensions.projection import ProjectionExtension
from pystac.extensions.table import TableExtension
from pystac.utils import datetime_to_str, make_absolute_href

from stactools.noaa_climate_normals import constants
from stactools.noaa_climate_normals.parquet import create_parquet

logger = logging.getLogger(__name__)


def create_tabular_item(
    csv_hrefs: List[str],
    _frequency: constants.Frequency,
    _period: constants.Period,
    parquet_dir: str,
) -> Item:

    frequency = constants.Frequency(_frequency)
    period = constants.Period(_period)
    start_year = int(period.value[0:4])
    end_year = int(period.value[5:])

    id = f"{frequency}_{period}"
    title = (
        f"{frequency.capitalize()} NOAA US Climate Normals for Period {period.value}"
    )

    parquet_dict = create_parquet(
        csv_hrefs, frequency, os.path.join(parquet_dir, f"{id}.parquet")
    )
    geometry = parquet_dict.pop("geometry")
    bbox = parquet_dict.pop("bbox")

    item = Item(
        id=id,
        geometry=geometry,
        bbox=bbox,
        datetime=None,
        properties={
            "noaa-climate-normals:frequency": frequency.value,
            "noaa-climate-normals:period": period.value,
            "start_datetime": datetime_to_str(datetime(start_year, 1, 1, 0, 0, 0)),
            "end_datetime": datetime_to_str(datetime(end_year, 12, 31, 23, 59, 59)),
            "created": datetime_to_str(datetime.now(tz=timezone.utc)),
            "title": title,
        },
    )

    parquet_dict["href"] = make_absolute_href(parquet_dict["href"])
    item.add_asset("parquet", Asset.from_dict(parquet_dict))
    TableExtension.ext(item.assets["parquet"], add_if_missing=True)

    projection = ProjectionExtension.ext(item, add_if_missing=True)
    projection.epsg = int(constants.CRS[5:])

    return item


def create_gridded_item() -> None:
    return None


# import glob
# import json
# path = "../climate-normals-scratch/normals-monthly/1981-2010/access"
# csv_files = glob.glob(path + "/*.csv")
# item = create_tabular_item(csv_files, "monthly", "1981-2010", ".")
# print(json.dumps(item.to_dict(), indent=4))

import json
import logging
from typing import Any, Dict, List

import geopandas as gpd
import pandas as pd
import pkg_resources
from shapely.geometry import mapping

from . import constants

logger = logging.getLogger(__name__)


def create_parquet(
    csv_hrefs: List[str],
    frequency: constants.Frequency,
    period: constants.Period,
    parquet_path: str,
) -> Dict[str, Any]:
    geodataframe = csv_to_geodataframe(csv_hrefs)
    geodataframe = make_categorical(geodataframe)
    columns = geodataframe_columns(geodataframe, frequency, period)
    geodataframe_dict = {
        "geometry": mapping(geodataframe.unary_union.convex_hull),
        "bbox": list(geodataframe.total_bounds),
        "href": parquet_path,
        "type": constants.PARQUET_MEDIA_TYPE,
        "title": constants.PARQUET_ASSET_TITLE,
        "table:primary_geometry": constants.PARQUET_GEOMETRY_COL,
        "table:columns": columns,
        "table:row_count": geodataframe.shape[0],
        "roles": ["data", "cloud-optimized"],
    }

    geodataframe.to_parquet(parquet_path)

    return geodataframe_dict


def make_categorical(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    substrings = [
        "_attributes",
        "comp_flag_",
        "meas_flag",
        "wind-1stdir",
        "wind-2nddir",
    ]
    for column in gdf.columns:
        if any([substring in column for substring in substrings]):
            gdf[column] = gdf[column].astype("category")
    return gdf


def csv_to_geodataframe(csv_hrefs: List[str]) -> gpd.GeoDataFrame:
    dataframes = (pd.read_csv(href) for href in csv_hrefs)
    dataframe = pd.concat(dataframes, ignore_index=True).copy()
    dataframe.columns = dataframe.columns.str.lower()
    return gpd.GeoDataFrame(
        data=dataframe,
        geometry=gpd.points_from_xy(
            x=dataframe.longitude,
            y=dataframe.latitude,
            z=dataframe.elevation,
            crs=constants.CRS,
        ),
    ).convert_dtypes()


def geodataframe_columns(
    geodataframe: gpd.GeoDataFrame,
    frequency: constants.Frequency,
    period: constants.Period,
) -> List[Dict[str, Any]]:
    column_metadata = load_column_metadata(frequency, period)
    columns = []
    for column, dtype in zip(geodataframe.columns, geodataframe.dtypes):
        temp = {
            "name": column,
            "type": dtype.name.lower(),
        }

        description = column_metadata.get(column, {}).get("description")
        if description:
            temp["description"] = description
        if not description:
            if "_attributes" in column or "comp_flag_" in column:
                temp["description"] = "Data record completeness flag"
            elif "meas_flag" in column:
                temp["description"] = "Data record measurement flag"
            elif "years_" in column:
                temp["description"] = "Number of years used"
            else:
                logger.warning(f"{frequency}_{period}: column '{column}' is missing")

        unit = column_metadata.get(column, {}).get("unit")
        if unit:
            temp["unit"] = unit

        columns.append(temp)

    return columns


def load_column_metadata(
    frequency: constants.Frequency, period: constants.Period
) -> Any:
    try:
        with pkg_resources.resource_stream(
            "stactools.noaa_climate_normals.tabular.parquet",
            f"column_metadata/{frequency}_{period}.json",
        ) as stream:
            return json.load(stream)
    except FileNotFoundError as e:
        raise e

import json
from typing import Any, Dict, List

import geopandas as gpd
import pandas as pd
import pkg_resources
from shapely.geometry import mapping

from stactools.noaa_climate_normals import constants


def create_parquet(csv_hrefs: List[str], frequency: str, parquet_path: str) -> Dict[str, Any]:
    geodataframe = csv_to_geodataframe(csv_hrefs)
    columns = geodataframe_columns(geodataframe, frequency)
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


def csv_to_geodataframe(csv_hrefs) -> gpd.GeoDataFrame:
    dataframes = (pd.read_csv(href) for href in csv_hrefs)
    dataframe = pd.concat(dataframes, ignore_index=True).copy()
    return gpd.GeoDataFrame(
        data=dataframe,
        geometry=gpd.points_from_xy(
            x=dataframe.LONGITUDE,
            y=dataframe.LATITUDE,
            z=dataframe.ELEVATION,
            crs=constants.CRS,
        ),
    ).convert_dtypes()


def geodataframe_columns(
    geodataframe: gpd.GeoDataFrame, frequency: str
) -> List[Dict[str, Any]]:
    column_metadata = load_column_metadata(frequency)
    columns = []
    for column, dtype in zip(geodataframe.columns, geodataframe.dtypes):
        temp = {
            "name": column,
            "type": dtype.name.lower(),
        }
        description = column_metadata.get(column.lower(), {}).get("description")
        if description:
            temp["description"] = description
        unit = column_metadata.get(column.lower(), {}).get("unit")
        if unit:
            temp["unit"] = unit
        columns.append(temp)
    return columns


def load_column_metadata(frequency: str) -> Dict[str, Any]:
    try:
        with pkg_resources.resource_stream(
            "stactools.noaa_climate_normals.parquet", f"column_metadata/{frequency}.json"
        ) as stream:
            return json.load(stream)
    except FileNotFoundError as e:
        raise e

import json
import logging
from typing import Any, Dict, List

import geopandas as gpd
import pandas as pd
import pkg_resources
import pyarrow.parquet as pq
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
    make_categorical(geodataframe)
    geodataframe.to_parquet(parquet_path)
    parquet_dtypes = get_parquet_dtypes(parquet_path)
    columns = geodataframe_columns(geodataframe, parquet_dtypes, frequency, period)

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

    return geodataframe_dict


def get_parquet_dtypes(parquet_path: str) -> Dict[str, str]:
    ds = pq.ParquetDataset(parquet_path, use_legacy_dataset=False)
    return {
        col.name.lower(): col.physical_type.lower()
        for col in ds.fragments[0].metadata.schema
    }


def make_categorical(gdf: gpd.GeoDataFrame) -> None:
    categorical_substrings = [
        "_attributes",
        "comp_flag_",
        "meas_flag",
        "wind-1stdir",
        "wind-2nddir",
    ]
    for column in gdf.columns:
        if any([substring in column for substring in categorical_substrings]):
            gdf[column] = gdf[column].astype("category")


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
    )


def geodataframe_columns(
    geodataframe: gpd.GeoDataFrame,
    parquet_dtypes: Dict[str, str],
    frequency: constants.Frequency,
    period: constants.Period,
) -> List[Dict[str, Any]]:
    column_metadata = load_column_metadata(frequency, period)
    columns = []
    for column in geodataframe.columns:
        temp = {}

        temp["name"] = column

        data_type = parquet_dtypes.get(column, None)
        if data_type:
            temp["type"] = data_type
        else:
            logger.warning(
                f"{frequency}_{period}: data type for column '{column}' is missing"
            )

        description = column_metadata.get(column, {}).get("description")
        if description:
            temp["description"] = description
        else:
            if "_attributes" in column or "comp_flag_" in column:
                temp["description"] = "Data record completeness flag"
            elif "meas_flag" in column:
                temp["description"] = "Data record measurement flag"
            elif "years_" in column:
                temp["description"] = "Number of years used"
            else:
                logger.warning(
                    f"{frequency}_{period}: description for column '{column}' is missing"
                )

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


def get_tables() -> Dict[str, str]:
    tables = {}
    idx = 0
    for period in constants.Period:
        for frequency in constants.Frequency:
            tables[idx] = {}
            tables[idx]["name"] = f"{period.value.replace('-', '_')}-{frequency}"

            if frequency is constants.Frequency.ANNUALSEASONAL:
                formatted_frequency = "Annual/Seasonal"
            else:
                formatted_frequency = frequency.value.capitalize()

            tables[idx][
                "description"
            ] = f"{formatted_frequency} Climate Normals for Period {period}"

            idx += 1

    return tables

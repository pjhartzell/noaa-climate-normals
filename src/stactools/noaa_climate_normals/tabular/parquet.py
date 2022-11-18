import json
import logging
from typing import Any, Dict, List, Optional
from warnings import simplefilter
import os

import dask
import dask.dataframe
import dask_geopandas
import pyarrow
import geopandas
import pandas
import pkg_resources
from stactools.core.io import ReadHrefModifier

from ..utils import modify_href
from . import constants
from .utils import formatted_frequency, id_string

logger = logging.getLogger(__name__)


def pandas_datatypes(
    frequency: constants.Frequency, period: constants.Period
) -> Dict[str, Any]:
    """Generates a dictionary of pandas datatypes for the CSV file columns from
    metadata stored in JSON files.

    Args:
        frequency (Frequency): Temporal interval of CSV data, e.g., 'monthly' or
            'hourly'.
        period (Period): Climate normal time period of CSV data, e.g.,
            '1991-2020'.

    Returns:
        Dict[str, Any]: Dictionary mapping column names to pandas datatypes.
    """
    column_metadata = load_column_metadata(frequency, period)
    dtypes = {}
    for key, value in column_metadata.items():
        if value["pandas_dtype"] == "category":
            dtypes[key] = pandas.CategoricalDtype(value["categories"], ordered=False)
        else:
            dtypes[key] = value["pandas_dtype"]
    return dtypes


def create_parquet(
    csv_hrefs: List[str],
    frequency: constants.Frequency,
    period: constants.Period,
    parquet_dir: str,
    read_href_modifier: Optional[ReadHrefModifier] = None,
) -> str:
    """Creates a GeoParquet file from a list of CSV files.

    Args:
        csv_hrefs (List[str]): List of HREFs to CSV files that will be
            converted to a single parquet file.
        frequency (Frequency): Temporal interval of CSV data, e.g., 'monthly' or
            'hourly'.
        period (Period): Climate normal time period of CSV data, e.g.,
            '1991-2020'.
        parquet_dir (str): Directory for created parquet files.
        read_href_modifier (Optional[ReadHrefModifier]): An optional function
            to modify an HREF, e.g., to add a token to a URL.

    Returns:
        str: Path to directory containing one or more parquet partitions.
    """

    @dask.delayed
    def dataframe_from_csv(
        csv_href: str, pd_dtypes: Dict[str, Any], empty_df: pandas.DataFrame
    ) -> pandas.DataFrame:
        """Returns a dataframe with an ordered, complete set of columns."""
        df = pandas.read_csv(csv_href, dtype=pd_dtypes)
        df = pandas.concat([df, empty_df])[empty_df.columns]
        df["geometry"] = geopandas.points_from_xy(
            x=df.LONGITUDE,
            y=df.LATITUDE,
            z=df.ELEVATION,
            crs=constants.CRS,
        )
        return df

    read_csv_hrefs = [
        modify_href(csv_href, read_href_modifier) for csv_href in csv_hrefs
    ]

    sorted_csv_hrefs = sorted(read_csv_hrefs)
    pd_dtypes = pandas_datatypes(frequency, period)
    empty_df = pandas.DataFrame(
        {c: pandas.Series(dtype=t) for c, t in pd_dtypes.items()}
    )

    pandas_dataframes = [
        dataframe_from_csv(csv_href, pd_dtypes, empty_df)
        for csv_href in sorted_csv_hrefs
    ]
    dask_dataframe = dask.dataframe.from_delayed(pandas_dataframes, meta=empty_df)
    dask_geodataframe = dask_geopandas.from_dask_dataframe(dask_dataframe)

    num_partitions = (
        5
        if frequency is constants.Frequency.DAILY
        or frequency is constants.Frequency.MONTHLY
        else 1
    )

    parquet_path = os.path.join(parquet_dir, f"{id_string(frequency, period)}.parquet")

    parquet_schema = create_parquet_schema(empty_df, frequency, period)

    simplefilter(action="ignore", category=pandas.errors.PerformanceWarning)
    dask_geodataframe.repartition(num_partitions).to_parquet(
        parquet_path, schema=parquet_schema, engine="pyarrow", write_index=False
    )

    return parquet_path


def create_parquet_schema(
    empty_df: pandas.DataFrame,
    frequency: constants.Frequency,
    period: constants.Period,
) -> pyarrow.Schema:

    print(empty_df.dtypes)
    schema = pyarrow.Schema.from_pandas(empty_df, preserve_index=False)
    schema = schema.with_metadata(
        {
            "description": (
                f"{formatted_frequency(frequency)} Climate Normals for "
                f"Period {period}"
            )
        }
    )

    column_metadata = load_column_metadata(frequency, period)
    for column in schema.names:
        field_metadata = {}
        field_metadata["description"] = column_metadata[column]["description"]
        unit = column_metadata[column].get("unit", False)
        if unit:
            field_metadata["unit"] = unit

        index = schema.get_field_index(column)
        field = schema.field(index).with_metadata(field_metadata)
        schema = schema.set(index, field)

    return schema


# def parquet_metadata(
#     parquet_path: str,
#     frequency: constants.Frequency,
#     period: constants.Period,
# ) -> Dict[str, Any]:
#     """Generate metadata from GeoParquet data for use in STAC Asset and Item
#     creation.

#     Args:
#         parquet_path (str): Path to GeoParquet data, either a GeoParquet file or
#             a directory of partioned GeoParquet files.
#         frequency (Frequency): Temporal interval of CSV data, e.g., 'monthly' or
#             'hourly'.
#         period (Period): Climate normal time period of CSV data, e.g.,
#             '1991-2020'.
#         read_href_modifier (Optional[ReadHrefModifier]): An optional function
#             to modify an HREF, e.g., to add a token to a URL.

#     Returns:
#         Dict[str, Any]: Dictionary of metadata for asset and Item creation.
#     """
#     metadata = pq.read_metadata(parquet_href)
#     columns_types = {col.name: col.physical_type for col in metadata.schema}
#     num_rows = metadata.num_rows

#     geodataframe = geopandas.read_parquet(parquet_href, columns=["geometry"])
#     convex_hull = geodataframe.unary_union.convex_hull
#     geometry = mapping(convex_hull)
#     bbox = list(convex_hull.bounds)

#     return {
#         "href": make_absolute_href(parquet_href),
#         "geometry": geometry,
#         "bbox": bbox,
#         "type": constants.PARQUET_MEDIA_TYPE,
#         "title": constants.PARQUET_ASSET_TITLE,
#         "table:primary_geometry": constants.PARQUET_GEOMETRY_COL,
#         "table:columns": parquet_columns(columns_types, frequency, period),
#         "table:row_count": num_rows,
#         "roles": ["data"],
#     }


# def parquet_columns(
#     columns_types: Dict[str, str],
#     frequency: constants.Frequency,
#     period: constants.Period,
# ) -> List[Dict[str, Any]]:
#     """Creates metadata for each column for use in the table extension.

#     Args:
#         columns_types (Dict[str, str]): A dictionary mapping column names to
#             parquet data types.
#         frequency (Frequency): Temporal interval of CSV data, e.g., 'monthly'
#             'hourly'.
#         period (Period): Climate normal time period of CSV data, e.g.,
#             '1991-2020'.

#     Returns:
#         List[Dict[str, Any]]: A list of dictionaries, each specifying a column
#             name, parquet data type, description, and unit.
#     """
#     column_metadata = load_column_metadata(frequency, period)
#     columns = []
#     for column, data_type in columns_types.items():
#         temp = {}
#         temp["name"] = column
#         temp["type"] = data_type
#         temp["description"] = column_metadata[column]["description"]
#         unit = column_metadata.get(column, {}).get("unit")
#         if unit:
#             temp["unit"] = unit
#         columns.append(temp)

#     return columns


def load_column_metadata(
    frequency: constants.Frequency, period: constants.Period
) -> Any:
    """Loads a JSON file containing column metadata.

    Args:
        frequency (Frequency): Temporal interval of CSV data, e.g., 'monthly'
            'hourly'.
        period (Period): Climate normal time period of CSV data, e.g.,
            '1991-2020'.

    Returns:
        Any: A dictionary mapping column names to metadata.
    """
    try:
        with pkg_resources.resource_stream(
            "stactools.noaa_climate_normals.tabular.parquet",
            f"column_metadata/{frequency}_{period}.json",
        ) as stream:
            return json.load(stream)
    except FileNotFoundError as e:
        raise e


def get_collection_tables() -> Dict[int, Dict[str, str]]:
    """Creates a dictionary of dictionaries containing table names and
    descriptions for use in the 'table' extension on the Tabular Collection.

    Returns:
        Dict[int, Dict[str, str]]: Dictionaries containing table names and
            descriptions.
    """
    tables: Dict[int, Dict[str, str]] = {}
    for idx, period in enumerate(constants.Period):
        for frequency in constants.Frequency:
            tables[idx] = {}
            tables[idx]["name"] = id_string(frequency, period)
            tables[idx][
                "description"
            ] = f"{formatted_frequency(frequency)} Climate Normals for Period {period}"

    return tables

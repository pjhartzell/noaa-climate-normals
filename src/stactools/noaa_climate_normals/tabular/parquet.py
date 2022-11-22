import json
import logging
import os
import warnings
from typing import Any, Dict, List, Optional
from warnings import simplefilter

import dask
import dask.dataframe
import dask_geopandas
import geopandas as gpd
import pandas as pd
import pkg_resources
import pyarrow
import pystac
from stactools.core.io import ReadHrefModifier

from ..utils import modify_href
from . import constants
from .utils import formatted_frequency, id_string

logger = logging.getLogger(__name__)


def pandas_datatypes(
    frequency: constants.Frequency, period: constants.Period
) -> Dict[str, Any]:
    """Generates a dictionary of pandas datatypes keyed by column name.

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
            dtypes[key] = pd.CategoricalDtype(value["categories"], ordered=False)
        else:
            dtypes[key] = value["pandas_dtype"]
    return dtypes


def create_parquet(
    csv_hrefs: List[str],
    frequency: constants.Frequency,
    period: constants.Period,
    parquet_dir: str,
    read_href_modifier: Optional[ReadHrefModifier] = None,
    num_partitions: int = 5,
) -> str:
    """Creates a GeoParquet file from a list of CSV files.

    Args:
        csv_hrefs (List[str]): HREFs to CSV files that will be converted to
            GeoParquet.
        frequency (Frequency): Temporal interval of CSV data, e.g., 'monthly' or
            'hourly'.
        period (Period): Climate normal time period of CSV data, e.g.,
            '1991-2020'.
        parquet_dir (str): Directory for created parquet data.
        read_href_modifier (Optional[ReadHrefModifier]): An optional function
            to modify an HREF, e.g., to add a token to a URL.
        num_partitions (int): Number of parquet files to create.

    Returns:
        str: Path to directory containing one or more parquet files.
    """

    @dask.delayed  # type:ignore
    def dataframe_from_csv(
        csv_href: str, pd_dtypes: Dict[str, Any], empty_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Ingests a CSV file to a dataframe having a complete, ordered set of
        columns."""
        df = pd.read_csv(csv_href, dtype=pd_dtypes)
        df = pd.concat([df, empty_df])[empty_df.columns]
        df["geometry"] = gpd.points_from_xy(
            x=df.LONGITUDE,
            y=df.LATITUDE,
            z=df.ELEVATION,
            crs=constants.CRS,
        )
        return df

    csv_hrefs = [modify_href(csv_href, read_href_modifier) for csv_href in csv_hrefs]
    csv_hrefs = sorted(csv_hrefs)
    pd_dtypes = pandas_datatypes(frequency, period)
    empty_df = pd.DataFrame({c: pd.Series(dtype=t) for c, t in pd_dtypes.items()})

    pd_df = [
        dataframe_from_csv(csv_href, pd_dtypes, empty_df) for csv_href in csv_hrefs
    ]
    dask_df = dask.dataframe.from_delayed(pd_df, meta=empty_df)
    dask_gdf = dask_geopandas.from_dask_dataframe(dask_df)

    parquet_path = os.path.join(parquet_dir, f"{id_string(frequency, period)}.parquet")

    if len(csv_hrefs) < num_partitions:
        logger.warning(
            f"Requested number of partitions ({num_partitions}) is greater than the "
            f"number of CSV files ({len(csv_hrefs)}). Number of partitions is set to 1."
        )
        num_partitions = 1

    # TODO: Fix highly fragmented dataframes. Silence for now.
    with warnings.catch_warnings():
        simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
        dask_gdf.repartition(num_partitions).to_parquet(parquet_path, write_index=False)

    # # DOES NOT WORK: schema is not retained in output parquet files
    # parquet_schema = create_parquet_schema(
    #     csv_hrefs[0], empty_df, pd_dtypes, frequency, period
    # )
    # with warnings.catch_warnings():
    #     simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
    #     dask_gdf.repartition(num_partitions).to_parquet(
    #         parquet_path, schema=parquet_schema, write_index=False
    #     )

    return parquet_path


def create_parquet_schema(
    csv_href: str,
    empty_df: pd.DataFrame,
    pd_dtypes: Dict[str, Any],
    frequency: constants.Frequency,
    period: constants.Period,
) -> pyarrow.Schema:
    """Attempt at injecting metadata into output parquet file schema."""
    # ingesting one row enables pyarrow to determine arrow types from ambiguous
    # pandas 'object' types
    df = pd.read_csv(csv_href, nrows=1, dtype=pd_dtypes)
    df = pd.concat([df, empty_df])[empty_df.columns]
    gdf = gpd.GeoDataFrame(data=df)
    gdf["geometry"] = gpd.points_from_xy(
        x=df.LONGITUDE,
        y=df.LATITUDE,
        z=df.ELEVATION,
        crs=constants.CRS,
    )
    df2 = pd.DataFrame(gdf.copy())
    df2["geometry"] = gdf["geometry"].to_wkb()
    schema = pyarrow.Schema.from_pandas(df2, preserve_index=False)

    # table metadata
    table_metadata = schema.metadata
    table_metadata[
        "description"
    ] = f"{formatted_frequency(frequency)} Climate Normals for Period {period}"
    schema = schema.with_metadata(table_metadata)

    # column metadata
    column_metadata = load_column_metadata(frequency, period)
    for name in schema.names:
        metadata = {}
        metadata["description"] = column_metadata[name]["description"]
        unit = column_metadata.get(name, {}).get("unit", False)
        if unit:
            metadata["unit"] = unit
        index = schema.get_field_index(name)
        field = schema.field(index).with_metadata(metadata)
        schema = schema.set(index, field)

    return schema


def update_table_columns(
    item: pystac.Item,
    frequency: constants.Frequency,
    period: constants.Period,
) -> pystac.Item:
    """Update `table:columns` list with column descriptions and units.

    Args:
        item (pystac.Item): Item containing a `table:columns` property.
        frequency (Frequency): Temporal interval of CSV data, e.g., 'monthly' or
            'hourly'.
        period (Period): Climate normal time period of CSV data, e.g.,
            '1991-2020'.

    Returns:
        pystac.Item: Item with updated `table:columns` property.
    """

    column_metadata = load_column_metadata(frequency, period)

    columns = item.properties["table:columns"]
    new_columns = []
    for column in columns:
        name = column["name"]
        column["description"] = column_metadata[name]["description"]
        unit = column_metadata.get(name, {}).get("unit", False)
        if unit:
            column["unit"] = unit
        new_columns.append(column)

    item.properties["table:columns"] = new_columns

    return item


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

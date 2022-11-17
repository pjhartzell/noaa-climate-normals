import json
import logging
from typing import Any, Dict, List, Optional

import dask
import dask.dataframe
import dask_geopandas
import fsspec
import geopandas
import pandas
import pkg_resources
import pyarrow.parquet as pq
from pystac.utils import make_absolute_href
from shapely.geometry import mapping
from stactools.core.io import ReadHrefModifier

from ..utils import modify_href
from . import constants
from .utils import formatted_frequency, id_string

logger = logging.getLogger(__name__)


def pandas_datatypes(
    frequency: constants.Frequency, period: constants.Period
) -> Dict[str, Any]:
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
    parquet_path: str,
) -> str:
    """Creates a GeoParquet file from a list of CSV files.

    Args:
        csv_hrefs (List[Dict[str, Any]]): List of HREFs to CSV files that will be
            converted to a single parquet file.
        frequency (Frequency): Temporal interval of CSV data, e.g., 'monthly' or
            'hourly'.
        period (Period): Climate normal time period of CSV data, e.g.,
            '1991-2020'.
        parquet_path (str): Path for created parquet file.

    Returns:
        str: Path to created geoparquet file.
    """

    @dask.delayed
    def dataframe_from_csv(
        csv_href: str, pd_dtypes: Dict[str, Any], empty_df: pandas.DataFrame
    ) -> pandas.DataFrame:
        """Returns a dataframe with an ordered, complete set of columns."""
        df = pandas.read_csv(csv_href, dtype=pd_dtypes)
        return pandas.concat([df, empty_df])[empty_df.columns]

    pd_dtypes = pandas_datatypes(frequency, period)
    empty_df = pandas.DataFrame({c: pandas.Series(dtype=t) for c, t in pd_dtypes.items()})

    pandas_dataframes = [
        dataframe_from_csv(csv_href, pd_dtypes, empty_df) for csv_href in csv_hrefs
    ]

    dask_dataframe = dask.dataframe.from_delayed(pandas_dataframes, meta=empty_df)

    dask_geodataframe = dask_geopandas.from_dask_dataframe(dask_dataframe)

    # dask_geodataframe.assign(
    #     geometry=lambda df: geopandas.points_from_xy(
    #         x=df.LONGITUDE,
    #         y=df.LATITUDE,
    #         z=df.ELEVATION,
    #         crs=constants.CRS,
    #     )
    # )

    dask_geodataframe.repartition(10).to_parquet(parquet_path)

    return parquet_path


def parquet_metadata(
    parquet_href: str,
    frequency: constants.Frequency,
    period: constants.Period,
    geometry: Optional[Dict[str, Any]] = None,
    read_href_modifier: Optional[ReadHrefModifier] = None,
) -> Dict[str, Any]:
    """Generate metadata from a GeoParquet file for use in STAC Asset and Item
    creation.

    Args:
        parquet_href (str): Path to GeoParquet file.
        frequency (Frequency): Temporal interval of CSV data, e.g., 'monthly' or
            'hourly'.
        period (Period): Climate normal time period of CSV data, e.g.,
            '1991-2020'.
        geometry (Optional[Dict[str, Any]]: Convex hull of GeoParquet points in
            GeoJSON format. Only the GeoParquet file metadata needs to be
            touched when this option is provided.
        read_href_modifier (Optional[ReadHrefModifier]): An optional function
            to modify an HREF, e.g., to add a token to a URL.

    Returns:
        Dict[str, Any]: Dictionary of metadata for asset and Item creation.
    """
    read_parquet_href = modify_href(parquet_href, read_href_modifier)
    with fsspec.open(read_parquet_href) as fobj:
        metadata = pq.read_metadata(fobj)
        columns_types = {
            col.name.lower(): col.physical_type.lower() for col in metadata.schema
        }
        num_rows = metadata.num_rows

        schema = pq.read_schema(fobj)
        bbox = json.loads(schema.metadata[b"geo"].decode())["columns"]["geometry"][
            "bbox"
        ]

        if not geometry:
            geodataframe = geopandas.read_parquet(fobj, columns=["geometry"])
            geometry = mapping(geodataframe.unary_union.convex_hull)

    return {
        "href": make_absolute_href(parquet_href),
        "geometry": geometry,
        "bbox": bbox,
        "type": constants.PARQUET_MEDIA_TYPE,
        "title": constants.PARQUET_ASSET_TITLE,
        "table:primary_geometry": constants.PARQUET_GEOMETRY_COL,
        "table:columns": geodataframe_columns(columns_types, frequency, period),
        "table:row_count": num_rows,
        "roles": ["data"],
    }


def geodataframe_columns(
    columns_types: Dict[str, str],
    frequency: constants.Frequency,
    period: constants.Period,
) -> List[Dict[str, Any]]:
    """Creates metadata for each column in a GeoPandas DataFrame for use in the
    'table' extension.

    Args:
        columns_types (Dict[str, str]): A dictionary mapping column names to
            parquet data types.
        frequency (Frequency): Temporal interval of CSV data, e.g., 'monthly'
            'hourly'.
        period (Period): Climate normal time period of CSV data, e.g.,
            '1991-2020'.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, each specifying a column
            name, data type, description, and unit.
    """
    column_metadata = load_column_metadata(frequency, period)
    columns = []
    for column, data_type in columns_types.items():
        temp = {}

        temp["name"] = column
        temp["type"] = data_type

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
    """Loads a dictionary mapping column names to descriptions.

    Args:
        frequency (Frequency): Temporal interval of CSV data, e.g., 'monthly'
            'hourly'.
        period (Period): Climate normal time period of CSV data, e.g.,
            '1991-2020'.

    Returns:
        Any: A dictionary mapping column names to descriptions and units.
    """
    try:
        with pkg_resources.resource_stream(
            "stactools.noaa_climate_normals.tabular.parquet",
            f"column_metadata/{frequency}_{period}.json",
        ) as stream:
            return json.load(stream)
    except FileNotFoundError as e:
        raise e


def get_tables() -> Dict[int, Dict[str, str]]:
    """Creates a dictionary of dictionaries containing table names and
    descriptions for use in the 'table' extension on a Collection.

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

import json
import logging
from typing import Any, Dict, List, Optional

import fsspec
import geopandas as gpd
import pandas as pd
import pkg_resources
import pyarrow.parquet as pq
from pystac.utils import make_absolute_href
from shapely.geometry import mapping
from stactools.core.io import ReadHrefModifier

from ..utils import modify_href
from . import constants
from .utils import formatted_frequency, id_string

logger = logging.getLogger(__name__)


def create_parquet(
    csv_hrefs: List[str],
    frequency: constants.Frequency,
    period: constants.Period,
    parquet_path: str,
    read_href_modifier: Optional[ReadHrefModifier] = None,
) -> Dict[str, Any]:
    """Creates a GeoParquet file from a list of CSV files.

    Args:
        csv_hrefs (List[Dict[str, Any]]): List of HREFs to CSV files that will be
            converted to a single parquet file.
        frequency (Frequency): Temporal interval of CSV data, e.g., 'monthly' or
            'hourly'.
        period (Period): Climate normal time period of CSV data, e.g.,
            '1991-2020'.
        parquet_path (str): Path for created parquet file.
        read_href_modifier (Optional[ReadHrefModifier]): An optional function
            to modify an HREF, e.g., to add a token to a URL.

    Returns:
        str: Dictionary of metadata for asset and Item creation.
    """
    read_csv_hrefs = [
        modify_href(csv_href, read_href_modifier) for csv_href in csv_hrefs
    ]

    dataframes = []
    for csv_href in read_csv_hrefs:
        dataframes.append(pd.read_csv(csv_href))

    dataframe = pd.concat(dataframes, ignore_index=True).copy()
    del dataframes

    # Parquet does not like columns containing data of multiple types. Some CSV
    # columns contain string and integer types, where valid data is a string
    # and integer flags are used to indicate why data is missing. Mixed column
    # data types also occur when concatenating dataframes with different columns.
    # In this case numpy.nan values are used as fill. These nan values are float
    # types, which creates columns of mixed types when valid data is not float
    # type. These issues only appear to occur for columns where valid data are
    # string type, so we convert the column values to strings when mixed types
    # are encountered. An error is raised if we encounter an unexpected mix of
    # types in a column (a mix that does not contain a string type).
    for column in dataframe.columns:
        column_types = set(dataframe[column].apply(type).values)
        if len(column_types) > 1 and type(str()) in column_types:
            logger.info(
                f"Column '{column}' has mixed data types: {column_types}. "
                f"Converting the column to 'str' data type."
            )
            dataframe[column] = dataframe[column].astype(str)
        elif len(column_types) != 1:
            raise ValueError(
                f"Unexpected data type mix in Column '{column}': {column_types}."
            )

    dataframe.columns = dataframe.columns.str.lower()

    make_categorical(dataframe)

    dataframe = dataframe.copy()  # de-fragment

    geodataframe = gpd.GeoDataFrame(
        data=dataframe,
        geometry=gpd.points_from_xy(
            x=dataframe.longitude,
            y=dataframe.latitude,
            z=dataframe.elevation,
            crs=constants.CRS,
        ),
    )
    del dataframe

    geodataframe.to_parquet(parquet_path)

    geometry = mapping(geodataframe.unary_union.convex_hull)

    return parquet_metadata(parquet_path, frequency, period, geometry=geometry)


def make_categorical(df: pd.DataFrame) -> None:
    """Convert pandas columns that contain categorical data to a categorical
    data type.

    Args:
        df (pd.DataFrame): Pandas DataFrame containing the unconverted data
            types.
    """
    categorical_substrings = [
        "_attributes",
        "comp_flag_",
        "meas_flag",
        "wind-1stdir",
        "wind-2nddir",
    ]
    for column in df.columns:
        if any([substring in column for substring in categorical_substrings]):
            df[column] = df[column].astype("category")


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
            geodataframe = gpd.read_parquet(fobj, columns=["geometry"])
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

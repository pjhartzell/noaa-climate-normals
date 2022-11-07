from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict

import pystac
from pystac.extensions.item_assets import AssetDefinition
from pystac.extensions.scientific import Publication

from ..constants import KEYWORDS


class Frequency(str, Enum):
    HOURLY = "hourly"
    DAILY = "daily"
    MONTHLY = "monthly"
    ANNUALSEASONAL = "annualseasonal"


class Period(str, Enum):
    PERIOD_1981_2010 = "1981-2010"
    PERIOD_1991_2020 = "1991-2020"
    PERIOD_2006_2020 = "2006-2020"


PARQUET_MEDIA_TYPE = "application/x-parquet"
PARQUET_GEOMETRY_COL = "geometry"
PARQUET_ASSET_TITLE = "GeoParquet for all stations"

CRS = "EPSG:4269"

HOMEPAGE = {
    "1981-2010": {
        "hourly": pystac.Link(
            rel="about",
            target="https://www.ncei.noaa.gov/access/metadata/landing-page/bin/iso?id=gov.noaa.ncdc:C00824",  # noqa
            media_type="text/html",
            title="U.S. Hourly Climate Normals (1981-2010) Landing Page",
        ),
        "daily": pystac.Link(
            rel="about",
            target="https://www.ncei.noaa.gov/access/metadata/landing-page/bin/iso?id=gov.noaa.ncdc:C00823",  # noqa
            media_type="text/html",
            title="U.S. Daily Climate Normals (1981-2010) Landing Page",
        ),
        "monthly": pystac.Link(
            rel="about",
            target="https://www.ncei.noaa.gov/access/metadata/landing-page/bin/iso?id=gov.noaa.ncdc:C00822",  # noqa
            media_type="text/html",
            title="U.S. Monthly Climate Normals (1981-2010) Landing Page",
        ),
        "annualseasonal": pystac.Link(
            rel="about",
            target="https://www.ncei.noaa.gov/access/metadata/landing-page/bin/iso?id=gov.noaa.ncdc:C00821",  # noqa
            media_type="text/html",
            title="U.S. Annual/Seasonal Climate Normals (1981-2010) Landing Page",
        ),
    },
    "1991-2020": {
        "hourly": pystac.Link(
            rel="about",
            target="https://www.ncei.noaa.gov/metadata/geoportal/rest/metadata/item/gov.noaa.ncdc:C01622/html",  # noqa
            media_type="text/html",
            title="U.S. Hourly Climate Normals (1991-2020) Landing Page",
        ),
        "daily": pystac.Link(
            rel="about",
            target="https://www.ncei.noaa.gov/metadata/geoportal/rest/metadata/item/gov.noaa.ncdc:C01621/html",  # noqa
            media_type="text/html",
            title="U.S. Daily Climate Normals (1991-2020) Landing Page",
        ),
        "monthly": pystac.Link(
            rel="about",
            target="https://www.ncei.noaa.gov/metadata/geoportal/rest/metadata/item/gov.noaa.ncdc:C01620/html",  # noqa
            media_type="text/html",
            title="U.S. Monthly Climate Normals (1991-2020) Landing Page",
        ),
        "annualseasonal": pystac.Link(
            rel="about",
            target="https://www.ncei.noaa.gov/metadata/geoportal/rest/metadata/item/gov.noaa.ncdc:C01619/html",  # noqa
            media_type="text/html",
            title="U.S. Annual/Seasonal Climate Normals (1991-2020) Landing Page",
        ),
    },
    "2006-2020": {
        "hourly": pystac.Link(
            rel="about",
            target="https://www.ncei.noaa.gov/metadata/geoportal/rest/metadata/item/gov.noaa.ncdc:C01626/html",  # noqa
            media_type="text/html",
            title="U.S. Hourly Climate Normals (2006-2020) Landing Page",
        ),
        "daily": pystac.Link(
            rel="about",
            target="https://www.ncei.noaa.gov/metadata/geoportal/rest/metadata/item/gov.noaa.ncdc:C01625/html",  # noqa
            media_type="text/html",
            title="U.S. Daily Climate Normals (2006-2020) Landing Page",
        ),
        "monthly": pystac.Link(
            rel="about",
            target="https://www.ncei.noaa.gov/metadata/geoportal/rest/metadata/item/gov.noaa.ncdc:C01624/html",  # noqa
            media_type="text/html",
            title="U.S. Monthly Climate Normals (2006-2020) Landing Page",
        ),
        "annualseasonal": pystac.Link(
            rel="about",
            target="https://www.ncei.noaa.gov/metadata/geoportal/rest/metadata/item/gov.noaa.ncdc:C01623/html",  # noqa
            media_type="text/html",
            title="U.S. Annual/Seasonal Climate Normals (2006-2020) Landing Page",
        ),
    },
}

DOCUMENTATION = {
    "1981-2010": {
        "hourly": pystac.Link(
            rel="describedby",
            target="https://www.ncei.noaa.gov/data/normals-hourly/1981-2010/doc/NORMAL_HLY_documentation.pdf",  # noqa
            media_type="application/pdf",
            title="U.S. Hourly Climate Normals (1981-2010) Documentation",
        ),
        "daily": pystac.Link(
            rel="describedby",
            target="https://www.ncei.noaa.gov/data/normals-daily/1981-2010/doc/NORMAL_DLY_documentation.pdf",  # noqa
            media_type="application/pdf",
            title="U.S. Daily Climate Normals (1981-2010) Documentation",
        ),
        "monthly": pystac.Link(
            rel="describedby",
            target="https://www.ncei.noaa.gov/data/normals-monthly/1981-2010/doc/NORMAL_MLY_documentation.pdf",  # noqa
            media_type="application/pdf",
            title="U.S. Monthly Climate Normals (1981-2010) Documentation",
        ),
        "annualseasonal": pystac.Link(
            rel="describedby",
            target="https://www.ncei.noaa.gov/data/normals-annualseasonal/1981-2010/doc/NORMAL_ANN_documentation.pdf",  # noqa
            media_type="application/pdf",
            title="U.S. Annual/Seasonal Climate Normals (1981-2010) Documentation",
        ),
    },
    "1991-2020": {
        "hourly": pystac.Link(
            rel="describedby",
            target="https://www.ncei.noaa.gov/data/normals-hourly/1991-2020/doc/Normals_HLY_Documentation_1991-2020.pdf",  # noqa
            media_type="application/pdf",
            title="U.S. Hourly Climate Normals (1991-2020) Documentation",
        ),
        "daily": pystac.Link(
            rel="describedby",
            target="https://www.ncei.noaa.gov/data/normals-daily/1991-2020/doc/Normals_DLY_Documentation_1991-2020.pdf",  # noqa
            media_type="application/pdf",
            title="U.S. Daily Climate Normals (1991-2020) Documentation",
        ),
        "monthly": pystac.Link(
            rel="describedby",
            target="https://www.ncei.noaa.gov/data/normals-monthly/1991-2020/doc/Normals_MLY_Documentation_1991-2020.pdf",  # noqa
            media_type="application/pdf",
            title="U.S. Monthly Climate Normals (1991-2020) Documentation",
        ),
        "annualseasonal": pystac.Link(
            rel="describedby",
            target="https://www.ncei.noaa.gov/data/normals-annualseasonal/1991-2020/doc/Normals_ANN_Documentation_1991-2020.pdf",  # noqa
            media_type="application/pdf",
            title="U.S. Annual/Seasonal Climate Normals (1991-2020) Documentation",
        ),
    },
    "2006-2020": {
        "hourly": pystac.Link(
            rel="describedby",
            target="https://www.ncei.noaa.gov/data/normals-hourly/2006-2020/doc/Normals_HLY_Documentation_2006-2020.pdf",  # noqa
            media_type="application/pdf",
            title="U.S. Hourly Climate Normals (2006-2020) Documentation",
        ),
        "daily": pystac.Link(
            rel="describedby",
            target="https://www.ncei.noaa.gov/data/normals-daily/2006-2020/doc/Normals_DLY_Documentation_2006-2020.pdf",  # noqa
            media_type="application/pdf",
            title="U.S. Daily Climate Normals (2006-2020) Documentation",
        ),
        "monthly": pystac.Link(
            rel="describedby",
            target="https://www.ncei.noaa.gov/data/normals-monthly/2006-2020/doc/Normals_MLY_Documentation_2006-2020.pdf",  # noqa
            media_type="application/pdf",
            title="U.S. Monthly Climate Normals (2006-2020) Documentation",
        ),
        "annualseasonal": pystac.Link(
            rel="describedby",
            target="https://www.ncei.noaa.gov/data/normals-annualseasonal/2006-2020/doc/Normals_ANN_Documentation_2006-2020.pdf",  # noqa
            media_type="application/pdf",
            title="U.S. Annual/Seasonal Climate Normals (2006-2020) Documentation",
        ),
    },
}

DATA_1981_2010 = {
    "doi": "10.7289/V5PN93JP",
    "citation": "Anthony Arguez, Imke Durre, Scott Applequist, Mike Squires, Russell Vose, Xungang Yin, and Rocky Bilotta (2010). NOAA's U.S. Climate Normals (1981-2010). FREQUENCY subset. NOAA National Centers for Environmental Information. DOI:10.7289/V5PN93JP.",  # noqa
}
PUBLICATION_HOURLY = Publication(
    doi="10.1175/BAMS-D-11-00173.1",
    citation="Applequist, S., A. Arguez, I. Durre, M. Squires, R. Vose, and X. Yin, 2012: 1981-2010 U.S. Hourly Normals. Bulletin of the American Meteorological Society, 93, 1637-1640. DOI: 10.1175/BAMS-D-11-00173.1.",  # noqa
)
PUBLICATION_DAILY_MONTHLY_ANNUALSEASONAL = Publication(
    doi="10.1175/BAMS-D-11-00197.1",
    citation="Arguez, A., I. Durre, S. Applequist, R. Vose, M. Squires, X. Yin, R. Heim, and T. Owen, 2012: NOAA's 1981-2010 climate normals: An overview. Bull. Amer. Meteor. Soc., 93, 1687-1697. DOI: 10.1175/BAMS-D-11-00197.1.",  # noqa
)

ITEM_ASSETS = {
    "geoparquet": AssetDefinition(
        {
            "type": PARQUET_MEDIA_TYPE,
            "title": PARQUET_ASSET_TITLE,
            "table:primary_geometry": PARQUET_GEOMETRY_COL,
            "roles": ["data"],
        }
    )
}

COLLECTION: Dict[str, Any] = {
    "id": "noaa-climate-normals-tabular",
    "title": "Tabular U.S. Climate Normals",
    "description": (
        "The NOAA U.S. Climate Normals provide information about typical climate "
        "conditions for thousands of weather station locations across the United "
        "States. Normals act both as a ruler to compare current weather and as a "
        "predictor of conditions in the near future. The official normals are "
        "calculated for a uniform 30 year period, and consist of annual/seasonal, "
        "monthly, daily, and hourly averages and statistics of temperature, "
        "precipitation, and other climatological variables for each weather "
        "station. This collection contains tabular format data for weather "
        "station climate normals for two conventional 30 year periods (1981-2010 "
        "and 1991-2020) and a recent 15 year period (2006-2020)."
    ),
    "license": "proprietary",
    "keywords": KEYWORDS,
    "extent": pystac.Extent(
        pystac.SpatialExtent([[-177.38333, -14.3306, 174.1, 71.3214]]),
        pystac.TemporalExtent(
            [
                [
                    datetime(1981, 1, 1, tzinfo=timezone.utc),
                    datetime(2020, 12, 31, tzinfo=timezone.utc),
                ]
            ]
        ),
    ),
}

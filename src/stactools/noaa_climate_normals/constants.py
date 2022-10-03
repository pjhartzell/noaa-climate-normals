from enum import Enum

import pystac


class Frequency(str, Enum):
    HOURLY = "hourly"
    DAILY = "daily"
    MONTHLY = "monthly"
    ANNUALSEASONAL = "annual/seasonal"


class Period(str, Enum):
    ONE = "1981-2010"
    TWO = "1991-2010"
    THREE = "2006-2020"


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
    }
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
    }
}

CITE_AS = {
    "1981-2010": {
        "doi": "10.7289/V5PN93JP",
        "citation": (
            "Anthony Arguez, Imke Durre, Scott Applequist, Mike Squires, "
            "Russell Vose, Xungang Yin, and Rocky Bilotta (2010). NOAA's "
            "U.S. Climate Normals (1981-2010). FREQUENCY. NOAA National "
            "Centers for Environmental Information. DOI:10.7289/V5PN93JP."
        ),
    }
}

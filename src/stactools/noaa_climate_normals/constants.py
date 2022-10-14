import pystac

KEYWORDS = [
    "NOAA",
    "Climate Normals",
    "Air Temperature",
    "Precipitation",
    "Surface Observations",
    "Climatology",
    "CONUS",
]
LANDING_PAGE_LINK = pystac.Link(
    rel="about",
    target="https://www.ncei.noaa.gov/products/land-based-station/us-climate-normals",
    media_type="text/html",
    title="NOAA U.S. Climate Normals Landing Page",
)
LICENSE_LINK = pystac.Link(
    rel="license",
    target=("https://www.noaa.gov/information-technology/open-data-dissemination"),
    title="NOAA Open Data Dissemination",
    media_type="text/html",
)
PROVIDERS = [
    pystac.Provider(
        name="NOAA National Centers for Environmental Information",
        roles=[
            pystac.ProviderRole.PRODUCER,
            pystac.ProviderRole.PROCESSOR,
            pystac.ProviderRole.LICENSOR,
            pystac.ProviderRole.HOST,
        ],
        url="https://www.ncei.noaa.gov/",
    )
]

from typing import Any, Dict

from stactools.testing.test_data import TestData

MONTHLY_FILES = ["prcp", "tavg", "tmin", "tmax"]

url_base = "https://www.nodc.noaa.gov/archive/arc0196/0245564/1.1/data/0-data/"
external_data: Dict[str, Dict[str, Any]] = dict()
for var in MONTHLY_FILES:
    filename = f"{var}-1991_2020-monthly-normals-v1.0.nc"
    external_data[filename] = {"url": f"{url_base}{filename}"}

test_data = TestData(__file__, external_data=external_data)

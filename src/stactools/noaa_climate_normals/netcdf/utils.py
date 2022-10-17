import os


def netcdf_item_id(nc_href: str) -> str:
    return os.path.splitext(os.path.basename(nc_href))[0]

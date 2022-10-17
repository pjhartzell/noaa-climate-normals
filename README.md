# stactools-noaa-climate-normals

[![PyPI](https://img.shields.io/pypi/v/stactools-noaa-climate-normals)](https://pypi.org/project/stactools-noaa-climate-normals/)

- Name: noaa-climate-normals
- Package: `stactools.noaa_climate_normals`
- [stactools-noaa-climate-normals on PyPI](https://pypi.org/project/stactools-noaa-climate-normals/)
- Owner: @pjhartzell
- [Dataset homepage](https://www.ncei.noaa.gov/products/land-based-station/us-climate-normals)
- STAC extensions used:
  - [item-assets](https://github.com/stac-extensions/item-assets)
  - [proj](https://github.com/stac-extensions/projection/)
  - [raster](https://github.com/stac-extensions/raster)
  - [scientific](https://github.com/stac-extensions/scientific)
  - [table](https://github.com/stac-extensions/table)

- Extra fields:
  - `noaa-climate-normals:period`: A year range indicating the time period from which the climate normals were computed.
  - `noaa-climate-normals:frequency`: The temporal interval for the climate normals, e.g., daily or hourly.
- [Browse the example in human-readable form](https://radiantearth.github.io/stac-browser/#/external/raw.githubusercontent.com/pjhartzell/noaa-climate-normals/main/examples/catalog.json)

## Background

The NOAA U.S. Climate Normals provide information about typical climate conditions for thousands of weather station locations across the United States. Normals act both as a ruler to compare current weather and as a predictor of conditions in the near future. Climate normals are calculated for uniform time periods (conventionally 30 years long), and consist of annual/seasonal, monthly, daily, and hourly averages and statistics of temperature, precipitation, and other climatological variables for each weather station.

Data is available in two forms: tabular and gridded. The tabular data consists of weather variables for each weather station location. The gridded data is an interpolated form of the tabular data and is derived from the [NClimGrid](https://www.ncei.noaa.gov/access/metadata/landing-page/bin/iso?id=gov.noaa.ncdc:C00332) dataset. Gridded data is limited to temperature and precipitation information.

Three Collections, and corresponding Items, can be generated with this package:

1. `noaa-climate-normals-tabular`

    - Items for each Climate Normal time period (e.g., 1991-2020) and temporal interval (e.g., monthly) combination.
    - Each Item contains a single GeoParquet Asset created from weather station tabular data contained in CSV files.

2. `noaa-climate-normals-gridded`

    - Items for each Climate Normal time period (e.g., 1991-2020) and temporal interval timestep (e.g., a month or a day) combination.
    - Each Item contains COG Assets for all available weather variables.

3. `noaa-climate-normals-netcdf`

    - Items for the NetCDF files that serve as the source data for the COGs in the gridded Collection.

## STAC Examples

- Collections

    - [tabular](examples/noaa-climate-normals-tabular/collection.json)
    - [gridded](examples/noaa-climate-normals-gridded/collection.json)
    - netcdf

- Items

    - [tabular](examples/noaa-climate-normals-tabular/1981_2010-daily/1981_2010-daily.json)
    - [gridded](examples/noaa-climate-normals-gridded/1991_2020-monthly-1/1991_2020-monthly-1.json)
    - netcdf

## Installation

```shell
pip install stactools-noaa-climate-normals
```

## Command-line Usage

To create a Collection:

```shell
stac noaa-climate-normals <tabular|gridded> create-collection <collection filepath>
```

To create an Item, e.g., for the `monthly` tabular data from the `1991-2020` time period:

```shell
stac noaa-climate-normals tabular create-item <filepath to text file of csv hrefs> <daily|monthly|seasonal|annual> <1901-2020|1991-2020|2006-2020> <directory for created item and geoparquet files>
```

Each Collection has unique subcommands and options. Use `stac noaa-climate-normals --help` to explore subcommands and options.

## Contributing

We use [pre-commit](https://pre-commit.com/) to check any changes.
To set up your development environment:

```shell
pip install -e .
pip install -r requirements-dev.txt
pre-commit install
```

To check all files:

```shell
pre-commit run --all-files
```

To run the tests:

```shell
pytest -vv
```

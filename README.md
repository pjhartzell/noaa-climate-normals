# stactools-noaa-climate-normals

[![PyPI](https://img.shields.io/pypi/v/stactools-noaa-climate-normals)](https://pypi.org/project/stactools-noaa-climate-normals/)

- Name: noaa-climate-normals
- Package: `stactools.noaa_climate_normals`
- [stactools-noaa-climate-normals on PyPI](https://pypi.org/project/stactools-noaa-climate-normals/)
- Owner: @githubusername
- [Dataset homepage](http://example.com)
- STAC extensions used:
  - [proj](https://github.com/stac-extensions/projection/)
- Extra fields:
  - `noaa-climate-normals:custom`: A custom attribute
- [Browse the example in human-readable form](https://radiantearth.github.io/stac-browser/#/external/raw.githubusercontent.com/stactools-packages/noaa-climate-normals/main/examples/collection.json)

A short description of the package and its usage.

## STAC Examples

- [Collection](examples/collection.json)
- [Item](examples/item/item.json)

## Installation

```shell
pip install stactools-noaa-climate-normals
```

## Command-line Usage

Description of the command line functions

```shell
stac noaa-climate-normals create-item source destination
```

Use `stac noaa-climate-normals --help` to see all subcommands and options.

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

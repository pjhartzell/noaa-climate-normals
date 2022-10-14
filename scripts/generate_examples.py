import os

from stactools.noaa_climate_normals.tabular import constants, stac

file_lists = [
    ["tests/data-files/tabular/annualseasonal/1981-2010/USW00094765.csv"],
    ["tests/data-files/tabular/annualseasonal/1991-2020/USW00094765.csv"],
    ["tests/data-files/tabular/annualseasonal/2006-2020/USW00094745.csv"],
    ["tests/data-files/tabular/daily/1981-2010/USW00094765.csv"],
    ["tests/data-files/tabular/daily/1991-2020/USW00094765.csv"],
    ["tests/data-files/tabular/daily/2006-2020/USW00094745.csv"],
    ["tests/data-files/tabular/hourly/1981-2010/USW00094746.csv"],
    ["tests/data-files/tabular/hourly/1991-2020/USW00094745.csv"],
    ["tests/data-files/tabular/hourly/2006-2020/USW00094745.csv"],
    [
        "tests/data-files/tabular/monthly/1981-2010/USW00013740.csv",
        "tests/data-files/tabular/monthly/1981-2010/USW00094765.csv",
    ],
    ["tests/data-files/tabular/monthly/1991-2020/USW00094765.csv"],
    ["tests/data-files/tabular/monthly/2006-2020/USW00094765.csv"],
]

for file_list in file_lists:
    frequency = file_list[0].split("/")[3]
    period = file_list[0].split("/")[4]

    _frequency = constants.Frequency(frequency)
    _period = constants.Period(period)

    output_dir = "examples/tabular/item"
    item = stac.create_item(file_list, _frequency, _period, output_dir)
    item.set_self_href(os.path.join(output_dir, item.id + ".json"))
    item.make_asset_hrefs_relative()
    item.validate()
    item.save_object(include_self_link=False)

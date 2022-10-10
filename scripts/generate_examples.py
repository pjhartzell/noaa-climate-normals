import os

from stactools.noaa_climate_normals.tabular import constants, stac

file_lists = [
    ["tests/data-files/annualseasonal/1981-2010/USW00094765.csv"],
    ["tests/data-files/annualseasonal/1991-2020/USW00094765.csv"],
    ["tests/data-files/annualseasonal/2006-2020/USW00094745.csv"],
    ["tests/data-files/daily/1981-2010/USW00094765.csv"],
    ["tests/data-files/daily/1991-2020/USW00094765.csv"],
    ["tests/data-files/daily/2006-2020/USW00094745.csv"],
    ["tests/data-files/hourly/1981-2010/USW00094746.csv"],
    ["tests/data-files/hourly/1991-2020/USW00094745.csv"],
    ["tests/data-files/hourly/2006-2020/USW00094745.csv"],
    [
        "tests/data-files/monthly/1981-2010/USW00013740.csv",
        "tests/data-files/monthly/1981-2010/USW00094765.csv",
    ],
    ["tests/data-files/monthly/1991-2020/USW00094765.csv"],
    ["tests/data-files/monthly/2006-2020/USW00094765.csv"],
]

for file_list in file_lists:
    frequency = file_list[0].split("/")[2]
    period = file_list[0].split("/")[3]

    _frequency = constants.Frequency(frequency)
    _period = constants.Period(period)

    output_dir = "examples/tabular"
    item = stac.create_item(file_list, _frequency, _period, output_dir)
    item.set_self_href(os.path.join(output_dir, item.id + ".json"))
    item.make_asset_hrefs_relative()
    item.validate()
    item.save_object(include_self_link=False)

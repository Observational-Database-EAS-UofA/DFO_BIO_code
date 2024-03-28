import pandas as pd
import xarray as xr
from datetime import datetime
import numpy as np
import os


def initialize_variables():
    string_attrs = ['platform', 'chief_scientist', 'cruise_name', 'orig_cruise_id', 'orig_profile_id', 'lat',
                    'lon', 'datestr']
    additional_attrs = ['shallowest_depth', 'deepest_depth', 'timestamp', 'parent_index', ]
    obs_attrs = ['depth', 'press', 'temp', 'psal', ]
    data_lists = {attr: [] for attr in string_attrs + obs_attrs + additional_attrs}
    return string_attrs, obs_attrs, data_lists, 0


def get_date(datestr):
    datestr = datestr.split("T")
    date = datestr[0]
    time = datestr[1]
    hour = time.split(":")[0]
    minutes = time.split(":")[1]
    date = date.split("-")
    year = date[0]
    month = date[1]
    day = date[2]
    datestr = datetime(int(year), int(month), int(day), int(hour), int(minutes))
    timestamp = datestr.timestamp()
    datestr = datetime.strftime(datestr, "%Y/%m/%d %H:%M:%S")

    return datestr, timestamp


def process_chunks(reader, data_lists):
    i = 0
    for chunk in reader:
        grouped_df = chunk.groupby(
            ['platform_name', 'chief_scientist', 'cruise_name', 'cruise_number', 'id', 'event_number', 'latitude',
             'longitude', 'time'])
        for group, data in grouped_df:
            platform, chief_scientist, cruise_name, orig_cruise_id, orig_profile_id, _, lat, lon, time = group
            data_lists['platform'].append(platform)
            data_lists['chief_scientist'].append(chief_scientist)
            data_lists['cruise_name'].append(cruise_name)
            data_lists['orig_cruise_id'].append(orig_cruise_id)
            data_lists['orig_profile_id'].append(orig_profile_id)
            data_lists['lat'].append(lat)
            data_lists['lon'].append(lon)
            datestr, timestamp = get_date(time)
            data_lists['datestr'].append(datestr)
            data_lists['timestamp'].append(timestamp)

            data_lists['depth'].extend(data['depth'])
            data_lists['temp'].extend(data['TEMPPR01'])
            data_lists['press'].extend(data['PRESPR01'])
            data_lists['psal'].extend(data['PSLTZZ01'])

            # if len(data['depth'].values) > 1:
            #     data_lists['shallowest_depth'].append(min(data['DEPTH_PRESS'][data['DEPTH_PRESS'] != 0]))
            # else:
            data_lists['shallowest_depth'].append(min(data['depth']))
            data_lists['deepest_depth'].append(max(data['depth']))
            data_lists['parent_index'].extend([i] * len(data['depth']))
            i += 1


def create_dataset(data_lists, string_attrs, data_path, save_path):
    os.chdir(data_path[:data_path.rfind("/")])
    os.chdir("../")
    dataset = os.getcwd()
    dataset = dataset[dataset.rfind("/") + 1:]

    if not os.path.isdir(save_path):
        os.mkdir(save_path)
    os.chdir(save_path)
    ds = xr.Dataset(
        coords=dict(
            timestamp=(['profile'], data_lists['timestamp']),
            lat=(['profile', ], data_lists['lat']),
            lon=(['profile', ], data_lists['lon']),
        ),
        data_vars=dict(
            **{attr: xr.DataArray(data_lists[attr], dims=['profile']) for attr in string_attrs if
               attr not in ['lat', 'lon', 'timestamp', 'parent_index']},
            # measurements
            parent_index=xr.DataArray(data_lists['parent_index'], dims=['obs']),
            depth=xr.DataArray(data_lists['depth'], dims=['obs']),
            press=xr.DataArray(data_lists['press'], dims=['obs']),
            temp=xr.DataArray(data_lists['temp'], dims=['obs']),
            psal=xr.DataArray(data_lists['psal'], dims=['obs']),
        ),
        attrs=dict(
            dataset_name=dataset,
            creation_date=str(datetime.now().strftime("%Y-%m-%d %H:%M")),
        ),
    )

    file_name = data_path[data_path.rfind("/") + 1:-4]
    ds.to_netcdf(f"{file_name}_raw.nc")


def read_DFO_BIO(data_path, save_path):
    print(data_path)
    string_attrs, measurements_attrs, data_lists, i = initialize_variables()

    with pd.read_csv(data_path, chunksize=10 ** 6, low_memory=False, skiprows=[1]) as reader:
        process_chunks(reader, data_lists)
        create_dataset(data_lists, string_attrs, data_path, save_path)

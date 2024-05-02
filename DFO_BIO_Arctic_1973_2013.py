import pandas as pd
import xarray as xr
from datetime import datetime
import numpy as np
import os

class DFO_BIOReader:
    def __init__(self, data_path, save_path, dataset_name):
        self.header = None
        self.data_path = data_path
        self.save_path = save_path
        self.dataset_name = dataset_name

    def initialize_variables(self):
        string_attrs = ['platform', 'chief_scientist', 'cruise_name', 'orig_cruise_id', 'orig_profile_id', 'lat',
                        'lon', 'datestr']
        additional_attrs = ['shallowest_depth', 'deepest_depth', 'timestamp', 'parent_index', ]
        obs_attrs = ['depth', 'press', 'temp', 'psal', ]
        data_lists = {attr: [] for attr in string_attrs + obs_attrs + additional_attrs}
        return string_attrs, obs_attrs, data_lists, 0

    def get_date(self, datestr):
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

    def process_chunks(self, reader, data_lists):
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
                datestr, timestamp = self.get_date(time)
                data_lists['datestr'].append(datestr)
                data_lists['timestamp'].append(timestamp)

                data_lists['depth'].extend(data['depth'])
                data_lists['temp'].extend(data['TEMPPR01'])
                data_lists['press'].extend(data['PRESPR01'])
                data_lists['psal'].extend(data['PSLTZZ01'])

                data_lists['shallowest_depth'].append(min(data['depth'][data['depth'] != 0]))
                data_lists['deepest_depth'].append(max(data['depth']))
                data_lists['parent_index'].extend([i] * len(data['depth']))
                i += 1

    def create_dataset(self, data_lists, string_attrs):
        if not os.path.isdir(self.save_path):
            os.mkdir(self.save_path)
        os.chdir(self.save_path)
        ds = xr.Dataset(
            coords=dict(
                timestamp=(['profile'], data_lists['timestamp']),
                lat=(['profile', ], data_lists['lat']),
                lon=(['profile', ], data_lists['lon']),
            ),
            data_vars=dict(
                **{attr: xr.DataArray(data_lists[attr], dims=['profile']) for attr in string_attrs if
                   attr not in ['lat', 'lon', 'timestamp', 'parent_index', 'datestr']},
                datestr=xr.DataArray(data_lists['datestr'], dims=['profile'],
                                     attrs={"timezone": [value for attr, value in self.header if attr == 'time'][0]}),

                # measurements
                parent_index=xr.DataArray(data_lists['parent_index'], dims=['obs']),
                depth=xr.DataArray(data_lists['depth'], dims=['obs']),
                press=xr.DataArray(data_lists['press'], dims=['obs']),
                temp=xr.DataArray(data_lists['temp'], dims=['obs']),
                psal=xr.DataArray(data_lists['psal'], dims=['obs']),
            ),
            attrs=dict(
                dataset_name=self.dataset_name,
                creation_date=str(datetime.now().strftime("%Y-%m-%d %H:%M")),
            ),
        )

        file_name = self.data_path[self.data_path.rfind("/") + 1:-4]
        ds.to_netcdf(f"{file_name}_raw.nc")

    def run(self):
        string_attrs, measurements_attrs, data_lists, i = self.initialize_variables()

        # read the second line to save information about time and lat and lon
        with open(self.data_path, 'r', newline='') as file:
            line1 = file.readline().split(",")
            line2 = file.readline().split(",")
            self.header = zip(line1, line2)

        with pd.read_csv(self.data_path, chunksize=10 ** 6, low_memory=False, skiprows=[1]) as reader:
            self.process_chunks(reader, data_lists)
            self.create_dataset(data_lists, string_attrs)


def main(data_paths, save_path):
    for path, dataset_name in data_paths.items():
        dfo_bio_reader = DFO_BIOReader(path, save_path, dataset_name)
        dfo_bio_reader.run()


if __name__ == '__main__':
    data_list = {
        '/mnt/storage6/caio/AW_CAA/CTD_DATA/DFO_BIO/DFO_BIO_1973_2022/original_data/bio_historical_arctic_ctd_1921_2d43_ff8c.csv': 'DFO_BIO_1973_2022',
        '/mnt/storage6/caio/AW_CAA/CTD_DATA/DFO_BIO/DFO_BIO_Barrow_1998_2010/original_data/bio_barrow_strait_program_ctd_4450_976d_79dd.csv': 'DFO_BIO_Barrow_1998_2010',
        '/mnt/storage6/caio/AW_CAA/CTD_DATA/DFO_BIO/DFO_BIO_WGreenland_1989_1994/original_data/bio_historical_west_greenland_ctd_dba9_5eb4_a3f0.csv': 'DFO_BIO_WGreenland_1989_1994'}
    save_directory = '/mnt/storage6/caio/AW_CAA/CTD_DATA/DFO_BIO/ncfiles_raw'
    main(data_list, save_directory)

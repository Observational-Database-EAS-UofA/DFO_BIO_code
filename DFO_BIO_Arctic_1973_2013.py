"""
This code processes CSV files in the DFO_BIO* dataset, extracting relevant information and saving it as NetCDF files. 
The script handles reading large CSV files in chunks, extracting data into structured lists, and then organizing the data 
into an Xarray Dataset before saving it.

Steps followed in the script:
1. Initialize the DFO_BIOReader class with paths and dataset information.
2. Read and process chunks of data from large CSV files.
3. Organize the data into an Xarray Dataset.
4. Save the Dataset as a NetCDF file.
"""

import pandas as pd
import xarray as xr
from datetime import datetime
import numpy as np
import os


class DFO_BIOReader:
    def __init__(self, data_path, save_path, dataset_name):
        """
        Initializes the DFO_BIOReader with paths and dataset information.

        Parameters:
        - data_path: Path to the CSV file containing the data.
        - save_path: Path where the processed NetCDF files will be saved.
        - dataset_name: Name of the dataset to be used in the NetCDF file.
        """
        self.header = None
        self.data_path = data_path
        self.save_path = save_path
        self.dataset_name = dataset_name

    def initialize_variables(self):
        """
        Initializes lists and variables for data storage.

        Returns:
        - string_attrs: List of string attributes to be extracted.
        - obs_attrs: List of observation attributes to be extracted.
        - data_lists: Dictionary to store extracted data.
        - 0: Initial index value.
        """
        string_attrs = [
            "platform",
            "chief_scientist",
            "cruise_name",
            "orig_cruise_id",
            "orig_profile_id",
            "lat",
            "lon",
            "datestr",
            "depth_row_size",
            "press_row_size",
            "temp_row_size",
            "psal_row_size",
        ]
        additional_attrs = [
            "shallowest_depth",
            "deepest_depth",
            "timestamp",
        ]
        obs_attrs = ["depth", "press", "temp", "psal"]
        data_lists = {attr: [] for attr in string_attrs + obs_attrs + additional_attrs}
        return string_attrs, obs_attrs, data_lists, 0

    def parse_datestr_to_datetime_objects_and_timestamps(self, datestr):
        """
        Parses date strings into datetime objects and timestamps.

        Parameters:
        - datestr: String representation of the date.

        Returns:
        - datestr: Formatted date string.
        - timestamp: Unix timestamp of the date.
        """
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
        """
        Processes chunks of data from the CSV file.

        Parameters:
        - reader: Pandas reader object to read chunks of the CSV file.
        - data_lists: Dictionary to store extracted data.
        """
        for chunk in reader:
            grouped_df = chunk.groupby(
                [
                    "platform_name",
                    "chief_scientist",
                    "cruise_name",
                    "cruise_number",
                    "id",
                    "event_number",
                    "latitude",
                    "longitude",
                    "time",
                ]
            )
            for group, data in grouped_df:
                (
                    platform,
                    chief_scientist,
                    cruise_name,
                    orig_cruise_id,
                    orig_profile_id,
                    _,
                    lat,
                    lon,
                    time,
                ) = group
                data_lists["platform"].append(platform)
                data_lists["chief_scientist"].append(chief_scientist)
                data_lists["cruise_name"].append(cruise_name)
                data_lists["orig_cruise_id"].append(orig_cruise_id)
                data_lists["orig_profile_id"].append(orig_profile_id)
                data_lists["lat"].append(lat)
                data_lists["lon"].append(lon)
                datestr, timestamp = self.parse_datestr_to_datetime_objects_and_timestamps(time)
                data_lists["datestr"].append(datestr)
                data_lists["timestamp"].append(timestamp)

                data_lists["depth"].extend(data["depth"])
                data_lists["temp"].extend(data["TEMPPR01"])
                data_lists["press"].extend(data["PRESPR01"])
                data_lists["psal"].extend(data["PSLTZZ01"])

                data_lists["shallowest_depth"].append(min(data["depth"][data["depth"] != 0]))
                data_lists["deepest_depth"].append(max(data["depth"]))

                data_lists["depth_row_size"].append(len(data["depth"]))
                data_lists["press_row_size"].append(len(data["PRESPR01"]))
                data_lists["temp_row_size"].append(len(data["TEMPPR01"]))
                data_lists["psal_row_size"].append(len(data["PSLTZZ01"]))

    def create_dataset(self, data_lists, string_attrs):
        """
        Creates an Xarray Dataset from the processed data.

        Parameters:
        - data_lists: Dictionary containing lists of data for each attribute.
        - string_attrs: List of string attributes to include in the Dataset.

        The method saves the Dataset as a NetCDF file in the specified save path.
        """
        if not os.path.isdir(self.save_path):
            os.mkdir(self.save_path)
        os.chdir(self.save_path)
        ds = xr.Dataset(
            coords=dict(
                timestamp=(["profile"], data_lists["timestamp"]),
                lat=(["profile"], data_lists["lat"]),
                lon=(["profile"], data_lists["lon"]),
            ),
            data_vars=dict(
                **{
                    attr: xr.DataArray(data_lists[attr], dims=["profile"])
                    for attr in string_attrs
                    if attr not in ["lat", "lon", "timestamp", "datestr"]
                },
                datestr=xr.DataArray(
                    data_lists["datestr"],
                    dims=["profile"],
                    attrs={"timezone": [value for attr, value in self.header if attr == "time"][0]},
                ),
                # measurements
                depth=xr.DataArray(data_lists["depth"], dims=["obs"]),
                press=xr.DataArray(data_lists["press"], dims=["obs"]),
                temp=xr.DataArray(data_lists["temp"], dims=["obs"]),
                psal=xr.DataArray(data_lists["psal"], dims=["obs"]),
            ),
            attrs=dict(
                dataset_name=self.dataset_name,
                creation_date=str(datetime.now().strftime("%Y-%m-%d %H:%M")),
            ),
        )

        file_name = self.data_path[self.data_path.rfind("/") + 1 : -4]
        ds.to_netcdf(f"{file_name}_raw.nc")

    def run(self):
        """
        Executes the data processing workflow.
        Initializes variables, reads data, processes chunks, and creates the Dataset.
        """
        string_attrs, measurements_attrs, data_lists, i = self.initialize_variables()

        # Read the second line to save information about time, latitude, and longitude
        with open(self.data_path, "r", newline="") as file:
            line1 = file.readline().split(",")
            line2 = file.readline().split(",")
            self.header = zip(line1, line2)

        with pd.read_csv(self.data_path, chunksize=10**6, low_memory=False, skiprows=[1]) as reader:
            self.process_chunks(reader, data_lists)
            self.create_dataset(data_lists, string_attrs)


def main(data_paths, save_path):
    """
    Main function to run the DFO_BIOReader.

    Parameters:
    - data_paths: Dictionary of paths to CSV files and their corresponding dataset names.
    - save_path: Path to the directory where the processed NetCDF files will be saved.
    """
    for path, dataset_name in data_paths.items():
        dfo_bio_reader = DFO_BIOReader(path, save_path, dataset_name)
        dfo_bio_reader.run()


if __name__ == "__main__":
    data_list = {
        "/mnt/storage6/caio/AW_CAA/CTD_DATA/DFO_BIO/DFO_BIO_1973_2022/original_data/bio_historical_arctic_ctd_1921_2d43_ff8c.csv": "DFO_BIO_1973_2022",
        "/mnt/storage6/caio/AW_CAA/CTD_DATA/DFO_BIO/DFO_BIO_Barrow_1998_2010/original_data/bio_barrow_strait_program_ctd_4450_976d_79dd.csv": "DFO_BIO_Barrow_1998_2010",
        "/mnt/storage6/caio/AW_CAA/CTD_DATA/DFO_BIO/DFO_BIO_WGreenland_1989_1994/original_data/bio_historical_west_greenland_ctd_dba9_5eb4_a3f0.csv": "DFO_BIO_WGreenland_1989_1994",
    }
    save_directory = "/mnt/storage6/caio/AW_CAA/CTD_DATA/DFO_BIO/ncfiles_raw"
    main(data_list, save_directory)

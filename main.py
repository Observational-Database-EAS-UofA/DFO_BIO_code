import os

from read import read_DFO_BIO


def get_data():
    data_paths = [
        '/home/novaisc/workspace/obs_database/AW_CAA/CTD_DATA/DFO_BIO/DFO_BIO_1973_2022/original_data/bio_historical_arctic_ctd_1921_2d43_ff8c.csv',
        '/home/novaisc/workspace/obs_database/AW_CAA/CTD_DATA/DFO_BIO/DFO_BIO_Barrow_1998_2010/original_data/bio_barrow_strait_program_ctd_4450_976d_79dd.csv',
        '/home/novaisc/workspace/obs_database/AW_CAA/CTD_DATA/DFO_BIO/DFO_BIO_WGreenland_1989_1994/original_data/bio_historical_west_greenland_ctd_dba9_5eb4_a3f0.csv']
    save_path = '/home/novaisc/workspace/obs_database/AW_CAA/CTD_DATA/DFO_BIO/ncfiles_raw'

    for data_path in data_paths:
        read_DFO_BIO(data_path, save_path)


if __name__ == '__main__':
    get_data()

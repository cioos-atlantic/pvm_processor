from time import strftime, strptime
import numpy as np
import pandas as pd
import xarray as xr
import re
import os
from pathlib import Path

FORMAT_CSV = "csv"
FORMAT_NetCDF = "nc"

DATASET_TIMESERIES = "timeseries"
DATASET_WAVE_SPECTRA = "wave_spectra"

def process_pvm(config, raw_data):

    # Separates the data into daily chunks
    ts_interval_format = "%Y%m%d"
    ws_interval_format = "%Y%m%d%H%M%S"

    timeseries_fields = config["timeseries_config"]
    wave_spectra_data = config["wave_spectra_config"]

    # A single PVM record
    # 12/09/2022;11:30
    #  1.3
    #  2.4
    #  7.2
    # 12/09/2022;11:00
    #  1.6
    #  4.9
    #  9.1
    # 98
    # 26
    # 14.7
    # 3.963760
    # 40.00;0.000020;36;69
    # 33.33;0.000092;23;64
    # ...
    #  9.09;1.000000;98;26
    # ...
    #  1.72;0.000000;353;0

    # French Date/Time Pattern: DD/MM/YYYY;HH:mm
    date_time_format = config["datetime_format_in"]

    datetime_pattern = re.compile(config["datetime_pattern"])
    numerical_pattern = re.compile(config["numerical_pattern"])
    spectral_pattern = re.compile(config["spectral_pattern"])

    # Remove first row as it is unique and contains the station identification
    dataset_id = raw_data.pop(0)

    print(dataset_id)

    row_counter = 0
    blank_counter = 0
    blank_max = config["blank_max"]
    start_of_record = None

    for raw_row in raw_data:
        # get rid of whitespace and line endings
        row = raw_row.strip()

        datetime_check = datetime_pattern.match(row)
        number_check = numerical_pattern.match(row)
        spectra_check = spectral_pattern.match(row)

        if datetime_check:
            value = strftime(config["datetime_format_out"], strptime(row, date_time_format))
            # print("Found Datetime!", value)

            # The first date/time of a batch of values is the date/time index of 
            # the entire record the second occurrence of a date/time field later in 
            # the record is the internal timestamp of the station itself.
            if not start_of_record:
                start_of_record = value
            
        if number_check:
            value = row
            # print("Found Number!", row)

        if spectra_check:
            value = row.split(';')
            # print("Found wave spectra", row)

        # if at least one check verifies then we're still on a single record
        if datetime_check or number_check or spectra_check:
            try:
                # print("Adding Record: ", timeseries_fields[row_counter]["field_name"], value)
                timeseries_fields[row_counter]["index"].append(start_of_record)
                timeseries_fields[row_counter]["data"].append(value)

            except IndexError:
                # print("Outisde of timeseries, must be wave spectra!")
                wave_spectra_data["index"].append(start_of_record)
                wave_spectra_data["data"].append(value)

            row_counter = row_counter + 1
        # increment blank lines until blank_max has been reached, 
        # once two consecutive blank rows are encountered a new record can begin
        elif blank_counter < blank_max:
            # print("Found a blank!", blank_counter)
            blank_counter = blank_counter + 1
        else:
            # after all data type checks fail and blank_max has been met or 
            # exceeded, a new record has begun.  Reset counters and start_or_record
            # value
            # print("*** NEW RECORD! RESET EVERYTHING! ***")
            row_counter = 0
            blank_counter = 0
            start_of_record = None

    print("TimeSeries Data:")

    series_list = {}

    for idx, field in enumerate(timeseries_fields):
        print(f"Adding {field['field_name']} to series list...")
        series_list[field["field_name"]] = pd.Series(field['data'], index=field['index'], name=field["field_name"])

    print("Building timeseries dataframe...")

    # Join list of pandas series into a dataframe using the same timestamp
    df_timeseries = pd.concat(series_list, axis='columns')
    df_timeseries.index = pd.to_datetime(df_timeseries.index)

    print(df_timeseries.info())
    print(df_timeseries)

    print("Wave Spectra Data:")
    wave_spectra_data["dataframe"] = pd.DataFrame(wave_spectra_data["data"], index=wave_spectra_data["index"], columns=wave_spectra_data["field_names"])
    wave_spectra_data["dataframe"].index = pd.to_datetime(wave_spectra_data["dataframe"].index)

    print(wave_spectra_data["dataframe"].info())
    print(wave_spectra_data["dataframe"])

    # Output options: CSV (pandas) or NetCDF (xarray)

    # get list of output paths for each dataset
    ts_log_files = df_timeseries.index.strftime(config["timeseries_output"]).unique()
    ts_file_index = df_timeseries.index.strftime(ts_interval_format).unique()

    ws_log_files = df_timeseries.index.strftime(config["wave_spectra_output"]).unique()
    ws_file_index = wave_spectra_data["dataframe"].index.strftime(ws_interval_format).unique()

    # print(ts_log_files)
    # print(ts_file_index)

    # print(ws_log_files)
    # print(ws_file_index)

    # Can encounter duplicates, will need to account for this
    for index, log_file in enumerate(ts_log_files):
        ts_output_path = f"{log_file}.{config['output_format']}"
        # Find the corresponding date index to the generated file list
        date_key = ts_file_index[index]
        df_section = df_timeseries.loc[date_key]
        skip_file = False

        if not Path(os.path.dirname(ts_output_path)).exists():
            os.makedirs(os.path.dirname(ts_output_path))

        if Path(ts_output_path).exists():
            print(f"Existing file found! {ts_output_path}")
            df_existing_data = pd.read_csv(ts_output_path, index_col=config["datetime_index_field"], parse_dates=True)
            
            # Insert timestamp column based on index in first position, format
            # according to datetime output format
            df_existing_data.insert(0, config["datetime_index_field"], df_existing_data.index.to_series().dt.strftime(config["datetime_format_out"]))

            # print("Current Data:")
            # print(df_section.info())
            # print(df_section)

            # print("Existing Data:")
            # print(df_existing_data.info())
            # print(df_existing_data)

            # print("Index Differences:")
            # Checks for records in existing file not present in new dataframe
            diff_idx_existing = df_existing_data.index.difference(df_section.index, sort=False)

            # Checks for records in new dataframe that are not in existing file,
            # if there is a difference then both dataframes should be merged.
            diff_idx_new = df_section.index.difference(df_existing_data.index, sort=False)
            # print(diff_idx_existing)
            # print(diff_idx_new)

            if not diff_idx_new.empty:
                print("Differences found between existing file and new data - merging dataframes...")
                # print(df_section.loc[diff_idx_new])

                df_merged = pd.concat([df_existing_data, df_section.loc[diff_idx_new]])
                df_section = df_merged
            else:
                print("File indexes match, no new data detected, skipping.")
                skip_file = True

        if not skip_file:
            # df_section.sort_index().to_csv(ts_output_path, index=False)
            write_dataset(df_section, DATASET_TIMESERIES, config["output_format"], date_key, ts_output_path)

    # By separating into individual files per spectral observation there is no 
    # need to account for duplicate observances
    for index, log_file in enumerate(ws_log_files):
        ws_output_path = f"{log_file}.{config['output_format']}"
        # Find the corresponding date index to the generated file list
        date_key = ws_file_index[index]

        if not Path(os.path.dirname(ws_output_path)).exists():
            os.makedirs(os.path.dirname(ws_output_path))

        if not Path(ws_output_path).exists():
            write_dataset(wave_spectra_data["dataframe"], DATASET_WAVE_SPECTRA, config["output_format"], date_key, ws_output_path)
            
        else:
            print(f"Wave Spectra file ({ws_output_path}) already exists, skipping...")


def write_dataset(data, dataset_type, destination_format, date_key, output_path):
    if destination_format == FORMAT_CSV:
        data.loc[date_key].sort_index().to_csv(output_path, index=False)
        pass

    elif destination_format == FORMAT_NetCDF:
        # data is assumed to be a Pandas DataFrame
        data_xr = xr.Dataset.from_dataframe(data)
        
        # set metadata based on config
        if dataset_type == DATASET_TIMESERIES:
            pass
        elif dataset_type == DATASET_WAVE_SPECTRA:
            pass

        print(data_xr)
        result = data_xr.to_netcdf(output_path, mode='a', format='NETCDF4_CLASSIC')
        print(f"File write result: {result}")
        pass

from time import strftime, strptime
import pandas as pd
import re

timeseries_fields = [
    {"field_name": "timestamp", "long_name": "Start of Record", "index": [], "data": [], "series": None}, 
    {"field_name": "wave_ht_sig", "long_name": "Significant Wave Height (H1/3)", "index": [], "data": [], "series": None}, 
    {"field_name": "wave_ht_max", "long_name": "Maximum Wave Height (Hmax)", "index": [], "data": [], "series": None}, 
    {"field_name": "wave_period_avg", "long_name": "Sea Surface Wave Mean Period (TH1/3)", "index": [], "data": [], "series": None}, 
    {"field_name": "timestamp_internal", "long_name": "Internal Timestamp", "index": [], "data": [], "series": None}, 
    {"field_name": "wave_ht_sig_Hm0", "long_name": "Spectral Significant Wave Height (Hm0)", "index": [], "data": [], "series": None}, 
    {"field_name": "wave_period", "long_name": "Mean wave period T(0,2) (s)", "index": [], "data": [], "series": None}, 
    {"field_name": "wave_period_peak", "long_name": "Tpeak (s)", "index": [], "data": [], "series": None}, 
    {"field_name": "wave_direction_peak_spectral", "long_name": "(degree)", "index": [], "data": [], "series": None}, 
    {"field_name": "wave_spread_peak", "long_name": "(degree)", "index": [], "data": [], "series": None}, 
    {"field_name": "sea_surface_temp_avg", "long_name": "Sea Surface Temp (C)", "index": [], "data": [], "series": None}, 
    {"field_name": "peak_power_spectral_density", "long_name": "peak power spectral density (m^2 Hz-1)", "index": [], "data": [], "series": None}, 
]

wave_spectra_data = {"index": [], "data": [], "field_names": ["period", "relative_PSD", "direction", "spread"], 'dataframe': None}

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
FRA_date_time_format = '%d/%m/%Y;%H:%M'

datetime_pattern = re.compile('^\d+\/\d+\/\d+;\d+:\d+$')
numerical_pattern = re.compile('^\d+(\.\d+)?$')
spectral_pattern = re.compile('^\d+\.\d+;\d+\.\d+;\d+;\d+$')

fp = open("sample/97501 - Saint-Pierre et Miquelon.pvm", 'r', encoding='UTF-8')
raw_data = fp.readlines()

# print(fp)

# Remove first row as it is unique and contains the station identification
dataset_id = raw_data.pop(0)

print(dataset_id)

wave_data = []
spectral_data = []
row_counter = 0
blank_counter = 0
blank_max = 1
start_of_record = None
row_max = 75 # 76 rows per record, last 64 are wave spectra

for raw_row in raw_data:
    # get rid of whitespace and line endings
    row = raw_row.strip()

    datetime_check = datetime_pattern.match(row)
    number_check = numerical_pattern.match(row)
    spectra_check = spectral_pattern.match(row)

    if datetime_check:
        value = strftime('%Y-%m-%dT%H:%M:%SZ', strptime(row, FRA_date_time_format))
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

fp.close()

print("TimeSeries Data:")

series_list = {}

for idx, field in enumerate(timeseries_fields):
    print(f"Adding {field['field_name']} to series list...")
    series_list[field["field_name"]] = pd.Series(field['data'], index=field['index'], name=field["field_name"])

print("Building timeseries dataframe...")

# Join list of pandas series into a dataframe using the same timestamp
df_timeseries = pd.concat(series_list, axis='columns')

print(df_timeseries.info())
print(df_timeseries)

print("Wave Spectra Data:")
wave_spectra_data["dataframe"] = pd.DataFrame(wave_spectra_data["data"], index=wave_spectra_data["index"], columns=wave_spectra_data["field_names"])
print(wave_spectra_data["dataframe"])
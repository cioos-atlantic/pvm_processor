import argparse
import json
from process_pvm import process_pvm
import os
from pathlib import Path
from copy import deepcopy
# import netCDF4

if __name__ == "__main__":
    raw_args = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    raw_args.add_argument(
        "configuration_file",
        help="Path to a JSON configuration file",
    )

    raw_args.add_argument(
        "source_file",
        help='Path to a specific file to process (default) or path to a directory of PVM files to be processed if using the --batch flag',
    )

    raw_args.add_argument(
        "--format",
        help="Specifies the output format for the resulting datasets (CSV or NetCDF).  Acceptable values: csv or nc, default: csv",
        action="store",
        default="csv"
    )

    raw_args.add_argument(
        "--batch",
        help="Specifies the kind of wildcard pattern to match files inside the source_file directory and subdirectories.  Uses glob pattern matching.",
        action="store",
    )

    prog_args = raw_args.parse_args()

    config = json.load(open(prog_args.configuration_file))
    config["output_format"] = prog_args.format

    if prog_args.batch:
        # Load list of files into a directory
        for source_file in Path(os.path.dirname(prog_args.source_file)).glob(prog_args.batch):
            # re-loading the configuration file each time prevents the internal 
            # dataframes from becoming ultra massive
            fresh_config = deepcopy(config)
            print(f"Loading: {source_file}...")

            fp = open(source_file, 'r', encoding='UTF-8')
            raw_data = fp.readlines()
            fp.close()
            
            print(f"Processing {source_file} ...")
            process_pvm(fresh_config, raw_data)
            print("Done.")
    else:
        fp = open(prog_args.source_file, 'r', encoding='UTF-8')
        raw_data = fp.readlines()
        fp.close()
        
        print(f"Processing {prog_args.source_file} ...")
        process_pvm(config, raw_data)
        print("Done.")

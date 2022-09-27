import argparse
import json
from pvm_processor.process_pvm import process_pvm

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
        "--batch",
        help="Source file argument is a directory path and all files in the directory should be processed using the same configuration",
        action="store_true",
    )

    prog_args = raw_args.parse_args()

    config = json.load(open(prog_args.configuration_file))

    if config.batch:
        # Load list of files into a directory
        pass

    # fp = open("sample/97501 - Saint-Pierre et Miquelon.pvm", 'r', encoding='UTF-8')
    fp = open(prog_args.source_file, 'r', encoding='UTF-8')
    raw_data = fp.readlines()
    fp.close()

    process_pvm(config, raw_data)


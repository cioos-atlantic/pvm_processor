# pvm_processor

A python tool to transform PVM wave data files into timeseries and wave spectra files appropriate for ingestion into an OPeNDAP server.

## Setup

```
pip install .
```

## Usage

```
usage: python -m pvm_processor [-h] [--batch BATCH] configuration_file source_file

positional arguments:
  configuration_file  Path to a JSON configuration file
  source_file         Path to a specific file to process (default) or path to a directory of PVM files to be processed if using the --batch flag

options:
  -h, --help          show this help message and exit
  --batch BATCH       Specifies the kind of wildcard pattern to match files inside the source_file directory and subdirectories. Uses glob pattern matching. (default: None) 
```

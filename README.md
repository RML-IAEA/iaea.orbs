# IAEA ORBS Data Parser

## Overview

This Python package provides tools for parsing and extracting data from the Overacting Radiation-Monitoring Data Browsing System (ORBS) in Japan's coastal oceans.
The package allows downloading, processing, and transforming CSV data related to seawater, fish, and seaweed measurements.

## Features

- Download datasets from `ORBS` for seawater, fish, and seaweed measurements
- Process and transform raw `CSV` files with manually created [Station by ID JSON file](stations/station_by_id.json)
- Extract Seawater station names and coordinates from [Alps Seawater PDF](src/iaea/orbs/stations/R6zahyo.pdf) , [Extracted file](src/iaea/orbs/stations/station_points.csv)
- Generate Fully Data `JSON` output
- Generate structured `CSV` files from `JSON` output

## Requirements

- Python 3.8+
- Dependencies listed in pyproject.toml

## Installation

```sh
pip install .
```

## Running

The `generate-data` script allows you to download each station's `CSV` file from `ORBS`and generate the `JSON`  and `CSV` files for sample types : `Fish`, `Seawater`, `Seaweed`.

```sh
generate-data -h
usage: generate-data [-h] [-d DOWNLOAD_DIR] [-json TRANSFORM_JSON_DIR] [-csv TRANSFORM_CSV_DIR]

ORBS Data Extraction Tool

options:
  -h, --help            show this help message and exit
  -d DOWNLOAD_DIR, --download_dir DOWNLOAD_DIR
                        Directory to download CSV files (default: downloaded_CSVs)
  -json TRANSFORM_JSON_DIR, --transform_json_dir TRANSFORM_JSON_DIR
                        Directory to save transformed JSON files (default: stations/transformed/json)
  -csv TRANSFORM_CSV_DIR, --transform_csv_dir TRANSFORM_CSV_DIR
                        Directory to save transformed CSV files (default: stations/transformed/csv)
```

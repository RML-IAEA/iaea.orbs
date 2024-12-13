
import argparse
from os.path import join
import pkg_resources

from iaea.orbs import logger
from iaea.orbs import HelpFormatter

from iaea.orbs.process.download_orbs import DownloadConfig
from iaea.orbs.process.download_orbs import download_dataset
from iaea.orbs.process.generate_json import DataProcessor
from iaea.orbs.process.generate_csv import extract_fish_and_seaweed_measurements
from iaea.orbs.process.generate_csv import extract_seawater_measurements
from iaea.orbs.utils import generate_output_path
from iaea.orbs.utils import load_json_data


STATIONS_INFO = pkg_resources.resource_filename(
    "iaea.orbs", "stations/station_by_id.json" )

FISH_KEY = "Fish"
SEAWATER_KEY = "Seawater"
SEAWEED_KEY = "Seaweed"


def parse_arguments():
    """
    Parse command-line arguments for ORBS data processing
    """
    parser = argparse.ArgumentParser(
        description="ORBS Data Extraction Tool",
        formatter_class=HelpFormatter
    )

    parser.add_argument(
        "-d", "--download_dir", 
        type=str,
        default="downloaded_CSVs",
        help="Directory to download CSV files (default: %(default)s)"
    )

    parser.add_argument(
        "-json", "--transform_json_dir",
        type=str,
        default="stations/transformed/json",
        help="Directory to save transformed JSON files (default: %(default)s)"
    )

    parser.add_argument(
        "-csv", "--transform_csv_dir",
        type=str,
        default="stations/transformed/csv",
        help="Directory to save transformed CSV files (default: %(default)s)"
    )

    return parser.parse_args()


def main():
    """Main function that orchestrates the data processing"""
    args = parse_arguments()   
    base_url = "https://www.monitororbs.jp/en/download"

    configs = [
        DownloadConfig(
            category="Seawater",
            prefix="Seawater_10_",
            start_num=1,
            end_num=400,
            min_delay=1,
            max_delay=5
        ),
        DownloadConfig(
            category="Fishes",
            prefix="Fishes_20_",
            start_num=256,
            end_num=400,
            min_delay=1,
            max_delay=5
        ),
        DownloadConfig(
            category="Seaweeds",
            prefix="Seaweeds_30_",
            start_num=359,
            end_num=400,
            min_delay=1,
            max_delay=5
        )
    ]

    # Download each dataset
    for config in configs:
        logger.info("Starting download of %s dataset from number %d to %d",
                config.category, config.start_num, config.end_num)
        download_dataset(base_url, config, args.download_dir)
        logger.info("Completed download of %s dataset", config.category)

    # save json files
    processor = DataProcessor(STATIONS_INFO, args.download_dir, args.transform_json_dir)
    processor.process_all_data()
    logger.info("Full data JSON file saved to '%s'", args.transform_json_dir)

    # save csv files
    def save_csv(sample_type):
        json_output = generate_output_path(args.transform_json_dir, sample_type, "json")
        json_data = load_json_data(json_output)
        csv_output = generate_output_path(args.transform_csv_dir, sample_type, "csv")
        if sample_type == SEAWATER_KEY:
            extract_seawater_measurements(json_data, csv_output)
        else:
            extract_fish_and_seaweed_measurements(json_data, csv_output)
        logger.info("%s data file saved to '%s'", sample_type, args.transform_csv_dir)


    save_csv(FISH_KEY)
    save_csv(SEAWATER_KEY)
    save_csv(SEAWEED_KEY)


if __name__ == "__main__":
    main()

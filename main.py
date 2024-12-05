from process.download_orbs import DownloadConfig
from process.download_orbs import download_dataset
from process.generate_csv import extract_fish_and_seaweed_measurements
from process.generate_csv import extract_seawater_measurements
from process.generate_json import DataProcessor
from utils import logger
from utils import load_json_data
from utils import save_json


def main():
    """Main function that orchestrates the data processing"""
    base_url = "https://www.monitororbs.jp/en/download"
    output_dir = "downloaded_CSVs"


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
        download_dataset(base_url, config, output_dir)
        logger.info("Completed download of %s dataset", config.category)

    processor = DataProcessor("stations/station_by_id.json")
    final_dict = processor.process_all_data()

    station_data_path = "stations/transformed/json/station_data.json"
    seawater_data_path = "stations/transformed/csv/seawater_data.csv"
    seaweed_data_path = "stations/transformed/csv/seaweed_data.csv"
    fish_data_path = "stations/transformed/csv/fish_data.csv"

    save_json(station_data_path, final_dict)
    logger.info("Full data json file saved to '%s'", station_data_path)

    station_coord = load_json_data(station_data_path)

    extract_fish_and_seaweed_measurements(station_coord,"Fish",fish_data_path)
    logger.info("Fish data file saved to '%s'", fish_data_path)
    extract_fish_and_seaweed_measurements(station_coord, "Seaweed", seaweed_data_path)
    logger.info("Seawwed data file saved to '%s'", seaweed_data_path)
    extract_seawater_measurements(station_coord, seawater_data_path)
    logger.info("Seawater data file saved to '%s'", seawater_data_path)

if __name__ == "__main__":
    main()

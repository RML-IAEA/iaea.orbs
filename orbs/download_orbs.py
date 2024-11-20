
import time
from dataclasses import dataclass
import logging
from pathlib import Path
import random
import requests


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

@dataclass
class DownloadConfig:
    category: str      # e.g., "Seawater" or "Fishes" or "Seaweeds"
    prefix: str        # e.g., "Seawater_10_" or "Fishes_20_" or "Seaweeds_30_"
    start_num: int     # Starting file number
    end_num: int       # Ending file number (inclusive)
    min_delay: float   # Minimum delay between downloads
    max_delay: float   # Maximum delay between downloads


def download_csv(base_url: str, config: DownloadConfig, file_num: int, output_dir: str) -> bool:
    """
    Download a single CSV file
    """
    url = f"{base_url}/{config.category}/{config.prefix}{file_num}.csv"
    output_path = Path(output_dir) / f"{config.prefix}{file_num}.csv"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        if 'text/csv' not in response.headers.get('content-type', '').lower():
            logging.warning("File %d doesn't appear to be a CSV. Skipping...", file_num)
            return False

        output_path.write_bytes(response.content)
        logging.info("Successfully downloaded %s file %d", config.category, file_num)
        return True

    except requests.exceptions.HTTPError as err:
        if response.status_code == 404:
            logging.info("%s file %d not found. Skipping...", config.category, file_num)
            return False
        logging.error("HTTP error occurred while downloading file %d: %s", file_num, err)
        return False
    except Exception as exc:
        logging.error("Error downloading file %d: %s", file_num, exc)
        return False


def download_dataset(base_url: str, config: DownloadConfig, output_dir: str) -> None:
    """
    Download a complete dataset with the given configuration
    """

    category_dir = Path(output_dir) / config.category
    category_dir.mkdir(parents=True, exist_ok=True)

    file_num = config.start_num
    while file_num <= config.end_num:
        # Random delay between configured values
        delay = random.uniform(config.min_delay, config.max_delay)
        time.sleep(delay)

        download_csv(base_url, config, file_num, str(category_dir))
        file_num += 1


def main():
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
        logging.info("Starting download of %s dataset from number %d to %d",
                    config.category, config.start_num, config.end_num)
        download_dataset(base_url, config, output_dir)
        logging.info("Completed download of %s dataset", config.category)

if __name__ == "__main__":
    main()

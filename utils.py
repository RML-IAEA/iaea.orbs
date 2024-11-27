import json
import logging
from dms2dec.dms_convert import dms2dec


def get_logger():
    """
    Configure and return a logger with file output
    """
    logger = logging.getLogger("custom_logger")
    if not logger.hasHandlers():
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler("status.log")
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s : %(message)s", "%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def load_json_data(data_path):
    """
    Load data from a JSON file
    """
    with open(data_path, "r", encoding="utf8") as json_file:
        station_coord = json.load(json_file)
    return station_coord


def save_json(json_path, dictionary: dict):
    """
    Save JSON data to a file
    """
    with open(json_path, "w", encoding="utf-8") as outfile:
        json.dump(dictionary, outfile, indent=4)


def dms_to_dd(degrees, minutes, seconds):
    return float(degrees) + (float(minutes) / 60) + (float(seconds) / 3600)


def parse_dms_coordinates(text):
    """
    Extract latitude and longitude coordinates from a given text
    """
    if not text:
        return None, None
    lat, lon = dms2dec(text.split('/')[0]), dms2dec(text.split("/")[1])
    return lat, lon

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


logger = get_logger()


def float_data(measure, key):
    """
    Safely convert measurement values to float, handling various formats
    Returns:
        float or None: Converted float value, or None if conversion fails
    """
    if not measure or key not in measure:
        return None

    value = measure[key]
    if value is None or value == '':
        return None

    value = str(value).strip()

    if '±' in value:
        parts = value.split('±')
        try:
            return float(parts[0].strip()), float(parts[1].strip())
        except (ValueError, TypeError, IndexError):
            logger.error("Could not convert value with uncertainty: '%s'", value)
            return None, None


    value = value.replace('%', '')
    try:
        return float(value)
    except (ValueError, TypeError):
        logger.error("Could not convert value: '%s'", value)
        return None


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

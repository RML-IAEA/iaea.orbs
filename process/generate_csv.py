import pandas as pd
from utils import float_data


def extract_fish_and_seaweed_measurements(station_coord, sample_type, output_path):
    """
    Transform fish or seaweed radiation measurement data into a flat CSV format
    """
    flattened_data = []
    for station in station_coord[sample_type]:
        for measurement in station["data"]:
            record = {
                "id": station.get("id"),
                "org": station.get("org"),
                "station": station.get("station"),
                "lat": float_data(station, "lat"),
                "lon": float_data(station, "lon"),
                "Sampling_data": measurement.get("Date and time of Sampling"),
                "Sample": measurement.get("Sample", None),
                "Radionuclide": measurement.get("Radionuclide", None),
                "Dt": float_data(measurement, "Dt"),
                "ND": float_data(measurement, "ND"),
                "Unit": measurement.get("Unit", None)
            }
            flattened_data.append(record)

    df = pd.DataFrame(flattened_data)
    df.to_csv(output_path, index=False)


def extract_seawater_measurements(station_coord, output_path):
    """
    Transform seawater radiation measurement data into a flat CSV format
    """
    flattened_data = []
    for station in station_coord["Seawater"]:
        for depth_info in station["depth_data"]:
            for measurement in depth_info["data"]:
                record = {
                    "id": station.get("id"),
                    "org": station.get("org"),
                    "station": station.get("station"),
                    "lat": float_data(station, "lat"),
                    "lon": float_data(station, "lon"),
                    "depth": depth_info.get("depth"),
                    "begperiod": measurement.get("begperiod"),
                    "Cs-134": float_data(measurement, "Cs-134"),
                    "Cs-134_nd": float_data(measurement, "Cs-134_nd"),
                    "Cs-137": float_data(measurement, "Cs-137"),
                    "Cs-137_nd": float_data(measurement, "Cs-137_nd"),
                    "H-3": float_data(measurement, "H-3"),
                    "H-3_nd": float_data(measurement, "H-3_nd")
                }
                flattened_data.append(record)

    df = pd.DataFrame(flattened_data)
    df.to_csv(output_path, index=False)

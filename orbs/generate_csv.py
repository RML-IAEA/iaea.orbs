import pandas as pd

from utils import load_json_data


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
                "lat": station.get("lat"),
                "lon": station.get("lon"),
                "Sampling_data": measurement.get("Date and time of Sampling"),
                "Sample": measurement.get("Sample", None),
                "Radionuclide": measurement.get("Radionuclide", None),
                "Dt": measurement.get("Dt", None),
                "ND": measurement.get("ND", None),
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
                    "lat": station.get("lat"),
                    "lon": station.get("lon"),
                    "depth": depth_info.get("depth"),
                    "begperiod": measurement.get("begperiod"),
                    "Cs-134": measurement.get("Cs-134", None),
                    "Cs-134_nd": measurement.get("Cs-134_nd", None),
                    "Cs-137": measurement.get("Cs-137", None),
                    "Cs-137_nd": measurement.get("Cs-137_nd"),
                    "H-3": measurement.get("H-3", None),
                    "H-3_nd": measurement.get("H-3_nd", None)
                }
                flattened_data.append(record)

    df = pd.DataFrame(flattened_data)
    df.to_csv(output_path, index=False)


def main():
    """
    Main function to orchestrate data transformation for different sample types
    """
    station_data_path = "stations/transformed/json/station_data.json"
    station_coord = load_json_data(station_data_path)

    extract_fish_and_seaweed_measurements(station_coord,"Fish",
                                          "stations/transformed/csv/fish_data.csv")
    extract_fish_and_seaweed_measurements(station_coord, "Seaweed",
                                          "stations/transformed/csv/seaweed_data.csv")
    extract_seawater_measurements(station_coord,
                                  "stations/transformed/csv/seawater_data.csv")

if __name__ == "__main__":
    main()

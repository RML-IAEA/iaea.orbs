import pandas as pd


def extract_fish_and_seaweed_measurements(station_coord, output_path):
    """
    Transform fish or seaweed radiation measurement data into a flat CSV format
    """
    flattened_data = []
    for station in station_coord:
        for measurement in station["data"]:
            record = {
                "id": station.get("id"),
                "org": station.get("org"),
                "station": station.get("station"),
                "lat": station.get("lat", None),
                "lon": station.get("lon", None),
                "begperiod": measurement.get("begperiod"),
                "Sample": measurement.get("Sample", None),
                "Radionuclide": measurement.get("Radionuclide", None),
                "Dt": measurement.get("Dt", None),
                "Dt_unc": measurement.get("Dt_unc", None),
                "ND": measurement.get("ND", None),
                "ND_unc": measurement.get("ND_unc", None),
                "Unit": measurement.get("Unit", None)
            }
            flattened_data.append(record)

    df = pd.DataFrame(flattened_data)
    df = df.dropna(axis=1, how='all')
    df.to_csv(output_path, index=False)


def extract_seawater_measurements(station_coord, output_path):
    """
    Transform seawater radiation measurement data into a flat CSV format
    """
    flattened_data = []
    for station in station_coord:
        for depth_info in station["depth_data"]:
            for measurement in depth_info["data"]:

                record = {
                    "id": station.get("id"),
                    "org": station.get("org"),
                    "station": station.get("station"),
                    "lat": station.get("lat", None),
                    "lon": station.get("lon", None),
                    "begperiod": measurement.get("begperiod"),
                    "Cs-134": measurement.get("Cs-134", None),
                    "Cs-134_unc": measurement.get("Cs-134_unc", None),
                    "Cs-134_nd": measurement.get("Cs-134_nd", None),
                    "Cs-134_nd_unc": measurement.get("Cs-134_nd_unc", None),
                    "Cs-137": measurement.get("Cs-137", None),
                    "Cs-137_unc": measurement.get("Cs-137_unc", None),
                    "Cs-137_nd": measurement.get("Cs-137_nd", None),
                    "Cs-137_nd_unc": measurement.get("Cs-137_nd_unc", None),
                    "H-3": measurement.get("H-3", None),
                    "H-3_unc": measurement.get("H-3_unc", None),                    
                    "H-3_nd": measurement.get("H-3_nd", None),
                    "H-3_nd_unc": measurement.get("H-3_nd_unc", None)
                }
                flattened_data.append(record)

    df = pd.DataFrame(flattened_data)
    df = df.dropna(axis=1, how='all')
    df.to_csv(output_path, index=False)

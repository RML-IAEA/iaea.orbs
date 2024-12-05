import pandas as pd
from utils import float_data


def extract_fish_and_seaweed_measurements(station_coord, sample_type, output_path):
    """
    Transform fish or seaweed radiation measurement data into a flat CSV format
    """
    flattened_data = []
    for station in station_coord[sample_type]:
        for measurement in station["data"]:

            # Handle Dt and ND with potential uncertainty
            dt = float_data(measurement, "Dt")
            nd = float_data(measurement, "ND")

            record = {
                "id": station.get("id"),
                "org": station.get("org"),
                "station": station.get("station"),
                "lat": float_data(station, "lat"),
                "lon": float_data(station, "lon"),
                "Sampling_data": measurement.get("Date and time of Sampling"),
                "Sample": measurement.get("Sample", None),
                "Radionuclide": measurement.get("Radionuclide", None),
                "Dt": dt[0] if isinstance(dt, tuple) else dt,
                "Dt_unc": dt[1] if isinstance(dt, tuple) else None,
                "ND": nd[0] if isinstance(nd, tuple) else nd,
                "ND_unc": nd[1] if isinstance(nd, tuple) else None,
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
    for station in station_coord["Seawater"]:
        for depth_info in station["depth_data"]:
            for measurement in depth_info["data"]:

                cs134 = float_data(measurement, "Cs-134")
                cs134_nd = float_data(measurement, "Cs-134_nd")
                cs137 = float_data(measurement, "Cs-137")
                cs137_nd = float_data(measurement, "Cs-137_nd")
                h3 = float_data(measurement, "H-3")
                h3_nd = float_data(measurement, "H-3_nd")
                record = {
                    "id": station.get("id"),
                    "org": station.get("org"),
                    "station": station.get("station"),
                    "lat": float_data(station, "lat"),
                    "lon": float_data(station, "lon"),
                    "depth": depth_info.get("depth"),
                    "begperiod": measurement.get("begperiod"),
                    "Cs-134": cs134[0] if isinstance(cs134, tuple) else cs134,
                    "Cs-134_unc": cs134[1] if isinstance(cs134, tuple) else None,
                    "Cs-134_nd": cs134_nd[0] if isinstance(cs134_nd, tuple) else cs134_nd,
                    "Cs-134_nd_unc": cs134_nd[1] if isinstance(cs134_nd, tuple) else None,
                    "Cs-137": cs137 if isinstance(cs137, tuple) else cs137,
                    "Cs-137_unc": cs137[1] if isinstance(cs137, tuple) else None,
                    "Cs-137_nd": cs137_nd[0] if isinstance(cs137_nd, tuple) else cs137_nd,
                    "Cs-137_nd_unc": cs137_nd[1] if isinstance(cs137_nd, tuple) else None,
                    "H-3": h3[0] if isinstance(h3, tuple) else h3,
                    "H-3_unc": h3[1] if isinstance(h3, tuple) else None,                    
                    "H-3_nd": h3_nd[0] if isinstance(h3_nd, tuple) else h3_nd,
                    "H-3_nd_unc": h3_nd[1] if isinstance(h3_nd, tuple) else None,
                }
                flattened_data.append(record)

    df = pd.DataFrame(flattened_data)
    df = df.dropna(axis=1, how='all')
    df = df[df['begperiod'].notna()] 
    df.to_csv(output_path, index=False)

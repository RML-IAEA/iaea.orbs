from os.path import join, splitext, basename
from os import listdir
from typing import Optional, Dict, List, Any
import numpy as np
import pandas as pd

from utils import logger
from utils import load_json_data
from utils import parse_dms_coordinates


class StationMatcher:
    @staticmethod
    def find_matching_station(lon: float, lat: float, organization: str,
                              csv_file: str) -> Optional[str]:
        """Find the station that matches the provided longitude, latitude, and organization"""
        if lon is None or lat is None:
            return None

        def count_decimals(number):
            """Count the number of decimal places in a float number"""
            if '.' not in str(number):
                return 0
            return len(str(number).split('.')[1])

        try:
            station_df = pd.read_csv(csv_file, index_col=False)
            matches = station_df[
                (station_df['org'] == organization) &
                (station_df.apply(
                    lambda row: round(float(row['lon']),
                                      count_decimals(row['lon'])) == round(float(lon),
                                                                    count_decimals(row['lon'])),
                    axis=1
                )) &
                (station_df.apply(
                    lambda row: round(float(row['lat']),
                            count_decimals(row['lat'])) == round(float(lat),
                                                                 count_decimals(row['lat'])),
                    axis=1
                ))
            ]

            if not matches.empty:
                return matches['station'].iloc[0]
            return None

        except (pd.errors.EmptyDataError, pd.errors.ParserError) as e:
            logger.error("CSV parsing error in finding matching station: %s", e)
            return None
        except (TypeError, ValueError) as e:
            logger.error("Type or value error in finding matching station: %s", e)
            return None

class DataProcessor:
    def __init__(self, station_coord_file: str):
        self.station_coord = load_json_data(station_coord_file)
        self.final_dict = {"Seawater": [], "Fish": [], "Seaweed": []}

    @staticmethod
    def get_lines(directory: str, data_file: str) -> List[str]:
        """Read the contents of a CSV file and return the lines as a list"""
        try:
            with open(join(directory, data_file), 'r', encoding="utf8") as f:
                return f.read().split("\n")
        except IOError as e:
            logger.error("IO error reading file %s: %s", data_file, e)
            return []
        except UnicodeDecodeError as e:
            logger.error("Encoding error reading file %s: %s", data_file, e)
            return []

    @staticmethod
    def get_id(data_file: str) -> str:
        """Extract the station ID from the filename of a CSV file"""
        try:
            return splitext(basename(data_file))[0].split("_")[-1]
        except (IndexError, AttributeError):
            return ""

    def process_seawater_data(self, lines: List[str]) -> List[Dict[str, Any]]:
        """Process seawater specific data"""
        try:
            depths = [value.strip() for value in lines[3].split(",") if value.strip()]
            nuclides_line = lines[4].split(",")
            depth_data = []

            def get_depth_index(length: int) -> int:
                return list(lines[3].split(',')).index(depths[length])

            def get_depth_columns(depth_start_index: int, depth_end_index: int) -> List[str]:
                nuclides = [value
                            for value in nuclides_line[depth_start_index:depth_end_index]
                            if value.strip()]
                return ["begperiod"] + [col
                                        for nuclide in nuclides
                                        for col in [nuclide, f"{nuclide}_nd"]]

            for i, depth in enumerate(depths):
                start_index = get_depth_index(i)
                end_index = get_depth_index(i + 1) if i < len(depths) - 1 else len(nuclides_line)
                columns_by_depth = get_depth_columns(start_index, end_index)

                full_data = pd.DataFrame([line.split(",") for line in lines[6:]])
                full_data = full_data.dropna()
                data = full_data.iloc[:, start_index:end_index]
                data.columns = columns_by_depth
                data = data.replace({"-": None, "": None})
                for column in columns_by_depth:
                    if column not in ["begperiod"]:
                        data[column] = pd.to_numeric(data[column], errors='coerce')

                filtered_data = [row for row in data.to_dict(orient="records")
                               if any(value not in [None, "-", ""] for value in row.values())]

                depth_data.append({"depth": depth, "data": filtered_data})

            return depth_data
        except (IndexError, KeyError, TypeError, ValueError):
            return []

    def process_station(self, sample_type: str, sample_type_dir: str) -> None:
        """Process the data for a particular sample type (Seawater, Fish, or Seaweed)"""
        station_matcher = StationMatcher()

        for station in self.station_coord[sample_type]:
            if not station["coordinates"]:
                continue

            parsed_lat, parsed_lon = parse_dms_coordinates(station["coordinates"])
            if parsed_lat is None or parsed_lon is None:
                continue

            org = station["org"]
            station_name = station["station"] or \
                station_matcher.find_matching_station(parsed_lon,
                                                      parsed_lat,
                                                      org, "stations/station_points.csv") or \
                station_matcher.find_matching_station(parsed_lon,
                                                      parsed_lat,
                                                      org, "stations/alps_seawater_data.csv")

            station_dict = {
                "id": station["id"],
                "org": org,
                "station": station_name,
                "lat": float(parsed_lat),
                "lon": float(parsed_lon),
            }

            if sample_type == "Seawater":
                station_dict["depth_data"] = []
            else:
                station_dict["data"] = []

            csv_files = [f for f in listdir(sample_type_dir)
                         if str(station["id"]) == self.get_id(f)]
            if not csv_files:
                continue

            for csv_file in csv_files:
                if sample_type == "Seawater":
                    lines = self.get_lines(sample_type_dir, csv_file)
                    station_dict["depth_data"] = self.process_seawater_data(lines)
                else:
                    try:
                        df = pd.read_csv(join(sample_type_dir, csv_file), skiprows=2)
                        station_dict["data"] = df.replace(
                            {np.nan: None}).to_dict(orient="records")
                    except (pd.errors.EmptyDataError, pd.errors.ParserError):
                        continue

            self.final_dict[sample_type].append(station_dict)

    def process_all_data(self) -> Dict[str, List[Dict[str, Any]]]:
        """Process all sample types and return the final dictionary"""
        sample_types = {
            "Seawater": "downloaded_CSVs/Seawater",
            "Fish": "downloaded_CSVs/Fishes",
            "Seaweed": "downloaded_CSVs/Seaweeds"
        }

        for sample_type, directory in sample_types.items():
            self.process_station(sample_type, directory)

        return self.final_dict

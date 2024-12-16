from os.path import join, splitext, basename
from os import listdir
from typing import Optional, Dict, List, Any
import numpy as np
import pandas as pd
from pkg_resources import resource_filename

from iaea.orbs import logger
from iaea.orbs.utils import generate_output_path
from iaea.orbs.utils import load_json_data
from iaea.orbs.utils import parse_dms_coordinates


STATIONS_POINTS = pkg_resources.resource_filename(
    "iaea.orbs", "stations/station_points.csv"
)
ALPES_SEAWATER_DATA = resource_filename(
    "iaea.orbs",  "stations/alps_seawater_data.csv"
)


def parse_column_value(value):
    """
    Parse a nuclide value into its float representation, handling uncertainty
    """
    if value is None or value == '':
        return None, None

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
        return float(value), None
    except (ValueError, TypeError):
        logger.error("Could not convert value: '%s'", value)
        return None, None


class DataProcessor:
    def __init__(self, station_coord_file: str, input_dir : str, output_dir: str):
        self.station_coord = load_json_data(station_coord_file)
        self.input_dir = input_dir
        self.output_dir = output_dir


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

    @staticmethod
    def get_csv_files(directory: str, station_id: int) -> List[str]:
        """Get all CSV files corresponding to a station ID"""
        return [
            f for f in listdir(directory)
            if str(station_id) == DataProcessor.get_id(f)
        ]

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
            logger.error("CSV parsing error in finding matching station for file %s: %s",
                         csv_file, e)
            return None
        except (TypeError, ValueError) as e:
            logger.error("Type or value error in finding matching station: %s", e)
            return None

    def _find_station_name(self, lon: float, lat: float, org: str) -> Optional[str]:
        """Find the station name by comparing coordinates"""
        station_points_matcher = DataProcessor.find_matching_station(
                                lon, lat, org, STATIONS_POINTS)
        alps_data_matcher = DataProcessor.find_matching_station(
                            lon, lat, org,  ALPES_SEAWATER_DATA)
        return (
            station_points_matcher or
            alps_data_matcher
        )

    def _extract_depths(self, depth_line: str) -> List[str]:
        """Extract and clean depth values"""
        return [value.strip() for value in depth_line.split(",") if value.strip()]

    def _get_depth_columns(self, nuclides_line: List[str],
                        depth_start_index: int, depth_end_index: int) -> List[str]:
        if depth_start_index >= len(nuclides_line) or depth_end_index > len(nuclides_line):
            logger.warning("Invalid depth column indices")
            return []

        nuclides = [value
                    for value in nuclides_line[depth_start_index:depth_end_index]
                    if value.strip()]
        return ["begperiod"] + [col for nuclide in nuclides for col in [nuclide, f"{nuclide}_nd"]]


    def update_dataframe(self, df : pd.DataFrame,
                         column : str) -> pd.DataFrame:
        """
        Update DataFrame with separate columns for value and uncertainty (if applicable)
        """
        if column not in df.columns:
            logger.warning("Nuclide column '%s' not found in DataFrame", column)
            return df
        parsed_data = df[column].apply(parse_column_value)
        df[column] = parsed_data.apply(lambda x: x[0]
                                        if isinstance(x, tuple) else x)
        df[f"{column}_unc"] = parsed_data.apply(lambda x: x[1]
                                                 if isinstance(x, tuple) else None)
        return df

    def update_dateframe_for_columns(self, df : pd.DataFrame,
                                      columns : List[str]) -> pd.DataFrame:
        """
        Update the DataFrame with separate value and uncertainty columns for each nuclide
        """
        for column in columns:
            df = self.update_dataframe(df, column)
        return df

    def update_seawater_dataframe(self, df : pd.DataFrame):
        columns = ["Cs-134", "Cs-134_nd", "Cs-137", "Cs-137_nd", "H-3", "H-3_nd"]
        return self.update_dateframe_for_columns(df, columns)

    def update_fish_seaweed_dataframe(self, df : pd.DataFrame):
        columns = ["Dt", "ND"]
        return self.update_dateframe_for_columns(df, columns)

    def process_sample_data(self, sample_type : str,
         sample_type_dir : str, csv_file : str, station_dict : dict):
        """
        Process the data from a CSV file for a given sample type (Seawater, Fish, or Seaweed),
        and update the station dictionary with the processed data
        """

        if sample_type == "Seawater":
            lines = self.get_lines(sample_type_dir, csv_file)
            station_dict["depth_data"] = self.process_seawater_data(lines)
        else:
            try:
                df = pd.read_csv(join(sample_type_dir, csv_file), skiprows=2)
                df = self.update_fish_seaweed_dataframe(df)
                df = df.rename(columns={"Date and time of Sampling": "begperiod"})

                station_dict["data"] = df.replace({np.nan: None}).to_dict(orient="records")
            except (pd.errors.EmptyDataError, pd.errors.ParserError):
                return None
        return station_dict

    def process_seawater_data(self, lines: List[str]) -> List[Dict[str, Any]]:
        try:
            depths = self._extract_depths(lines[3])
            nuclides_line = lines[4].split(",")
            depth_data = []

            # Use list comprehension for more concise code
            depth_data = [
                {
                    "depth": depth,
                    "data": self._process_depth_data(lines, depths, nuclides_line, depth)
                }
                for depth in depths
            ]

            return depth_data
        except Exception as e:
            logger.error("Error processing seawater data: %s", e)
            return []

    def _process_depth_data(self, lines, depths, nuclides_line, current_depth):

        start_index = list(lines[3].split(',')).index(current_depth)
        end_index = list(lines[3].split(',')).index(depths[depths.index(current_depth) + 1]) \
            if depths.index(current_depth) < len(depths) - 1 else len(nuclides_line)

        columns_by_depth = self._get_depth_columns(nuclides_line, start_index, end_index)

        data = pd.DataFrame([line.split(",") for line in lines[6:]])
        data = data.iloc[:, start_index:end_index]
        data.columns = columns_by_depth
        data = data.replace({"-": None, "": None}).dropna(subset=['begperiod'])

        df = self.update_seawater_dataframe(data)
        df = df.dropna(axis=1, how='all')

        return df.to_dict(orient="records")

    def process_station(self, sample_type: str, sample_type_dir: str) -> None:
        """Process the data for a particular sample type (Seawater, Fish, or Seaweed)
        """
        output_file = generate_output_path(self.output_dir, sample_type, "json")
        all_data = []

        for station in self.station_coord[sample_type]:
            if not station["coordinates"]:
                continue

            parsed_lat, parsed_lon = parse_dms_coordinates(station["coordinates"])
            if parsed_lat is None or parsed_lon is None:
                continue

            org = station["org"]
            station_name = station["station"] or \
                self._find_station_name(float(parsed_lon), float(parsed_lat), org)

            station_dict = {
                "id": int(station["id"]),
                "org": org,
                "station": station_name,
                "lat": float(parsed_lat),
                "lon": float(parsed_lon),
            }

            csv_files = DataProcessor.get_csv_files(sample_type_dir, station["id"])
            if not csv_files:
                continue

            for csv_file in csv_files:
                self.process_sample_data(sample_type, sample_type_dir, csv_file, station_dict)

            all_data.append(station_dict)

        pd.DataFrame(all_data).to_json(output_file, orient='records', indent=2)

    def process_all_data(self):
        """
        Process all sample types and write results to separate
        """
        sample_types = {
            "Seawater": join(self.input_dir, "Seawater"),
            "Fish": join(self.input_dir, "Fishes"),
            "Seaweed": join(self.input_dir, "Seaweeds"),
        }

        for sample_type, directory in sample_types.items():
            self.process_station(sample_type, directory)

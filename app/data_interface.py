import os
import json
import mmap
import pathlib
import itertools
from datetime import datetime
from multiprocessing import Pool

import pandas as pd
from bs4 import BeautifulSoup, SoupStrainer
from icalendar import Calendar
from dateutil import parser
from babel.dates import get_month_names, get_day_names
import locale

import duckdb
import csv

def create_connection(db_file, read_only=False):
    """
    Create and return a DuckDB connection, optionally in read-only mode.
    """
    return duckdb.connect(database=db_file, read_only=read_only)

def query_data(db_file, query):
    """
    Open a temporary connection, execute the query, then close the connection.
    """
    conn = create_connection(db_file, read_only=True)
    df = conn.execute(query).df()
    conn.close()
    return df

def setup_database(db_file=os.path.join('data', 'my_duckdb.duckdb'), reset=False):
    """
    Create or reset the DuckDB file.
    """
    if reset:
        if os.path.exists(db_file):
            os.remove(db_file)
    # Return the file path rather than a connection
    # so we can create fresh connections on demand
    return db_file

def create_raw_view(db_file, file_path, table_name):
    conn = create_connection(db_file, read_only=False)
    view_name = f"raw_{table_name}"
    try:
        if file_path.endswith('.csv'):
            conn.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_csv_auto('{file_path}')")
        elif file_path.endswith('.json'):
            conn.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_json_auto('{file_path}')")
        elif file_path.endswith('.xml'):
            conn.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_xml_auto('{file_path}')")
        else:
            raise ValueError(f"Unsupported file type for {file_path}")
    finally:
        conn.close()

def create_table_from_mapping(db_file, mapping_path):
    conn = create_connection(db_file, read_only=False)
    try:
        with open(mapping_path, 'r') as file:
            script = file.read().strip()
            if not script:
                raise ValueError("Mapping file is empty or only contains whitespace.")
        conn.execute(script)
    finally:
        conn.close()

class GoogleTakeoutDataInterface:
    def __init__(
        self,
        takeout_path="/home/ivan/Desktop/datasets/other_takeouts/Takeout",
        data_output_folder="data",
        reset_db=True
    ):
        self.TAKEOUT_PATH = takeout_path
        self.DATA_OUTPUT_FOLDER = data_output_folder
        self.ACTIVITY_LOG_PATH = os.path.join(self.TAKEOUT_PATH, "Mi actividad")
        self.ACTIVITY_PLACEHOLDER_NAME = "MiActividad.html"
        self.FOLDER_DRIVE_ACTIVITY_LOG_PATH = os.path.join(self.ACTIVITY_LOG_PATH, "Drive")
        self.FILE_DRIVE_ACTIVITY_LOG_PATH = os.path.join(self.FOLDER_DRIVE_ACTIVITY_LOG_PATH, self.ACTIVITY_PLACEHOLDER_NAME)
        self.FOLDER_TAKEOUT_ACTIVITY_LOG_PATH = os.path.join(self.ACTIVITY_LOG_PATH, "Takeout")
        self.FILE_TAKEOUT_ACTIVITY_LOG_PATH = os.path.join(self.FOLDER_TAKEOUT_ACTIVITY_LOG_PATH, self.ACTIVITY_PLACEHOLDER_NAME)
        self.FOLDER_YOUTUBE_ACTIVITY_LOG_PATH = os.path.join(self.ACTIVITY_LOG_PATH, "YouTube")
        self.FILE_YOUTUBE_ACTIVITY_LOG_PATH = os.path.join(self.TAKEOUT_PATH, "Mi actividad")
        self.FILE_PROFILE_JSON_PATH = os.path.join(self.TAKEOUT_PATH, "Perfil", "Perfil.json")
        self.FOLDER_YOUTUBE_SUBSCRIPTIONS_PATH = os.path.join(self.TAKEOUT_PATH, "YouTube y YouTube Music", "suscripciones")
        self.FILE_SUBSCRIBED_CHANNELS_CSV_PATH = os.path.join(self.FOLDER_YOUTUBE_SUBSCRIPTIONS_PATH, "suscripciones.csv")
        self.FOLDER_YOUTUBE_VIDEO_METADATA_PATH = os.path.join(self.TAKEOUT_PATH, "YouTube y YouTube Music", "metadatos del vídeo")
        self.FILE_PUBLISHED_VIDEOS_METADATA_CSV_PATH = os.path.join(self.FOLDER_YOUTUBE_VIDEO_METADATA_PATH, "vídeos.csv")
        self.FOLDER_YOUTUBE_PLAYLISTS_PATH = os.path.join(self.TAKEOUT_PATH, "YouTube y YouTube Music", "listas de reproducción")
        self.FILE_PLAYLISTS_CSV_PATH = os.path.join(self.FOLDER_YOUTUBE_PLAYLISTS_PATH, "Listas de reproducción.csv")
        self.CALENDAR_PATH = os.path.join(self.TAKEOUT_PATH, "Calendar")
        self.FOLDER_ALL_ACCESSES = os.path.join(self.TAKEOUT_PATH, "Actividad de registro de accesos")
        self.FILE_ALL_ACCESSES = os.path.join(self.FOLDER_ALL_ACCESSES, "Actividades_ una lista con los servicios de Google.csv")
        self.OUTPUT_CSV_PATH = os.path.join(self.DATA_OUTPUT_FOLDER, 'output.csv')

        self.db_file = setup_database(reset=reset_db)

        self.run_mapping(os.path.join('config','mapping.json'))

    def run_mapping(self, config_path, language_code='es'):
        try:
            paths = self.load_config(config_path, language_code)
            for key, cfg in paths.items():
                if cfg['enabled']:
                    create_raw_view(self.db_file, cfg['file_path'], key)
                    create_table_from_mapping(self.db_file, cfg['mapping_path'])
                    print(f"FINISHED processing {key} from {cfg['file_path']} using {cfg['mapping_path']}")
                else:
                    print(f"IGNORED {key} from {cfg['file_path']}")
        except Exception as e:
            print(f"Error while processing files: {e}")

    def load_config(self, config_path, language_code='es'):
        with open(config_path, 'r') as file:
            config = json.load(file)
        base_path = self.TAKEOUT_PATH
        data_mapping = {}
        for item in config['data_files']:
            file_path = item['files'][language_code].replace("{base_path}", base_path).replace("{transformations_path}", self.DATA_OUTPUT_FOLDER)
            mapping_path = item['mapping_file'].replace("{base_path}", base_path)
            data_mapping[item['id']] = {
                'file_path': file_path,
                'mapping_path': mapping_path,
                'enabled': item.get('enabled', True)
            }
        return data_mapping

    def query_data(self, query):
        return query_data(self.db_file, query)

    def example_workflow(self):
        query = "SELECT * FROM some_table LIMIT 5"
        result = self.query_data(query)
        print(result)
        return result


if __name__ == '__main__':
    data_interface = GoogleTakeoutDataInterface(
        takeout_path=os.environ.get('TAKEOUT_PATH', '/home/ivan/Desktop/datasets/Takeout'),
        data_output_folder='data',
        reset_db=True
    )
    data_interface.example_workflow()

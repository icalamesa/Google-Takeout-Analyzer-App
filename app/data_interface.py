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
import app.data_preprocessor as dp


class DuckDBInterface:
    """Handles DuckDB database connections and operations."""

    @staticmethod
    def create_connection(db_file, read_only=False):
        """Create and return a DuckDB connection."""
        return duckdb.connect(database=db_file, read_only=read_only)

    @staticmethod
    def query_data(db_file, query):
        """Execute a query and return the result as a DataFrame."""
        conn = DuckDBInterface.create_connection(db_file, read_only=True)
        df = conn.execute(query).df()
        conn.close()
        return df

    @staticmethod
    def setup_database(db_file, reset=False):
        """Set up the DuckDB file. Optionally reset the database."""
        if reset and os.path.exists(db_file):
            os.remove(db_file)
        return db_file

    @staticmethod
    def create_raw_view(db_file, file_path, table_name):
        """Create or replace a raw view in the database."""
        conn = DuckDBInterface.create_connection(db_file, read_only=False)
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

    @staticmethod
    def create_table_from_mapping(db_file, mapping_path):
        """Execute a SQL script from a mapping file."""
        conn = DuckDBInterface.create_connection(db_file, read_only=False)
        try:
            with open(mapping_path, 'r') as file:
                script = file.read().strip()
                if not script:
                    raise ValueError("Mapping file is empty or only contains whitespace.")
            conn.execute(script)
        finally:
            conn.close()


class GoogleTakeoutProcessor:
    """
    Main interface for processing Google Takeout data and managing 
    the DuckDB-based pipeline. Integrates with DataPreprocessor for 
    parsing and preparing data prior to DB ingestion.
    """

    def __init__(self, takeout_path, data_output_folder, reset_db=True):
        self.takeout_path = takeout_path
        self.data_output_folder = data_output_folder
        self.db_file = DuckDBInterface.setup_database(
            os.path.join(data_output_folder, 'my_duckdb.duckdb'), 
            reset=reset_db
        )

        self.paths = {
            "activity_root": os.path.join(self.takeout_path, "Mi actividad"),
            "profile_json": os.path.join(self.takeout_path, "Perfil", "Perfil.json"),
            "subscriptions_csv": os.path.join(
                self.takeout_path, "YouTube y YouTube Music", "suscripciones", "suscripciones.csv"
            ),
            "video_metadata_csv": os.path.join(
                self.takeout_path, "YouTube y YouTube Music", "metadatos del vídeo", "vídeos.csv"
            ),
            "playlists_csv": os.path.join(
                self.takeout_path, "YouTube y YouTube Music", "listas de reproducción", "Listas de reproducción.csv"
            ),
            "calendar": os.path.join(self.takeout_path, "Calendar"),
            "access_logs_csv": os.path.join(
                self.takeout_path, "Actividad de registro de accesos", 
                "Actividades_ una lista con los servicios de Google.csv"
            ),
        }

        self.activity_logs = [
            os.path.join(self.paths["activity_root"], "Drive", "MiActividad.html"),
            os.path.join(self.paths["activity_root"], "Takeout", "MiActividad.html"),
            os.path.join(self.paths["activity_root"], "YouTube", "MiActividad.html"),
        ]

        self.data_preprocessor = dp.DataPreprocessor(
            html_chunk_factor=4,
            max_threads=8,
            calendar_path=self.paths["calendar"],
            profile_path=self.paths["profile_json"],
            activity_log_paths=self.activity_logs
        )
        for filename, content in self.data_preprocessor.load_all_datasets().items():
            content.to_csv(os.path.join(data_output_folder, filename+'.csv',), index=False)
        
        self.run_mapping(os.path.join('config', 'mapping.json'))

    def run_mapping(self, config_path, language_code='es'):
        """
        Load and apply SQL mappings to create or update 
        tables/views in the DuckDB database.
        """
        try:
            paths = self.load_config(config_path, language_code)
            for key, cfg in paths.items():
                if cfg['enabled']:
                    DuckDBInterface.create_raw_view(self.db_file, cfg['file_path'], key)
                    DuckDBInterface.create_table_from_mapping(self.db_file, cfg['mapping_path'])
                    print(f"FINISHED processing {key} from {cfg['file_path']} using {cfg['mapping_path']}")
                else:
                    print(f"IGNORED {key} from {cfg['file_path']}")
        except Exception as e:
            print(f"Error while processing files: {e}")

    def load_config(self, config_path, language_code='es'):
        """Load mapping configurations for data ingestion."""
        with open(config_path, 'r') as file:
            config = json.load(file)
        data_mapping = {}
        for item in config['data_files']:
            file_path = item['files'][language_code]\
                .replace("{base_path}", self.takeout_path)\
                .replace("{transformations_path}", self.data_output_folder)
            mapping_path = item['mapping_file'].replace("{base_path}", self.takeout_path)
            data_mapping[item['id']] = {
                'file_path': file_path,
                'mapping_path': mapping_path,
                'enabled': item.get('enabled', True)
            }
        return data_mapping

    def query_data(self, query):
        """Run a SQL query on the database."""
        return DuckDBInterface.query_data(self.db_file, query)

    def preprocess_data(self):
        """
        Leverage the DataPreprocessor to load raw data from
        profile, calendar, subscriptions, etc. 
        Returns multiple DataFrames for further ingestion or exploration.
        """
        self.data_preprocessor.load_all()

if __name__ == '__main__':
    processor = GoogleTakeoutProcessor(
        takeout_path=os.environ.get('TAKEOUT_PATH', '/home/ivan/Desktop/datasets/other_takeouts/Takeout'),
        data_output_folder='data',
        reset_db=True
    )
    query = "SELECT * FROM clean_profiles LIMIT 5"
    result = processor.query_data(query)
    print("Query result:\n", result)

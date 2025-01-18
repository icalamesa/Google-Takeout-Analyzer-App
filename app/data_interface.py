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

# Your database module
import database as db

class GoogleTakeoutDataInterface:
    """
    This class encapsulates all the logic for reading and processing data from
    Google Takeout directories, including HTML activity logs, ICS calendar files,
    CSV-based subscriptions and more. It also integrates with a database layer (DuckDB
    or your RDB of choice) to store and query data.
    """

    def __init__(self, 
                 takeout_path="/home/ivan/Desktop/datasets/other_takeouts/Takeout",
                 data_output_folder="data",
                 reset_db=True):
        """
        Constructor for the GoogleTakeoutDataInterface class.
        
        :param takeout_path: Base path to the Google Takeout data.
        :param data_output_folder: Folder where processed data (like CSVs) will be stored.
        :param reset_db: Whether to reset the database when initializing.
        """
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

        self.conn = db.setup_database(reset=reset_db)
        self.run_mapping(os.path.join('config','mapping.json'))


    def get_optimal_chunk_size(self, factor=4):
        """
        Calculate chunk size as a multiple of the memory page size.
        """
        return mmap.PAGESIZE * factor

    def get_html_contents(self, file_path, chunk_size=524288):
        """
        Generator function that reads an HTML file in chunks to avoid loading the entire file into memory.
        
        :param file_path: Path to the HTML file.
        :param chunk_size: Size of each chunk in bytes. Default is 512 KB.
        
        :yield: A chunk of HTML that ends on a complete tag.
        """
        with open(file_path, 'r', encoding='utf-8') as file:
            buffer = ''
            while True:
                data = file.read(chunk_size)
                if not data:
                    if buffer:
                        yield buffer
                    break

                buffer += data
                last_tag_end = max(buffer.rfind('>'), buffer.rfind('/>'))
                if last_tag_end == -1:
                    continue  
                yield buffer[:last_tag_end + 1]
                buffer = buffer[last_tag_end + 1:]

    def translate_date(self, date_str):
        """
        Translate short Spanish month names to English so datetime can parse them.
        """
        spanish_months = {
            'ene': 'Jan', 'feb': 'Feb', 'mar': 'Mar', 'abr': 'Apr',
            'may': 'May', 'jun': 'Jun', 'jul': 'Jul', 'ago': 'Aug',
            'sept': 'Sep', 'oct': 'Oct', 'nov': 'Nov', 'dic': 'Dec'
        }
        
        for sp, en in spanish_months.items():
            date_str = date_str.replace(sp, en)
        return date_str

    def parse_date(self, date_str, lang='es'):
        """
        Attempt to parse the date string, currently only supporting Spanish -> English conversion.
        
        :param date_str: The date string, e.g. '21 abr 2022, 12:32:10 cet'
        :param lang: The language code, default 'es'.
        
        :return: The parsed date in 'YYYY-mm-dd HH:MM:SS' format, or '-1' on failure.
        """
        try:
            if lang == 'es':
                date_str = self.translate_date(date_str)

            dt = datetime.strptime(date_str, '%d %b %Y, %H:%M:%S cet')
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            return '-1'

    def process_chunk(self, html_content):
        """
        Processes a chunk of HTML to extract relevant data from specified div elements, including a timestamp.
        
        :param html_content: A string of HTML content.
        :return: A list of dict with extracted data from each content cell in the HTML chunk.
        """
        strainer = SoupStrainer('div', class_="outer-cell mdl-cell mdl-cell--12-col mdl-shadow--2dp")
        soup = BeautifulSoup(html_content, 'html.parser', parse_only=strainer)
        entries = []
        
        for outer_div in soup.find_all('div', recursive=False):
            platform = None
            title_cell = outer_div.find_all('p', class_="mdl-typography--title")
            for elem in title_cell:
                platform = elem.text.strip()

            content_cells = outer_div.find_all('div', class_="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1")
            for div in content_cells:
                links = div.find_all('a', href=True, recursive=False)
                stripped_strings = list(div.stripped_strings)
                link_action_text = links[0].get_text(strip=True) if len(links) > 0 else ''
                
                if stripped_strings:
                    action_parts = stripped_strings[0].split('|')
                    action_code = action_parts[0].strip()
                    timestamp = stripped_strings[-1].strip()
                else:
                    action_code = ''
                    timestamp = ''

                entry = {
                    "platform": platform,
                    "action_code": action_code,
                    "timestamp": self.parse_date(str(timestamp)),
                    "link_action_name": links[0]['href'] if len(links) > 0 else '',
                    "link_action_text": link_action_text,
                    "channel_link": links[1]['href'] if len(links) > 1 else '',
                    "channel_name": links[1].get_text(strip=True) if len(links) > 1 else '',
                    "link3": links[2]['href'] if len(links) > 2 else '',
                    "link3_text": links[2].get_text(strip=True) if len(links) > 2 else '',
                    "all": stripped_strings
                }
                entries.append(entry)
        
        return entries

    def read_html(self, file_path, max_threads=8):
        """
        Processes an HTML file using multiprocessing, with dynamic chunk sizing and optimized load balancing.
        
        :param file_path: Path to the HTML file.
        :param max_threads: Maximum number of threads (processes) for parallel processing.
        :return: Pandas DataFrame with the combined extracted data.
        """
        file_size = os.path.getsize(file_path)
        chunk_size = self.get_optimal_chunk_size(factor=2)
        num_chunks = (file_size // chunk_size) + 1
        num_processes = min(num_chunks, max_threads)
        
        chunks = self.get_html_contents(file_path, chunk_size=chunk_size)
        with Pool(processes=num_processes) as pool:
            result_iter = pool.imap_unordered(self.process_chunk, chunks)
            results = list(itertools.chain.from_iterable(result_iter))
        
        df = pd.DataFrame(results)
        return df

    def parse_profile_file(self, file_path):
        """
        Parse the user's profile JSON file.
        
        :param file_path: Path to the JSON file.
        :return: Dictionary containing person info.
        """
        with open(file_path, 'r') as file:
            data = json.load(file)

        person_info = {
            "givenName": data['name']['givenName'],
            "formattedName": data['name']['formattedName'],
            "displayName": data['displayName'],
            "email": data['emails'][0]['value'],
            "genderType": data['gender']['type']
        }
        return person_info

    def parse_ics(self, file_path):
        """
        Parse an iCalendar (ICS) file for events.
        
        :param file_path: Path to the ICS file.
        :return: A list of dictionaries for each event in the calendar.
        """
        with open(file_path, 'rb') as file:
            cal = Calendar.from_ical(file.read())
        calendar_name = pathlib.Path(file_path).stem
        
        meetings = []
        for component in cal.walk():
            if component.name == "VEVENT":
                summary = component.get('summary')
                start = component.get('dtstart').dt
                end = component.get('dtend').dt
                organizer = component.get('organizer')
                if organizer:
                    organizer = str(organizer).replace('mailto:', '')

                meeting_info = {
                    'Calendar': calendar_name,
                    'Title': summary,
                    'Start': start,
                    'End': end,
                    'Duration': end - start if end and start else None,
                    'Organizer': organizer
                }
                meetings.append(meeting_info)

        df = pd.DataFrame(meetings)
        df.to_csv(os.path.join(self.DATA_OUTPUT_FOLDER, 'all_calendars.csv'), 
                  sep='\t', index=False)
        return meetings

    def load_all(self):
        """
        Example high-level function that shows how various data pieces can be loaded.
        """
        # 1) PROFILE DATA
        profile_data = self.parse_profile_file(self.FILE_PROFILE_JSON_PATH)
        person_info_df = pd.DataFrame([profile_data])

        # 2) YOUTUBE AND YOUTUBE MUSIC
        subscribed_channels_df = pd.read_csv(self.FILE_SUBSCRIBED_CHANNELS_CSV_PATH)
        published_videos_metadata_df = pd.read_csv(self.FILE_PUBLISHED_VIDEOS_METADATA_CSV_PATH)
        playlists_df = pd.read_csv(self.FILE_PLAYLISTS_CSV_PATH)

        # 3) CALENDAR
        calendar_events_list = []
        for entry in os.scandir(self.CALENDAR_PATH):
            if entry.is_file() and entry.name.lower().endswith('.ics'):
                calendar_events_list.extend(self.parse_ics(entry))

        calendar_events_df = pd.DataFrame(calendar_events_list)
        calendar_events_df.to_csv(os.path.join(self.DATA_OUTPUT_FOLDER, 'all_calendars.csv'), index=False, sep='\t')

        # 4) ACTIVITY LOGS (example usage - if you'd like to parse them)
        # activity_logs_df = pd.concat(
        #     [
        #         self.read_html(self.FILE_YOUTUBE_ACTIVITY_LOG_PATH, max_threads=8),
        #         self.read_html(self.FILE_DRIVE_ACTIVITY_LOG_PATH, max_threads=8),
        #         self.read_html(self.FILE_TAKEOUT_ACTIVITY_LOG_PATH, max_threads=8)
        #     ], 
        #     ignore_index=True
        # )
        # activity_logs_df.to_csv(os.path.join(self.DATA_OUTPUT_FOLDER, 'output.csv'), index=False, sep='\t')
        
        return {
            "profile_info": person_info_df,
            "youtube_subscriptions": subscribed_channels_df,
            "youtube_published_videos": published_videos_metadata_df,
            "youtube_playlists": playlists_df,
            "calendar_events": calendar_events_df
        }

    def load_config(self, config_path, language_code='es'):
        """
        Loads and returns configuration mappings from a JSON file based on a specified language.
        
        :param config_path: Path to the configuration JSON file.
        :param language_code: Language code to load specific file paths (default: 'es' for Spanish).
        :return: A dictionary with configuration details for each data file.
        """
        try:
            with open(config_path, 'r') as file:
                config = json.load(file)

            base_path = self.TAKEOUT_PATH
            data_mapping = {
                item['id']: {
                    'file_path': item['files'][language_code].replace("{base_path}", base_path)
                                                           .replace("{transformations_path}", self.DATA_OUTPUT_FOLDER),
                    'mapping_path': item['mapping_file'].replace("{base_path}", base_path),
                    'enabled': item.get('enabled', True)
                }
                for item in config['data_files']
            }
            return data_mapping
        except KeyError as e:
            raise KeyError(f"Missing required configuration item: {e}")
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found at {config_path}")
        except json.JSONDecodeError:
            raise ValueError("Configuration file is not valid JSON")

    def run_mapping(self, config_path, language_code='es'):
        """
        Processes files based on configurations loaded from a JSON file.
        
        :param config_path: Path to the configuration JSON file.
        :param language_code: Language code to process specific files (default: 'es' for Spanish).
        """
        try:
            paths = self.load_config(config_path, language_code)
            for key, config in paths.items():
                if config['enabled']:
                    db.create_raw_view(self.conn, config['file_path'], key)
                    db.create_table_from_mapping(self.conn, config['mapping_path'])
                    print(f"FINISHED processing {key} from {config['file_path']} using {config['mapping_path']}")
                else:
                    print(f"IGNORED {key} from {config['file_path']}")
        except Exception as e:
            print(f"An error occurred while processing files: {e}")

    def query_data(self, query):
        """
        Simple interface to pass a query to the underlying DB.
        """
        return db.query_data(self.conn, query=query)

    def example_workflow(self):
        """
        Optional method demonstrating a typical usage workflow:
          1) Load all major data (profile, YT, calendars...)
          2) Run a mapping from a config
          3) Perform a test query
        """
        loaded_data = self.load_all()
        self.run_mapping(os.path.join('config','mapping.json'))

        with open(os.path.join('mappings', 'test_query.sql'), 'r') as file:
            query = file.read()
            query_result = self.query_data(query=query)
            print(query_result)
        
        return query_result

data_interface = GoogleTakeoutDataInterface(
    takeout_path=os.environ.get('TAKEOUT_PATH', '/home/ivan/Desktop/datasets/Takeout'),
    data_output_folder='data',
    reset_db=True
)

data_interface.example_workflow()
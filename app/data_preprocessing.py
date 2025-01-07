import pandas as pd
import json
import os
from multiprocessing import Pool
from bs4 import BeautifulSoup, SoupStrainer
import itertools
import mmap
from icalendar import Calendar, Event
import pathlib
from datetime import datetime
from dateutil import parser
from babel.dates import get_month_names, get_day_names
import locale

import database as db

# Base paths
DATA_OUTPUT_FOLDER = "data"
TAKEOUT_PATH = "/home/ivan/Desktop/datasets/Takeout"  # Change if necessary

# Activity log paths
ACTIVITY_LOG_PATH = os.path.join(TAKEOUT_PATH, "Mi actividad")
ACTIVITY_PLACEHOLDER_NAME = "MiActividad.html"

# Drive activity log
FOLDER_DRIVE_ACTIVITY_LOG_PATH = os.path.join(ACTIVITY_LOG_PATH, "Drive")
FILE_DRIVE_ACTIVITY_LOG_PATH = os.path.join(FOLDER_DRIVE_ACTIVITY_LOG_PATH, ACTIVITY_PLACEHOLDER_NAME)

# Takeout activity log
FOLDER_TAKEOUT_ACTIVITY_LOG_PATH = os.path.join(ACTIVITY_LOG_PATH, "Takeout")
FILE_TAKEOUT_ACTIVITY_LOG_PATH = os.path.join(FOLDER_TAKEOUT_ACTIVITY_LOG_PATH, ACTIVITY_PLACEHOLDER_NAME)

# YouTube activity log
FOLDER_YOUTUBE_ACTIVITY_LOG_PATH = os.path.join(ACTIVITY_LOG_PATH, "YouTube")
FILE_YOUTUBE_ACTIVITY_LOG_PATH = os.path.join(FOLDER_YOUTUBE_ACTIVITY_LOG_PATH, ACTIVITY_PLACEHOLDER_NAME)

# Profile data
FILE_PROFILE_JSON_PATH = os.path.join(TAKEOUT_PATH, "Perfil", "Perfil.json")

# YouTube and YouTube Music data
FOLDER_YOUTUBE_SUBSCRIPTIONS_PATH = os.path.join(TAKEOUT_PATH, "YouTube y YouTube Music", "suscripciones")
FILE_SUBSCRIBED_CHANNELS_CSV_PATH = os.path.join(FOLDER_YOUTUBE_SUBSCRIPTIONS_PATH, "suscripciones.csv")

FOLDER_YOUTUBE_VIDEO_METADATA_PATH = os.path.join(TAKEOUT_PATH, "YouTube y YouTube Music", "metadatos del vídeo")
FILE_PUBLISHED_VIDEOS_METADATA_CSV_PATH = os.path.join(FOLDER_YOUTUBE_VIDEO_METADATA_PATH, "vídeos.csv")

FOLDER_YOUTUBE_PLAYLISTS_PATH = os.path.join(TAKEOUT_PATH, "YouTube y YouTube Music", "listas de reproducción")
FILE_PLAYLISTS_CSV_PATH = os.path.join(FOLDER_YOUTUBE_PLAYLISTS_PATH, "Listas de reproducción.csv")

# Calendar data
CALENDAR_PATH = os.path.join(TAKEOUT_PATH, "Calendar")

# Output file path for activity logs
OUTPUT_CSV_PATH = os.path.join(DATA_OUTPUT_FOLDER, 'output.csv')


def get_optimal_chunk_size(factor=4):
    """Calculate chunk size as a multiple of the memory page size."""
    page_size = mmap.PAGESIZE  # Gets the memory page size
    return page_size * factor


def get_html_contents(file_path, chunk_size=524288):
    """
    Generator function that reads an HTML file in chunks to avoid loading the entire file into memory.
    
    Args:
    - file_path (str): Path to the HTML file.
    - chunk_size (int): Size of each chunk in bytes. Default is 512 KB.
    
    Yields:
    - str: A chunk of HTML that ends on a complete tag.
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        buffer = ''
        while True:
            data = file.read(chunk_size)
            if not data:
                # Only yield the remaining buffer if it's a valid chunk with a closed tag
                if buffer:
                    yield buffer
                break

            buffer += data
            last_tag_end = max(buffer.rfind('>'), buffer.rfind('/>'))
            if last_tag_end == -1:
                continue  # Continue reading into buffer until a tag end is found

            # Yield up to the last complete tag and adjust the buffer
            yield buffer[:last_tag_end + 1]
            buffer = buffer[last_tag_end + 1:]

def translate_date(date_str):
    # curently only supporting spanish
    spanish_months = {
        'ene': 'Jan', 'feb': 'Feb', 'mar': 'Mar', 'abr': 'Apr',
        'may': 'May', 'jun': 'Jun', 'jul': 'Jul', 'ago': 'Aug',
        'oct': 'Oct', 'nov': 'Nov', 'dic': 'Dec', 'sept':'Sep'
    }
    
    for sp, en in spanish_months.items():
        date_str = date_str.replace(sp, en)
    return date_str

def parse_date(date_str, lang='es'):
    try:
        if lang == 'es':
            date_str = translate_date(date_str)

        # Parse the date using the standard datetime library
        dt = datetime.strptime(date_str, '%d %b %Y, %H:%M:%S cet')
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except ValueError as e:
        #print(f"Error parsing date: {e}")
        return '-1'

def process_chunk(html_content):
    """
    Processes a chunk of HTML to extract relevant data from specified div elements, including a timestamp.
    
    Args:
    - html_content (str): A string of HTML content.
    
    Returns:
    - list of dict: Extracted data from each content cell in the HTML chunk.
    """
    strainer = SoupStrainer('div', class_="outer-cell mdl-cell mdl-cell--12-col mdl-shadow--2dp")
    soup = BeautifulSoup(html_content, 'html.parser', parse_only=strainer)
    entries = []
    count = 0
    
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
                "timestamp": parse_date(str(timestamp)),
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


def read_html(file_path, max_threads=8):
    """
    Processes an HTML file using multiprocessing, with dynamic chunk sizing and optimized load balancing.
    """
    file_size = os.path.getsize(file_path)
    chunk_size = get_optimal_chunk_size(factor=2)  # Dynamically determine the chunk size
    num_chunks = (file_size // chunk_size) + 1
    
    # Adjust number of processes based on the actual workload
    num_processes = min(num_chunks, max_threads)
    
    chunks = get_html_contents(file_path, chunk_size=chunk_size)
    with Pool(processes=num_processes) as pool:
        result_iter = pool.imap_unordered(process_chunk, chunks)  # Use imap_unordered for better load balancing
        results = list(itertools.chain.from_iterable(result_iter))
    
    df = pd.DataFrame(results)
    return df

def parse_profile_file(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    person_info = {
        "givenName": data['name']['givenName'],
        "formattedName": data['name']['formattedName'],
        "displayName": data['displayName'],
        "email": data['emails'][0]['value'],  # Assume there is at least one email
        "genderType": data['gender']['type']
    }

    return person_info

def parse_ics(file_path):
    with open(file_path, 'rb') as file:
        cal = Calendar.from_ical(file.read())
    calendar_name = pathlib.Path(file_path).stem
    meetings = []
    for component in cal.walk():
        if component.name == "VEVENT":
            summary = component.get('summary')
            start = component.get('dtstart').dt
            end = component.get('dtend').dt
            organizer = component.get('organizer').replace('mailto:', '') if component.get('organizer') != None else None
            #organizer_email = organizer.params['CN'] if organizer and 'CN' in organizer.params else "No organizer"

            meeting_info = {
                'Calendar': calendar_name,
                'Title': summary,
                'Start': start,
                'End': end,
                'Duration': end - start,
                'Organizer': organizer
            }
            meetings.append(meeting_info)
    pd.DataFrame(meetings).to_csv(os.path.join('data', 'all_calendars.csv'), sep='\t')
    return meetings

def load_all():
    # PROFILE DATA
    person_info = pd.DataFrame([parse_profile_file("/home/ivan/Desktop/datasets/Takeout/Perfil/Perfil.json")])
    print(person_info)

    # YOUTUBE AND YOUTUBE MUSIC
    subscribed_channels_df = pd.read_csv("/home/ivan/Desktop/datasets/Takeout/YouTube y YouTube Music/suscripciones/suscripciones.csv")
    published_videos_metadata_df = pd.read_csv("/home/ivan/Desktop/datasets/Takeout/YouTube y YouTube Music/metadatos del vídeo/vídeos.csv")
    list_df = pd.read_csv("/home/ivan/Desktop/datasets/Takeout/YouTube y YouTube Music/listas de reproducción/Listas de reproducción.csv")

    # CALENDAR
    calendar_events = []
    calendar_path = '/home/ivan/Desktop/datasets/Takeout/Calendar'
    for entry in os.scandir(calendar_path):
        calendar_events.append(pd.DataFrame(parse_ics(entry)))
    
    calendar_events = pd.concat(calendar_events)
    print(calendar_events)

    # ACTIVITY LOGS
    activity_logs_df = pd.concat(
        [
            read_html(FILE_YOUTUBE_ACTIVITY_LOG_PATH, max_threads=8), 
            read_html(FILE_DRIVE_ACTIVITY_LOG_PATH, max_threads=8), 
            read_html(FILE_TAKEOUT_ACTIVITY_LOG_PATH, max_threads=8)
        ], 
        ignore_index=True)
    activity_logs_df.to_csv(os.path.join('data', 'output.csv'), index=False, sep='\t')


############# ENTITY MAPPER FUNCTIONS
import json

def load_config(config_path, language_code='es'):
    """
    Loads and returns configuration mappings from a JSON file based on a specified language.
    
    Args:
    - config_path (str): Path to the configuration JSON file.
    - language_code (str): Language code to load specific file paths (default: 'es' for Spanish).
    
    Returns:
    - dict: A dictionary with configuration details for each data file.
    """
    try:
        with open(config_path, 'r') as file:
            config = json.load(file)

        base_path = TAKEOUT_PATH
        
        a = {
            item['id']: {
                'file_path': item['files'][language_code].replace("{base_path}", base_path).replace("{transformations_path}", 'data'),
                'mapping_path': item['mapping_file'].replace("{base_path}", base_path),
                'enabled': item['enabled']  # Default to enabled if not specified
            }
            for item in config['data_files']
        }
        print(a)
        return a
    except KeyError as e:
        raise KeyError(f"Missing required configuration item: {e}")
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found at {config_path}")
    except json.JSONDecodeError:
        raise ValueError("Configuration file is not valid JSON")

def run_mapping(conn, config_path, language_code='es'):
    """
    Processes files based on configurations loaded from a JSON file.
    
    Args:
    - config_path (str): Path to the configuration JSON file.
    - language_code (str): Language code to process specific files (default: 'es' for Spanish).
    """
    try:
        paths = load_config(config_path, language_code)
        for key, config in paths.items():
            if config['enabled'] == True:
                db.create_raw_view(conn, config['file_path'], key)
                db.create_table_from_mapping(conn, config['mapping_path'])
                print(f"FINISHED processing {key} from {config['file_path']} using {config['mapping_path']}")
            else:
                print(f"IGNORED {key} from {config['file_path']} using {config['mapping_file']}")
    except Exception as e:
        print(f"An error occurred while processing files: {e}")

#############

def __main__():

    conn = db.setup_database(reset=True)
    load_all()
    #load_data_to_duckdb(os.path.join("data", "output.csv"), 'csv_table', conn)
    run_mapping(conn, os.path.join('config','mapping.json'))
    
    query = None
    with open(os.path.join('mappings', 'test_query.sql'), 'r', encoding='utf-8') as file:
        query = file.read()
    
    result = db.query_data(conn, query=query)

    print(result.head(50))

__main__()
    

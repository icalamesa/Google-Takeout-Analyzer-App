import os
import json
import pathlib
import mmap
import itertools
from datetime import datetime
from multiprocessing import Pool

import pandas as pd
from bs4 import BeautifulSoup, SoupStrainer
from icalendar import Calendar
from babel.dates import get_month_names, get_day_names


class DataPreprocessor:
    """
    A class responsible for loading and processing various Google Takeout
    datasets including profile JSON, CSV files (subscriptions, published videos),
    calendar ICS files, and HTML activity logs.
    """

    def __init__(
        self,
        html_chunk_factor=4,
        max_threads=8,
        calendar_path=None,
        profile_path=None,
        activity_log_paths=None,
        subscribed_channels_csv=None,
        published_videos_csv=None,
        output_folder=None
    ):
        """
        Initialize the DataPreprocessor with all required file paths and parameters.
        
        :param html_chunk_factor: Multiplier for determining the size of chunks 
                                  when reading large HTML files.
        :param max_threads: Maximum number of processes used for parallel operations.
        :param calendar_path: Directory path containing .ics calendar files.
        :param profile_path: File path to the JSON file containing profile data.
        :param activity_log_paths: List of file paths to HTML activity logs.
        :param subscribed_channels_csv: CSV file path for YouTube subscriptions data.
        :param published_videos_csv: CSV file path for published videos metadata.
        :param output_folder: Directory path for output data (if needed).
        """
        self.html_chunk_factor = html_chunk_factor
        self.max_threads = max_threads

        # Centralized Path Assignments
        self.calendar_path = calendar_path
        self.profile_path = profile_path
        self.activity_log_paths = activity_log_paths if activity_log_paths else []
        self.subscribed_channels_csv = subscribed_channels_csv
        self.published_videos_csv = published_videos_csv

        # Optional output folder
        self.output_folder = output_folder if output_folder else "data"

    # -------------------------------------------------------------------------
    #                      PRIVATE / UTILITY METHODS
    # -------------------------------------------------------------------------
    def _calculate_chunk_size(self, factor=None):
        """
        Calculate chunk size in bytes as a multiple of the memory page size.
        Defaults to the class-wide html_chunk_factor if none is provided.
        """
        factor = factor if factor else self.html_chunk_factor
        return mmap.PAGESIZE * factor

    def _translate_spanish_months(self, date_str):
        """
        Convert Spanish month abbreviations to English. 
        Example: 'mar' -> 'Mar' (for March), so standard libraries can parse them.
        """
        month_map = {
            'ene': 'Jan', 'feb': 'Feb', 'mar': 'Mar', 'abr': 'Apr',
            'may': 'May', 'jun': 'Jun', 'jul': 'Jul', 'ago': 'Aug',
            'oct': 'Oct', 'nov': 'Nov', 'dic': 'Dec', 'sept': 'Sep'
        }
        for sp, en in month_map.items():
            date_str = date_str.replace(sp, en)
        return date_str

    # -------------------------------------------------------------------------
    #                            DATE PARSING
    # -------------------------------------------------------------------------
    def parse_date(self, date_str, lang='es'):
        """
        Parse a date string and return it in 'YYYY-MM-DD HH:MM:SS' format.
        If lang='es', Spanish month abbreviations are translated first.
        Returns '-1' if parsing fails.
        """
        if lang == 'es':
            date_str = self._translate_spanish_months(date_str)
        try:
            dt = datetime.strptime(date_str, '%d %b %Y, %H:%M:%S cet')
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            return '-1'

    # -------------------------------------------------------------------------
    #                           HTML PARSING
    # -------------------------------------------------------------------------
    def _stream_html_in_chunks(self, file_path, chunk_size=524288):
        """
        Generator function that reads an HTML file incrementally, ensuring
        we yield only complete tag boundaries. This avoids partial tags 
        within a chunk.
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

    def _extract_html_chunk_data(self, html_content):
        """
        Extracts relevant data from a chunk of HTML content specific to 
        Google Takeout's "My Activity" format.
        """
        strainer = SoupStrainer('div', class_="outer-cell mdl-cell mdl-cell--12-col mdl-shadow--2dp")
        soup = BeautifulSoup(html_content, 'html.parser', parse_only=strainer)
        entries = []
        for outer_div in soup.find_all('div', recursive=False):
            # Retrieve platform (if present)
            platform_elem = outer_div.find('p', class_="mdl-typography--title")
            platform = platform_elem.text.strip() if platform_elem else None

            content_cells = outer_div.find_all(
                'div', class_="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1"
            )
            for div in content_cells:
                links = div.find_all('a', href=True, recursive=False)
                stripped_strings = list(div.stripped_strings)
                
                link_action_text = links[0].get_text(strip=True) if links else ''
                action_code, timestamp = '', ''

                if stripped_strings:
                    action_parts = stripped_strings[0].split('|')
                    action_code = action_parts[0].strip()
                    timestamp = stripped_strings[-1].strip()

                entries.append({
                    "platform": platform,
                    "action_code": action_code,
                    "timestamp": self.parse_date(timestamp),
                    "link_action_name": links[0]['href'] if len(links) > 0 else '',
                    "link_action_text": link_action_text,
                    "channel_link": links[1]['href'] if len(links) > 1 else '',
                    "channel_name": links[1].get_text(strip=True) if len(links) > 1 else '',
                    "link3": links[2]['href'] if len(links) > 2 else '',
                    "link3_text": links[2].get_text(strip=True) if len(links) > 2 else '',
                    "all": stripped_strings
                })
        return entries

    def read_activity_html(self, file_path):
        """
        Read and parse a large Google Takeout HTML activity log using multiprocessing.
        Each chunk is processed by '_extract_html_chunk_data'.
        """
        file_size = os.path.getsize(file_path)
        chunk_size = self._calculate_chunk_size()
        num_chunks = (file_size // chunk_size) + 1
        num_processes = min(num_chunks, self.max_threads)

        chunk_stream = self._stream_html_in_chunks(file_path, chunk_size=chunk_size)
        with Pool(processes=num_processes) as pool:
            result_iter = pool.imap_unordered(self._extract_html_chunk_data, chunk_stream)
            records = itertools.chain.from_iterable(result_iter)
            df = pd.DataFrame(records)
        return df

    # -------------------------------------------------------------------------
    #                          ICS (CALENDAR) PARSING
    # -------------------------------------------------------------------------
    def parse_ics(self, file_path):
        """
        Parse calendar events from an ICS file and return them as 
        a list of dictionaries (title, start, end, etc.).
        """
        with open(file_path, 'rb') as file:
            data = file.read()
        cal = Calendar.from_ical(data)
        calendar_name = pathlib.Path(file_path).stem

        meetings = []
        for component in cal.walk():
            if component.name == "VEVENT":
                summary = component.get('summary')
                start_value = component.get('dtstart')
                end_value = component.get('dtend')
                organizer_value = component.get('organizer')

                start_dt = start_value.dt if start_value else None
                end_dt = end_value.dt if end_value else None

                organizer = None
                if organizer_value:
                    # Convert organizer object to string then strip 'mailto:'
                    org_str = str(organizer_value)
                    organizer = org_str.replace('mailto:', '')

                meetings.append({
                    'Calendar': calendar_name,
                    'Title': summary,
                    'Start': start_dt,
                    'End': end_dt,
                    'Duration': (end_dt - start_dt) if (start_dt and end_dt) else None,
                    'Organizer': organizer
                })
        return meetings

    # -------------------------------------------------------------------------
    #                           PROFILE DATA PARSING
    # -------------------------------------------------------------------------
    def parse_profile_file(self):
        """
        Parse JSON profile data, returning a dictionary with keys like 
        givenName, formattedName, displayName, email, and genderType.
        """
        if not self.profile_path or not os.path.isfile(self.profile_path):
            raise FileNotFoundError(f"Profile JSON path is invalid or not set: {self.profile_path}")

        with open(self.profile_path, 'r') as file:
            data = json.load(file)

        return {
            "givenName": data['name']['givenName'],
            "formattedName": data['name']['formattedName'],
            "displayName": data['displayName'],
            "email": data['emails'][0]['value'],
            "genderType": data['gender']['type']
        }

    # -------------------------------------------------------------------------
    #                           DATA LOADING
    # -------------------------------------------------------------------------
    def load_all_datasets(self):
        """
        Load and process all configured datasets. Returns a dictionary of 
        non-empty DataFrames:
          {
            "person_info": <DataFrame>,
            "subscribed_channels": <DataFrame>,
            "published_videos": <DataFrame>,
            "calendar_events": <DataFrame>,
            "activity_logs": <DataFrame>
          }
        """
        # 1. PROFILE
        profile_df = pd.DataFrame([self.parse_profile_file()])

        # 2. YOUTUBE SUBSCRIPTIONS
        if self.subscribed_channels_csv and os.path.exists(self.subscribed_channels_csv):
            subscribed_channels_df = pd.read_csv(self.subscribed_channels_csv)
        else:
            subscribed_channels_df = pd.DataFrame()

        # 3. PUBLISHED VIDEOS
        if self.published_videos_csv and os.path.exists(self.published_videos_csv):
            published_videos_df = pd.read_csv(self.published_videos_csv)
        else:
            published_videos_df = pd.DataFrame()

        # 4. CALENDAR ICS
        calendar_frames = []
        if self.calendar_path and os.path.isdir(self.calendar_path):
            for entry in os.scandir(self.calendar_path):
                if entry.is_file() and entry.path.endswith('.ics'):
                    ics_events = self.parse_ics(entry.path)
                    if ics_events:
                        calendar_frames.append(pd.DataFrame(ics_events))
        calendar_events_df = pd.concat(calendar_frames) if calendar_frames else pd.DataFrame()

        # 5. ACTIVITY LOGS (HTML)
        activity_log_frames = []
        for path in self.activity_log_paths:
            if os.path.isfile(path):
                activity_log_frames.append(self.read_activity_html(path))
        activity_logs_df = (
            pd.concat(activity_log_frames, ignore_index=True) 
            if activity_log_frames else pd.DataFrame()
        )

        # Consolidate
        all_data = {
            "person_info": profile_df,
            "subscribed_channels": subscribed_channels_df,
            "published_videos": published_videos_df,
            "calendar_events": calendar_events_df,
            "activity_logs": activity_logs_df
        }

        # Return only non-empty DataFrames
        return {name: df for name, df in all_data.items() if not df.empty}

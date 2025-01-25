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
from babel.dates import get_month_names, get_day_names

class DataPreprocessor:
    def __init__(self, html_chunk_factor=4, max_threads=8, calendar_path=None, profile_path=None, activity_log_paths=None):
        self.html_chunk_factor = html_chunk_factor
        self.max_threads = max_threads
        self.calendar_path = calendar_path
        self.profile_path = profile_path
        self.activity_log_paths = activity_log_paths if activity_log_paths else []

    def get_optimal_chunk_size(self, factor=None):
        """Calculate chunk size as a multiple of the memory page size."""
        factor = factor if factor else self.html_chunk_factor
        page_size = mmap.PAGESIZE
        return page_size * factor

    def get_html_contents(self, file_path, chunk_size=524288):
        """Reads HTML file in chunks to avoid loading the entire file into memory."""
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
        """Translate Spanish month names to English."""
        spanish_months = {
            'ene': 'Jan', 'feb': 'Feb', 'mar': 'Mar', 'abr': 'Apr',
            'may': 'May', 'jun': 'Jun', 'jul': 'Jul', 'ago': 'Aug',
            'oct': 'Oct', 'nov': 'Nov', 'dic': 'Dec', 'sept': 'Sep'
        }
        for sp, en in spanish_months.items():
            date_str = date_str.replace(sp, en)
        return date_str

    def parse_date(self, date_str, lang='es'):
        """Parse and translate date string to a standard format."""
        try:
            if lang == 'es':
                date_str = self.translate_date(date_str)
            dt = datetime.strptime(date_str, '%d %b %Y, %H:%M:%S cet')
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            return '-1'

    def process_chunk(self, html_content):
        """Processes a chunk of HTML to extract relevant data."""
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

    def read_html(self, file_path):
        """Processes an HTML file using multiprocessing."""
        file_size = os.path.getsize(file_path)
        chunk_size = self.get_optimal_chunk_size(factor=self.html_chunk_factor)
        num_chunks = (file_size // chunk_size) + 1
        num_processes = min(num_chunks, self.max_threads)
        chunks = self.get_html_contents(file_path, chunk_size=chunk_size)
        with Pool(processes=num_processes) as pool:
            result_iter = pool.imap_unordered(self.process_chunk, chunks)
            results = list(itertools.chain.from_iterable(result_iter))
        return pd.DataFrame(results)

    def parse_profile_file(self):
        """Parse profile data from JSON."""
        with open(self.profile_path, 'r') as file:
            data = json.load(file)
        person_info = {
            "givenName": data['name']['givenName'],
            "formattedName": data['name']['formattedName'],
            "displayName": data['displayName'],
            "email": data['emails'][0]['value'],  # Assume there is at least one email
            "genderType": data['gender']['type']
        }
        return person_info

    def parse_ics(self, file_path):
        """Parse calendar events from an ICS file."""
        with open(file_path, 'rb') as file:
            cal = Calendar.from_ical(file.read())
        calendar_name = pathlib.Path(file_path).stem
        meetings = []
        for component in cal.walk():
            if component.name == "VEVENT":
                summary = component.get('summary')
                start = component.get('dtstart').dt
                end = component.get('dtend').dt
                organizer = component.get('organizer').replace('mailto:', '') if component.get('organizer') else None
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

    def load_all(self):
        """Load and process all the required datasets."""
        person_info = pd.DataFrame([self.parse_profile_file()])
        subscribed_channels_df = pd.read_csv("/path/to/subscribed_channels.csv")
        published_videos_metadata_df = pd.read_csv("/path/to/published_videos.csv")
        list_df = pd.read_csv("/path/to/playlists.csv")

        calendar_events = []
        if self.calendar_path:
            for entry in os.scandir(self.calendar_path):
                calendar_events.append(pd.DataFrame(self.parse_ics(entry)))
        calendar_events = pd.concat(calendar_events)
        activity_logs_df = pd.concat([self.read_html(path) for path in self.activity_log_paths], ignore_index=True)

        return person_info, subscribed_channels_df, published_videos_metadata_df, list_df, calendar_events, activity_logs_df


import os
import pytest
import tempfile
import shutil
import pandas as pd

from app.data_preprocessor import DataPreprocessor


@pytest.fixture
def temporary_dir():
    """
    Create a temporary directory for testing files and return its path.
    Clean up after tests complete.
    """
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_profile_json(temporary_dir):
    """
    Create a small JSON file that simulates a user profile JSON structure.
    Returns the file path.
    """
    profile_data = {
        "name": {
            "givenName": "John",
            "formattedName": "John Doe"
        },
        "displayName": "Johnny",
        "emails": [
            {"value": "john@example.com"}
        ],
        "gender": {
            "type": "male"
        }
    }
    file_path = os.path.join(temporary_dir, "profile.json")
    with open(file_path, "w", encoding="utf-8") as f:
        import json
        json.dump(profile_data, f)
    return file_path


@pytest.fixture
def data_preprocessor_instance(sample_profile_json, temporary_dir):
    """
    Provide an instance of DataPreprocessor for tests,
    pointing to any sample files/folders we want.
    """
    return DataPreprocessor(
        html_chunk_factor=2,
        max_threads=2,
        profile_path=sample_profile_json,
        calendar_path=os.path.join(temporary_dir, "calendar_files"),
        activity_log_paths=[],
        subscribed_channels_csv=None,
        published_videos_csv=None,
        output_folder=temporary_dir
    )


def test_parse_date(data_preprocessor_instance):
    """
    Test parsing a Spanish date string into a standardized format.
    """
    dp = data_preprocessor_instance
    date_str = "01 ene 2023, 12:30:00 cet"
    result = dp.parse_date(date_str, lang='es')
    assert result == "2023-01-01 12:30:00", "Should parse Spanish months to correct datetime."


def test_parse_profile_file(data_preprocessor_instance):
    """
    Ensure parse_profile_file properly reads the sample profile JSON.
    """
    dp = data_preprocessor_instance
    profile_info = dp.parse_profile_file()
    
    assert profile_info["givenName"] == "John"
    assert profile_info["formattedName"] == "John Doe"
    assert profile_info["displayName"] == "Johnny"
    assert profile_info["email"] == "john@example.com"
    assert profile_info["genderType"] == "male"


def test_stream_html_in_chunks(data_preprocessor_instance, temporary_dir):
    """
    Example test verifying we can read chunked HTML.
    Typically you'd create a small HTML file and confirm 
    the chunking logic. Here, we do a minimal check.
    """
    dp = data_preprocessor_instance

    # Create a small HTML file
    test_html_path = os.path.join(temporary_dir, "test_activity.html")
    sample_html_content = """
    <div class="outer-cell mdl-cell mdl-cell--12-col mdl-shadow--2dp">
        <p class="mdl-typography--title">Test Platform</p>
        <div class="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1">
            <a href="http://example.com">Example Link</a>
            Some text | 01 ene 2023, 10:00:00 cet
        </div>
    </div>
    <div class="outer-cell mdl-cell mdl-cell--12-col mdl-shadow--2dp">
        <p class="mdl-typography--title">Test Platform</p>
        <div class="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1">
            <a href="http://example.com">Example Link 2</a>
            Some text | 02 feb 2023, 10:00:00 cet
        </div>
    </div>
    <div class="outer-cell mdl-cell mdl-cell--12-col mdl-shadow--2dp">
        <p class="mdl-typography--title">Test Platform</p>
        <div class="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1">
            <a href="http://example.com">Example Link 3</a>
            Some text | 03 mar 2023, 10:00:00 cet
        </div>
    </div>
    """
    with open(test_html_path, "w", encoding="utf-8") as f:
        f.write(sample_html_content)

    df = dp.read_activity_html(test_html_path)
    assert not df.empty, "DataFrame should not be empty after parsing valid HTML."
    assert df.iloc[0]["platform"] == "Test Platform"


def test_parse_ics_no_events(data_preprocessor_instance, temporary_dir):
    """
    Test parse_ics on an empty or minimal ICS file with no VEVENT.
    Should return an empty list of meetings.
    """
    dp = data_preprocessor_instance

    # Create an empty .ics file
    ics_file_path = os.path.join(temporary_dir, "no_events.ics")
    with open(ics_file_path, "wb") as f:
        f.write(b"BEGIN:VCALENDAR\nVERSION:2.0\nEND:VCALENDAR\n")

    events = dp.parse_ics(ics_file_path)
    assert events == [], "No events should be found in an empty ICS calendar."


def test_load_all_datasets(data_preprocessor_instance):
    """
    High-level test to ensure load_all_datasets returns a dictionary,
    and the 'person_info' DataFrame is present (since we gave it a profile).
    """
    dp = data_preprocessor_instance
    results = dp.load_all_datasets()

    assert isinstance(results, dict), "Should return a dictionary of DataFrames."
    assert "person_info" in results, "person_info should be included if profile is parsed."
    assert not results["person_info"].empty, "person_info DataFrame should not be empty."

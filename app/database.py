import duckdb
import pandas as pd
import xml.etree.ElementTree as ET
import os
import pathlib

SUPPORTED_FILE_TYPES = ['.csv', '.html', '.xml', '.json']

def find_leaf_files(directory):
    """
    Finds all files in the given directory that are leaves (i.e., files that are not directories themselves).
    
    Args:
    - directory (str): The path to the directory to search within.

    Returns:
    - list of str: A list of paths to leaf files.
    """
    leaf_files = []
    for root, dirs, files in os.walk(directory):
        if files:  # Check if there are files in the current directory
            # Append each file path to the leaf_files list
            for file in files:
                if pathlib.Path(file).suffix in SUPPORTED_FILE_TYPES: 
                    leaf_files.append(os.path.join(root, file))
    return leaf_files

def parse_xml_to_dataframe(xml_file_path):
    """Parse XML file to a pandas DataFrame."""
    tree = ET.parse(xml_file_path)
    root = tree.getroot()
    all_data = []
    for elem in root.findall('.//Record'):  # Adjust the path as necessary for your XML structure
        data_dict = {child.tag: child.text for child in elem}
        all_data.append(data_dict)
    return pd.DataFrame(all_data)

import pandas as pd
import duckdb

def parse_xml_to_dataframe(xml_file_path):
    pass

import csv
import pandas as pd
import duckdb

def detect_separator(sample):
    """Detect the CSV file separator using csv.Sniffer."""
    sniffer = csv.Sniffer()
    if sniffer.has_header(sample):
        dialect = sniffer.sniff(sample)
        return dialect.delimiter
    return ','

def load_csv_to_duckdb(file_path, table_name, conn):
    """Load a potentially empty CSV to DuckDB, detecting the separator."""
    try:
        with open(file_path, 'r') as file:
            sample = file.read(2048) 
            if not sample.strip(): 
                raise ValueError("File is empty or only contains whitespace.")

            separator = detect_separator(sample)
        df = pd.read_csv(file_path, delimiter=separator)

        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns] 

        if df.empty:
            query = f"CREATE TABLE {table_name} ({', '.join(df.columns)});"
            conn.execute(query)
        else:
            conn.register('temp_table', df)
            query = f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_table"
            conn.execute(query)

    except FileNotFoundError:
        raise FileNotFoundError(f"The file {file_path} was not found.")
    except ValueError as e:
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (dummy_column INTEGER);")
        print(e)
    except Exception as e:
        raise Exception(f"An error occurred while processing {file_path}: {str(e)}")

def detect_separator(file_path):
    """Detect the CSV file separator using a heuristic or a simple parser."""
    with open(file_path, 'r') as file:
        header = file.readline()
        sniffer = csv.Sniffer()
        if sniffer.has_header(header):
            return sniffer.sniff(header).delimiter
    return '\t'

def setup_database(db_file=os.path.join('data', 'my_duckdb.duckdb'), reset=False):
    """Setup and return a DuckDB connection. Optionally reset the database.
    
    Args:
        db_file (str): Path to the DuckDB database file.
        reset (bool): Flag to determine whether to reset the database by deleting the existing file.

    Returns:
        duckdb.DuckDBPyConnection: A connection to the DuckDB database.
    """
    if reset:
        if os.path.exists(db_file):
            os.remove(db_file)
            print(f"Database reset: {db_file} has been removed.")
    
    conn = duckdb.connect(database=db_file)
    print(f"Connected to DuckDB database at {db_file}.")
    return conn

def create_raw_view(conn, file_path, table_name):
    """Create a SQL view in DuckDB for direct file query based on its type."""
    view_name = f"raw_{table_name}"
    try:
        if file_path.endswith('.csv'):
            separator = detect_separator(file_path)  
            query = f"CREATE VIEW {view_name} AS SELECT * FROM read_csv_auto('{file_path}')"
        elif file_path.endswith('.json'):
            query = f"CREATE VIEW {view_name} AS SELECT * FROM read_json_auto('{file_path}')"
        elif file_path.endswith('.xml'):
            query = f"CREATE VIEW {view_name} AS SELECT * FROM read_xml_auto('{file_path}')" 
        else:
            raise ValueError(f"Unsupported file type for {file_path}")

        conn.execute(query)
        print(f"View {view_name} created successfully for {file_path}.")
    except Exception as e:
        raise Exception(f"An error occurred while creating the view: {str(e)}")

def query_data(conn, query):
        """Execute a SQL query and return the result as a DataFrame."""
        return conn.execute(query).df()

def create_table_from_mapping(conn, mapping_path):
    """Load a table from a provided SQL mapping file.
    
    Args:
    - conn: A connection object to the database.
    - mapping_path: Path to the SQL file that contains the mapping.
    """
    try:
        with open(mapping_path, 'r') as file:
            script = file.read().strip()
            if not script:
                raise ValueError("Mapping file is empty or only contains whitespace.")

        conn.execute(script)

    except FileNotFoundError:
        raise FileNotFoundError(f"The file {mapping_path} was not found.")
    except ValueError as e:
        raise ValueError(f"Error in mapping file content: {str(e)}")
    except Exception as e:
        raise Exception(f"An error occurred while processing {mapping_path}: {str(e)}")


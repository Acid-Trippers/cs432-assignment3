import os

# Project root directory
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Data directory
DATA_DIR = os.path.join(ROOT_DIR, "data")

# JSON and Data File Paths
INITIAL_SCHEMA_FILE = os.path.join(DATA_DIR, "initial_schema.json")
COUNTER_FILE = os.path.join(DATA_DIR, "counter.txt")
RECEIVED_DATA_FILE = os.path.join(DATA_DIR, "received_data.json")
CLEANED_DATA_FILE = os.path.join(DATA_DIR, "cleaned_data.json")
BUFFER_FILE = os.path.join(DATA_DIR, "buffer.json")
ANALYZED_SCHEMA_FILE = os.path.join(DATA_DIR, "analyzed_schema.json")
METADATA_FILE = os.path.join(DATA_DIR, "metadata.json")

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

"""
Splits the globally cleaned dataset into distinct operational payloads for disparate database engines 
based on the final algorithmic routing decisions in the metadata playbook.

- Decision Ingestion: Reads the final SQL/MONGO/UNKNOWN decisions tagged on each field from metadata.json.
- Route Mapping: Compiles a hierarchical dictionary to identify where top-level parent keys and their entire nested structures should safely be sent.
- Payload Fragmentation: Iterates over the master cleaned_data.json and physically splits horizontal records into three vertical shards (SQL fields, Mongo fields, Unknown fields).
- Checkpoint Export: Serializes the fragmented data out to three independent temporary JSON files (sql_data.json, mongo_data.json, unknown_data.json) to separate database engine concerns.
"""

import json
import os
from typing import Dict, List

from src.config import CLEANED_DATA_FILE, DATA_DIR, METADATA_FILE, BUFFER_FILE
SQL_OUTPUT_FILE = os.path.join(DATA_DIR, "sql_data.json")
MONGO_OUTPUT_FILE = os.path.join(DATA_DIR, "mongo_data.json")
UNKNOWN_OUTPUT_FILE = os.path.join(DATA_DIR, "unknown_data.json")


def _read_json(file_path: str):
    with open(file_path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(file_path: str, payload) -> None:
    with open(file_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def _build_field_routes(field_metadata: List[Dict]) -> Dict[str, str]:
    routes: Dict[str, str] = {}

    for field in field_metadata:
        field_name = field.get("field_name") or field.get("fieldName")
        if not field_name:
            continue

        top_level_name = str(field_name).split(".", 1)[0].replace("[]", "")
        decision = str(field.get("decision", "UNKNOWN")).upper()
        
        # ADDITION: Check for probation/discovery flag
        is_buffer = field.get("is_discovered_buffer", False)

        if decision not in {"SQL", "MONGO", "UNKNOWN"}:
            decision = "UNKNOWN"

        # Force PK candidates or record_id to BOTH
        if field.get("is_primary_key_candidate") is True or top_level_name == "record_id":
            decision = "BOTH"
        elif is_buffer:
            decision = "BUFFER"

        if top_level_name not in routes or decision == "BOTH":
            routes[top_level_name] = decision

    return routes


def route_data() -> None:
    if not os.path.exists(METADATA_FILE) or not os.path.exists(CLEANED_DATA_FILE):
        return

    metadata = _read_json(METADATA_FILE)
    cleaned_data = _read_json(CLEANED_DATA_FILE)
    if not cleaned_data: return

    routes = _build_field_routes(metadata.get('fields', []))

    # Helper to load existing data for appending
    def _load_existing(path):
        if os.path.exists(path):
            try: return _read_json(path)
            except: return []
        return []

    sql_data = _load_existing(SQL_OUTPUT_FILE)
    mongo_data = _load_existing(MONGO_OUTPUT_FILE)
    buffer_data = _load_existing(BUFFER_FILE)

    for record in cleaned_data:
        sql_rec, mongo_rec, buf_rec = {}, {}, {}
        # Get the glue ID
        ref_id = record.get("record_id")

        for key, value in record.items():
            decision = routes.get(key, "UNKNOWN")
            
            if decision == "BUFFER":
                buffer_data.append({"record_id": ref_id, "field": key, "value": value})
            elif decision == "SQL":
                sql_rec[key] = value
            elif decision == "MONGO":
                mongo_rec[key] = value
            elif decision == "BOTH":
                sql_rec[key] = value
                mongo_rec[key] = value

        if sql_rec: sql_data.append(sql_rec)
        if mongo_rec: mongo_data.append(mongo_rec)

    # Write back the persistent shards
    _write_json(SQL_OUTPUT_FILE, sql_data)
    _write_json(MONGO_OUTPUT_FILE, mongo_data)
    _write_json(BUFFER_FILE, buffer_data)

    # FINAL FLUSH: Clear the cleaned_data.json
    _write_json(CLEANED_DATA_FILE, [])
    stats = {
        "sql": len(sql_data), 
        "mongo": len(mongo_data), 
        "buffer": len(buffer_data)
    }
    print(f"[*] Routed: SQL({stats['sql']}) | Mongo({stats['mongo']}) | Buffer({stats['buffer']})")
    
    return stats


if __name__ == "__main__":
    route_data()
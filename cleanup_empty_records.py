"""
Cleanup utility to identify and remove empty records from SQL and MongoDB.

An "empty record" is defined as having:
- Only record_id field (no other fields with data)
- All fields are NULL/empty
"""

import json
from src.phase_5.sql_engine import SQLEngine
from src.phase_5.mongo_engine import determineMongoStrategy
from src.config import MONGO_URI, MONGO_DB_NAME, METADATA_FILE
from pymongo import MongoClient
from sqlalchemy import text
from pathlib import Path

def get_metadata():
    """Load metadata to get field names"""
    try:
        with open(METADATA_FILE, 'r') as f:
            metadata = json.load(f)
            return {field['field_name']: field for field in metadata.get('fields', [])}
    except:
        return {}

def identify_empty_records():
    """Find empty records in SQL and MongoDB"""
    
    # Initialize SQL engine
    sql_engine = SQLEngine()
    sql_engine.initialize()
    session = sql_engine.schema_builder.get_session()
    
    # Initialize MongoDB
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    mongo_db = mongo_client[MONGO_DB_NAME]
    
    metadata = get_metadata()
    empty_record_ids = set()
    
    try:
        # Check SQL records for emptiness
        main_model = sql_engine.models.get("main_records")
        if main_model:
            print("[*] Checking SQL records for empty entries...")
            
            # Get all record IDs from SQL
            sql_records = session.query(main_model).all()
            
            for record in sql_records:
                # Check if all non-ID fields are NULL
                is_empty = True
                for col in main_model.__table__.columns:
                    if col.name != 'record_id':
                        col_value = getattr(record, col.name, None)
                        if col_value is not None and str(col_value).strip():
                            is_empty = False
                            break
                
                if is_empty:
                    empty_record_ids.add(record.record_id)
                    print(f"  [!] Empty SQL record found: record_id={record.record_id}")
        
        # Check MongoDB records for emptiness
        mongo_collection = mongo_db["main_records"]
        print("[*] Checking MongoDB records for empty entries...")
        
        for mongo_doc in mongo_collection.find():
            record_id = mongo_doc.get("_id")
            is_empty = True
            
            # Check all fields except _id and system fields
            for key, value in mongo_doc.items():
                if key not in ('_id', '_type', 'sys_ingested_time'):
                    if value is not None and str(value).strip():
                        is_empty = False
                        break
            
            if is_empty:
                empty_record_ids.add(record_id)
                print(f"  [!] Empty MongoDB record found: _id={record_id}")
        
        return empty_record_ids
        
    finally:
        session.close()
        mongo_client.close()

def delete_empty_records(record_ids):
    """Delete the identified empty records"""
    if not record_ids:
        print("[*] No empty records to delete.")
        return
    
    print(f"\n[*] Deleting {len(record_ids)} empty records...")
    
    # Initialize SQL engine
    sql_engine = SQLEngine()
    sql_engine.initialize()
    session = sql_engine.schema_builder.get_session()
    
    # Initialize MongoDB
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    mongo_db = mongo_client[MONGO_DB_NAME]
    
    try:
        # Delete from SQL
        main_model = sql_engine.models.get("main_records")
        if main_model:
            for record_id in record_ids:
                try:
                    session.query(main_model).filter(main_model.record_id == record_id).delete()
                except:
                    pass
            session.commit()
            print(f"  [✓] Deleted {len(record_ids)} records from SQL")
        
        # Delete from MongoDB
        mongo_collection = mongo_db["main_records"]
        for record_id in record_ids:
            mongo_collection.delete_one({"_id": record_id})
        print(f"  [✓] Deleted {len(record_ids)} records from MongoDB")
        
        print(f"[*] Cleanup complete! Removed {len(record_ids)} empty records.")
        
    finally:
        session.close()
        mongo_client.close()

if __name__ == "__main__":
    import sys
    
    print("=" * 60)
    print("EMPTY RECORD CLEANUP UTILITY")
    print("=" * 60)
    
    empty_ids = identify_empty_records()
    
    if empty_ids:
        print(f"\n[!] Found {len(empty_ids)} empty records: {sorted(empty_ids)}")
        
        # Auto-confirm if --auto flag passed or if stdin is not a TTY
        auto_confirm = '--auto' in sys.argv or not sys.stdin.isatty()
        
        if auto_confirm:
            print("\n[*] Auto-confirming deletion...")
            delete_empty_records(empty_ids)
        else:
            confirm = input("\nProceed with deletion? (type 'yes' to confirm): ").strip().lower()
            if confirm == 'yes':
                delete_empty_records(empty_ids)
            else:
                print("[*] Operation cancelled.")
    else:
        print("\n[✓] No empty records found!")

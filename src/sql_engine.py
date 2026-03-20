"""
SQL Engine Module
Purpose: Handle data normalization, CRUD operations, and nested data decomposition.

Responsibilities:
1. Denormalize nested JSON data into relational SQL tables
2. Handle array decomposition (store array items in junction tables)
3. Manage primary/foreign key relationships
4. Insert, read, update, delete operations
5. Handle bulk inserts from sql_data.json
6. Maintain referential integrity
"""

import json
import os
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import logging

from sqlalchemy import inspect, text
from sqlalchemy.orm import Session

from config import DATA_DIR, METADATA_MANAGER_FILE, FIELD_METADATA_FILE
from sql_schema_definer import SQLSchemaBuilder

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataNormalizer:
    """Handles normalization of nested data for SQL storage"""
    
    def __init__(self, metadata_manager: Dict = None, field_metadata: Dict = None):
        self.metadata_manager = metadata_manager or {}
        self.field_metadata = field_metadata or {}
    
    def load_metadata(self):
        """Load metadata files"""
        if os.path.exists(METADATA_MANAGER_FILE):
            with open(METADATA_MANAGER_FILE, 'r') as f:
                data = json.load(f)
                self.metadata_manager = {
                    field['field_name']: field
                    for field in data.get('fields', [])
                }
        
        if os.path.exists(FIELD_METADATA_FILE):
            with open(FIELD_METADATA_FILE, 'r') as f:
                self.field_metadata = {
                    field['fieldName']: field
                    for field in json.load(f)
                }
    
    def normalize_record(self, record: Dict) -> Tuple[Dict, Dict[str, List[Dict]]]:
        """
        Decompose a nested record into:
        1. Root-level data (for main_records table)
        2. Nested tables data (for separate tables)
        
        Returns:
            (root_data, nested_data_by_table)
        
        Example:
            Input:  {name: "John", tags: ["a", "b"], sensor: {temp: 25}}
            Output: (
                {name: "John"},
                {
                    "main_records_tags": [
                        {"value": "a", "position": 0},
                        {"value": "b", "position": 1}
                    ],
                    "main_records_sensor": [
                        {"temp": 25}
                    ]
                }
            )
        """
        root_data = {}
        nested_data = {}  # table_name -> list of records
        
        for field_name, value in record.items():
            metadata = self.metadata_manager.get(field_name, {})
            
            # Determine if this is root, nested, or array
            is_nested = metadata.get('is_nested', False)
            is_array = metadata.get('is_array', False)
            
            if is_array:
                # Handle array fields
                table_name = f"main_records_{field_name}".replace('.', '_')
                
                if metadata.get('array_content_type') == 'primitive':
                    # Array of primitives
                    nested_data[table_name] = [
                        {
                            'value': str(item),
                            'value_type': type(item).__name__.lower(),
                            'position': idx
                        }
                        for idx, item in enumerate(value if isinstance(value, list) else [value])
                    ]
                else:
                    # Array of objects
                    nested_data[table_name] = [
                        {**item, 'position': idx}
                        if isinstance(item, dict) else {'data': item, 'position': idx}
                        for idx, item in enumerate(value if isinstance(value, list) else [value])
                    ]
            
            elif is_nested:
                # Handle nested objects
                table_name = f"main_records_{field_name}".replace('.', '_')
                
                if isinstance(value, dict):
                    nested_data[table_name] = [value]
                else:
                    # If nested field is missing or None, skip it
                    pass
            
            else:
                # Root-level field
                if value is not None:
                    root_data[field_name] = value
        
        return root_data, nested_data
    
    def parse_nested_fields(self, record: Dict) -> Dict[str, Any]:
        """
        Parse nested field paths (e.g., from metadata: "metadata.sensor_data.readings")
        This is for handling deeply nested structures.
        """
        parsed = {}
        
        def flatten(obj, prefix=""):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    key = f"{prefix}.{k}" if prefix else k
                    if isinstance(v, (dict, list)):
                        flatten(v, key)
                    else:
                        parsed[key] = v
            elif isinstance(obj, list):
                for idx, item in enumerate(obj):
                    key = f"{prefix}[{idx}]"
                    flatten(item, key)
        
        flatten(record)
        return parsed


class SQLEngine:
    """Core SQL operations and data management"""
    
    def __init__(self, database_url: str = None):
        self.schema_builder = SQLSchemaBuilder(database_url=database_url)
        self.normalizer = DataNormalizer()
        self.session: Optional[Session] = None
        self.models = {}
        self.table_relationships = {}  # Maps table names to their parent
    
    def initialize(self) -> bool:
        """Initialize the SQL engine and database schema"""
        try:
            logger.info("Initializing SQL Engine...")
            
            # Build schema
            self.schema_builder.analyze_and_build()
            self.models = self.schema_builder.get_models()
            
            # Load metadata
            self.normalizer.load_metadata()
            
            # Build table relationships
            self._build_relationships()
            
            # Get session
            self.session = self.schema_builder.get_session()
            
            logger.info("SQL Engine initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize SQL Engine: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _build_relationships(self):
        """Build parent-child relationship map"""
        for table_name in self.models.keys():
            if table_name.startswith('main_records_'):
                self.table_relationships[table_name] = 'main_records'
            elif '_' in table_name:
                parts = table_name.rsplit('_', 1)
                if len(parts) == 2:
                    self.table_relationships[table_name] = parts[0]
    
    def insert_record(self, record: Dict) -> Optional[int]:
        """
        Insert a record with all its nested data.
        
        Returns:
            ID of inserted record in main_records table, or None if failed
        """
        try:
            # Normalize the record
            root_data, nested_data = self.normalizer.normalize_record(record)
            
            # Insert root record
            MainRecords = self.models.get('main_records')
            if not MainRecords:
                logger.error("MainRecords model not found")
                return None
            
            main_record = MainRecords(**root_data)
            self.session.add(main_record)
            self.session.flush()  # Get the ID without committing
            record_id = main_record.id
            
            # Insert nested records
            for table_name, nested_records in nested_data.items():
                NestedModel = self.models.get(table_name)
                if not NestedModel:
                    logger.warning(f"Model for {table_name} not found, skipping")
                    continue
                
                # Get the foreign key column name
                parent_table = self.table_relationships.get(table_name, 'main_records')
                fk_column = f"{parent_table}_id"
                
                for nested_record in nested_records:
                    nested_record[fk_column] = record_id
                    nested_obj = NestedModel(**nested_record)
                    self.session.add(nested_obj)
            
            self.session.commit()
            logger.info(f"Inserted record with ID {record_id}")
            return record_id
            
        except Exception as e:
            logger.error(f"Error inserting record: {e}")
            self.session.rollback()
            import traceback
            traceback.print_exc()
            return None
    
    def bulk_insert_from_file(self, json_file: str) -> Tuple[int, int]:
        """
        Bulk insert records from a JSON file.
        
        Returns:
            (successful_inserts, failed_inserts)
        """
        if not os.path.exists(json_file):
            logger.error(f"File not found: {json_file}")
            return 0, 0
        
        try:
            with open(json_file, 'r') as f:
                records = json.load(f)
            
            if not isinstance(records, list):
                logger.error("JSON file must contain a list of records")
                return 0, 1
            
            success_count = 0
            fail_count = 0
            
            logger.info(f"Starting bulk insert of {len(records)} records...")
            
            for idx, record in enumerate(records):
                record_id = self.insert_record(record)
                if record_id:
                    success_count += 1
                else:
                    fail_count += 1
                
                if (idx + 1) % 100 == 0:
                    logger.info(f"  Processed {idx + 1}/{len(records)} records...")
            
            logger.info(f"Bulk insert complete: {success_count} success, {fail_count} failed")
            return success_count, fail_count
            
        except Exception as e:
            logger.error(f"Bulk insert failed: {e}")
            return 0, 1
    
    def query_all(self, table_name: str = 'main_records', limit: int = 100) -> List[Dict]:
        """Query all records from a table (with limit)"""
        try:
            Model = self.models.get(table_name)
            if not Model:
                logger.error(f"Table {table_name} not found")
                return []
            
            records = self.session.query(Model).limit(limit).all()
            
            # Convert to dict
            result = []
            for record in records:
                result.append({col.name: getattr(record, col.name) for col in inspect(Model).columns})
            
            return result
            
        except Exception as e:
            logger.error(f"Query failed: {e}")
            return []
    
    def query_by_id(self, table_name: str, record_id: int) -> Optional[Dict]:
        """Query a specific record by ID"""
        try:
            Model = self.models.get(table_name)
            if not Model:
                return None
            
            record = self.session.query(Model).filter(Model.id == record_id).first()
            
            if record:
                return {col.name: getattr(record, col.name) for col in inspect(Model).columns}
            return None
            
        except Exception as e:
            logger.error(f"Query by ID failed: {e}")
            return None
    
    def get_record_with_nested(self, record_id: int) -> Optional[Dict]:
        """
        Get a record with all its nested data reconstructed.
        
        Returns:
            Complete record with nested arrays/objects restored
        """
        try:
            # Get root record
            root_record = self.query_by_id('main_records', record_id)
            if not root_record:
                return None
            
            # Add nested data
            for table_name, parent_table in self.table_relationships.items():
                if parent_table == 'main_records':
                    nested_records = self.session.query(self.models[table_name]).filter(
                        getattr(self.models[table_name], f"{parent_table}_id") == record_id
                    ).all()
                    
                    field_name = table_name.replace('main_records_', '')
                    nested_data = []
                    
                    for record in nested_records:
                        nested_data.append({
                            col.name: getattr(record, col.name)
                            for col in inspect(self.models[table_name]).columns
                            if col.name != f"{parent_table}_id"
                        })
                    
                    if nested_data:
                        root_record[field_name] = nested_data
            
            return root_record
            
        except Exception as e:
            logger.error(f"Error reconstructing record: {e}")
            return None
    
    def delete_record(self, record_id: int) -> bool:
        """Delete a record and all its nested data"""
        try:
            MainRecords = self.models.get('main_records')
            
            # Delete nested records first (CASCADE would handle this, but explicit is safer)
            for table_name, parent_table in self.table_relationships.items():
                if parent_table == 'main_records':
                    self.session.query(self.models[table_name]).filter(
                        getattr(self.models[table_name], f"{parent_table}_id") == record_id
                    ).delete()
            
            # Delete main record
            result = self.session.query(MainRecords).filter(MainRecords.id == record_id).delete()
            self.session.commit()
            
            return result > 0
            
        except Exception as e:
            logger.error(f"Delete failed: {e}")
            self.session.rollback()
            return False
    
    def get_table_count(self, table_name: str = 'main_records') -> int:
        """Get row count for a table"""
        try:
            Model = self.models.get(table_name)
            if not Model:
                return 0
            return self.session.query(Model).count()
        except Exception as e:
            logger.error(f"Count query failed: {e}")
            return 0
    
    def get_database_stats(self) -> Dict[str, int]:
        """Get statistics for all tables"""
        stats = {}
        for table_name in self.models.keys():
            stats[table_name] = self.get_table_count(table_name)
        return stats
    
    def close(self):
        """Close database connection"""
        if self.session:
            self.session.close()
            logger.info("Database connection closed")


def run_sql_engine_demo():
    """Demo/test the SQL engine"""
    print("\n" + "=" * 80)
    print("SQL ENGINE DEMO")
    print("=" * 80)
    
    engine = SQLEngine()
    
    if not engine.initialize():
        print("[!] Failed to initialize SQL Engine")
        return
    
    print("[+] SQL Engine initialized")
    
    # Demo insert
    demo_record = {
        "name": "John Doe",
        "email": "john@example.com",
        "age": 30,
    }
    
    record_id = engine.insert_record(demo_record)
    if record_id:
        print(f"[+] Inserted demo record with ID: {record_id}")
    
    # Demo query
    stats = engine.get_database_stats()
    print(f"\n[INFO] Database stats:")
    for table_name, count in stats.items():
        print(f"  {table_name}: {count} records")
    
    engine.close()


if __name__ == "__main__":
    run_sql_engine_demo()

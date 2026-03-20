"""
SQL Pipeline Orchestrator
Purpose: Integrate the SQL schema definer and engine into the main data pipeline.

This script:
1. Runs schema definition
2. Loads sql_data.json (routed data)
3. Bulk inserts into SQL database
4. Provides pipeline summary
"""

import os
import sys
import argparse
import json
from typing import Tuple

from config import SQL_DATA_FILE, DATA_DIR, DATABASE_URL
from sql_schema_definer import SQLSchemaBuilder
from sql_engine import SQLEngine


def run_sql_pipeline(schema_builder: SQLSchemaBuilder, engine: SQLEngine) -> Tuple[int, int]:
    """
    Run the complete SQL pipeline:
    1. Schema creation
    2. Data loading from sql_data.json
    
    Returns:
        (successful_inserts, failed_inserts)
    """
    print("\n" + "=" * 80)
    print("SQL PIPELINE ORCHESTRATOR")
    print("=" * 80)
    
    # Step 1: Initialize schema
    print("\n[STEP 1] Initializing SQL Schema...")
    if not engine.initialize():
        print("[!] Failed to initialize SQL Engine")
        return 0, 1
    
    stats = engine.get_database_stats()
    print(f"[+] Database initialized with {len(stats)} tables")
    
    # Step 2: Load data from sql_data.json
    print("\n[STEP 2] Loading SQL Data from Router...")
    
    if not os.path.exists(SQL_DATA_FILE):
        print(f"[!] No SQL data file found at {SQL_DATA_FILE}")
        print("[*] This is expected if no data has been routed yet.")
        print("[*] Run the ingestion and classification pipeline first.")
        return 0, 0
    
    # Check file size
    file_size = os.path.getsize(SQL_DATA_FILE)
    file_size_mb = file_size / (1024 * 1024)
    print(f"[*] Loading {file_size_mb:.2f} MB from {SQL_DATA_FILE}")
    
    # Step 3: Bulk insert
    print("\n[STEP 3] Bulk Inserting Records...")
    success_count, fail_count = engine.bulk_insert_from_file(SQL_DATA_FILE)
    
    # Step 4: Print summary
    print("\n" + "=" * 80)
    print("SQL PIPELINE SUMMARY")
    print("=" * 80)
    
    final_stats = engine.get_database_stats()
    
    print(f"\nDatabase: {DATABASE_URL}")
    print(f"\nLoad Results:")
    print(f"  Successful Inserts: {success_count}")
    print(f"  Failed Inserts: {fail_count}")
    print(f"  Total Processed: {success_count + fail_count}")
    
    print(f"\nFinal Database State:")
    total_records = 0
    for table_name, count in sorted(final_stats.items()):
        print(f"  {table_name:<30} {count:>10} records")
        total_records += count
    
    print(f"\nTotal Records: {total_records}")
    print("=" * 80)
    
    return success_count, fail_count


def create_sample_sql_data():
    """Create sample SQL data for testing (if no routed data exists)"""
    sample_data = [
        {
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "age": 28,
            "is_active": True,
            "subscription": "premium"
        },
        {
            "name": "Bob Smith",
            "email": "bob@example.com",
            "age": 35,
            "is_active": True,
            "subscription": "basic"
        },
        {
            "name": "Carol White",
            "email": "carol@example.com",
            "age": 42,
            "is_active": False,
            "subscription": "free"
        }
    ]
    
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(SQL_DATA_FILE, 'w') as f:
        json.dump(sample_data, f, indent=2)
    
    print(f"[+] Created sample SQL data at {SQL_DATA_FILE}")
    return sample_data


def main():
    parser = argparse.ArgumentParser(
        description="SQL Pipeline Orchestrator - Integrate SQL engine with data pipeline"
    )
    parser.add_argument(
        'command',
        nargs='?',
        choices=['run', 'init', 'sample', 'status'],
        default='run',
        help='Command to execute (default: run)'
    )
    parser.add_argument(
        '--database-url',
        help='Custom database URL (default: SQLite at data/engine.db)',
        default=None
    )
    
    args = parser.parse_args()
    
    # Initialize components
    schema_builder = SQLSchemaBuilder(database_url=args.database_url)
    engine = SQLEngine(database_url=args.database_url)
    
    try:
        if args.command == 'init':
            # Just initialize schema without loading data
            print("\n" + "=" * 80)
            print("SQL SCHEMA INITIALIZATION")
            print("=" * 80)
            
            if engine.initialize():
                stats = engine.get_database_stats()
                print(f"\n[+] Schema initialized successfully")
                print(f"[+] Tables created: {len(stats)}")
                for table_name, count in stats.items():
                    print(f"    {table_name}: {count} records")
            else:
                print("[!] Schema initialization failed")
                sys.exit(1)
        
        elif args.command == 'sample':
            # Create and load sample data
            print("\n[*] Creating sample SQL data...")
            sample_data = create_sample_sql_data()
            
            print("[*] Initializing SQL Engine...")
            if engine.initialize():
                print("[*] Loading sample data...")
                success, fail = engine.bulk_insert_from_file(SQL_DATA_FILE)
                
                print(f"\n[+] Sample data loaded!")
                print(f"    Successful: {success}, Failed: {fail}")
            else:
                print("[!] Engine initialization failed")
                sys.exit(1)
        
        elif args.command == 'status':
            # Show database status without loading new data
            print("\n" + "=" * 80)
            print("SQL DATABASE STATUS")
            print("=" * 80)
            
            if engine.initialize():
                stats = engine.get_database_stats()
                print(f"\nDatabase: {DATABASE_URL or 'default (SQLite)'}")
                print(f"\nTable Statistics:")
                total = 0
                for table_name, count in sorted(stats.items()):
                    print(f"  {table_name:<30} {count:>10} records")
                    total += count
                print(f"\nTotal Records: {total}")
            else:
                print("[!] Failed to connect to database")
                sys.exit(1)
        
        else:  # args.command == 'run' (default)
            success, fail = run_sql_pipeline(schema_builder, engine)
            
            if success > 0 or fail == 0:
                sys.exit(0)
            else:
                sys.exit(1)
    
    finally:
        engine.close()


if __name__ == "__main__":
    main()

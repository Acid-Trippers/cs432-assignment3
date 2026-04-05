"""
Verify that ACID tests don't leave behind extra records
Run this after executing tests to check for data leaks
"""

import json
from src.phase_5.sql_engine import SQLEngine
from pymongo import MongoClient
from src.config import MONGO_URI, MONGO_DB_NAME, COUNTER_FILE
from pathlib import Path

print("=" * 70)
print("ACID TEST ISOLATION VERIFICATION")
print("=" * 70)

# Get current state
sql_engine = SQLEngine()
sql_engine.initialize()
session = sql_engine.schema_builder.get_session()

model = sql_engine.models.get("main_records")
sql_count = session.query(model).count() if model else 0
session.close()

mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
mongo_db = mongo_client[MONGO_DB_NAME]
mongo_count = mongo_db["main_records"].count_documents({})
mongo_client.close()

counter = 0
if Path(COUNTER_FILE).exists():
    with open(COUNTER_FILE, 'r') as f:
        counter = int(f.read().strip() or 0)

print(f"\nCurrent System State:")
print(f"  SQL records:     {sql_count}")
print(f"  MongoDB records: {mongo_count}")
print(f"  Counter:         {counter}")

print(f"\nExpected State After Clean Initialization (1000 records):")
print(f"  SQL records:     1000")
print(f"  MongoDB records: 1000 (approx)")
print(f"  Counter:         1000")

# Check for leaks
sql_leak = sql_count - counter
mongo_leak = mongo_count - counter

print(f"\n{'='*70}")
if sql_leak > 10 or mongo_leak > 10:
    print(f"⚠️  DATA LEAK DETECTED!")
    print(f"  SQL leak:   {sql_leak} extra records")
    print(f"  Mongo leak: {mongo_leak} extra records")
    print(f"\nTests may not be cleaning up properly.")
    print(f"Run: python cleanup_empty_records.py --auto")
elif sql_leak == 0 and mongo_leak == 0:
    print(f"✓ PASS: No data leaks detected")
    print(f"  ACID tests are properly isolated")
else:
    print(f"⚠️  MINOR LEAK: {sql_leak + mongo_leak} total extra records")
    print(f"  Within acceptable range (< 10 total)")

print("=" * 70)

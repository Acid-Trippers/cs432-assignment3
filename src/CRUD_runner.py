import json
from .config import DATA_DIR
from .sql_engine import SQLEngine
from pymongo import MongoClient
import os

# Initialize SQL Engine gracefully (non-blocking if PostgreSQL unavailable)
sql_engine = SQLEngine()
try:
    sql_engine.initialize()
    sql_available = True
except Exception as e:
    print(f"[WARNING] SQL Engine initialization failed: {str(e)[:100]}...")
    print("[WARNING] Continuing with MongoDB and Unknown data sources only")
    sql_available = False

mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
db_name = os.getenv("MONGO_DB_NAME", "cs432_db")
mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=2000)
mongo_db = mongo_client[db_name]

def read_operation(parsed_query, db_analysis):
    """
    Read records using two-phase approach:
    Phase 1: Find all matching record_ids based on filters
    Phase 2: Fetch full records using those record_ids
    """
    print(f"\n{'='*60}")
    print("[READ OPERATION - TWO PHASE APPROACH]")
    print(f"{'='*60}")
    
    entity = parsed_query.get("entity")
    filters = parsed_query.get("filters", {})
    databases_needed = db_analysis.get("databases_needed", [])
    field_locations = db_analysis.get("field_locations", {})
    
    print(f"\n[DEBUG] Entity: {entity}, Filters: {filters}, Databases: {databases_needed}")
    
    # ============================================================================
    # PHASE 1: FIND record_ids matching filters
    # ============================================================================
    print(f"\n{'─'*60}")
    print("[PHASE 1] Finding matching record_ids...")
    print(f"{'─'*60}")
    
    matching_record_ids = {}
    
    # Query SQL for matching record_ids
    if "SQL" in databases_needed and sql_available:
        try:
            Model = sql_engine.models.get(entity)
            if Model:
                query = sql_engine.session.query(Model.record_id)
                
                # Apply SQL filters
                sql_filters = {k: v for k, v in filters.items() if field_locations.get(k) == "SQL"}
                if sql_filters:
                    for field_name, field_value in sql_filters.items():
                        query = query.filter(getattr(Model, field_name) == field_value)
                        print(f"[SQL Filter] {field_name} = {field_value}")
                
                record_ids = [rid[0] for rid in query.all()]
                matching_record_ids["SQL"] = record_ids
                print(f"[SQL] Found {len(record_ids)} matching record_ids: {record_ids[:5]}{'...' if len(record_ids) > 5 else ''}")
        except Exception as e:
            print(f"[SQL] Error in Phase 1: {e}")
            matching_record_ids["SQL"] = []
    elif "SQL" in databases_needed:
        print(f"[SQL] Skipped (SQL Engine not available)")
        matching_record_ids["SQL"] = []
    
    # Query MongoDB for matching record_ids
    if "MongoDB" in databases_needed:
        try:
            collection = mongo_db[entity]
            mongo_filters = {k: v for k, v in filters.items() if field_locations.get(k) == "MongoDB"}
            
            projection = {"record_id": 1}
            docs = list(collection.find(mongo_filters, projection))
            record_ids = [doc.get("record_id") for doc in docs if "record_id" in doc]
            matching_record_ids["MongoDB"] = record_ids
            print(f"[MongoDB] Found {len(record_ids)} matching record_ids: {record_ids[:5]}{'...' if len(record_ids) > 5 else ''}")
        except Exception as e:
            print(f"[MongoDB] Error in Phase 1: {e}")
            matching_record_ids["MongoDB"] = []
    
    # Query Unknown for matching record_ids
    if "Unknown" in databases_needed:
        try:
            unknown_file = os.path.join(DATA_DIR, "unknown_data.json")
            if os.path.exists(unknown_file):
                with open(unknown_file, 'r') as f:
                    data = json.load(f)
                
                unknown_filters = {k: v for k, v in filters.items() if field_locations.get(k) == "Unknown"}
                all_records = data if isinstance(data, list) else [data]
                
                if unknown_filters:
                    matching = [r for r in all_records if all(r.get(k) == v for k, v in unknown_filters.items())]
                else:
                    matching = all_records
                
                record_ids = [r.get("record_id") for r in matching if "record_id" in r]
                matching_record_ids["Unknown"] = record_ids
                print(f"[Unknown] Found {len(record_ids)} matching record_ids: {record_ids[:5]}{'...' if len(record_ids) > 5 else ''}")
        except Exception as e:
            print(f"[Unknown] Error in Phase 1: {e}")
            matching_record_ids["Unknown"] = []
    
    # ============================================================================
    # PHASE 2: FETCH full records using record_ids
    # ============================================================================
    print(f"\n{'─'*60}")
    print("[PHASE 2] Fetching full records by record_id...")
    print(f"{'─'*60}")
    
    results = {}
    
    # Fetch from SQL
    if "SQL" in databases_needed and sql_available and matching_record_ids.get("SQL"):
        try:
            Model = sql_engine.models.get(entity)
            if Model:
                records = sql_engine.session.query(Model).filter(
                    Model.record_id.in_(matching_record_ids["SQL"])
                ).all()
                
                from sqlalchemy import inspect as sql_inspect
                results["SQL"] = [
                    {col.name: getattr(record, col.name) for col in sql_inspect(Model).columns}
                    for record in records
                ]
                print(f"[SQL] Fetched {len(results['SQL'])} full records")
        except Exception as e:
            print(f"[SQL] Error in Phase 2: {e}")
            results["SQL"] = []
    else:
        results["SQL"] = []
    
    # Fetch from MongoDB
    if "MongoDB" in databases_needed and matching_record_ids.get("MongoDB"):
        try:
            collection = mongo_db[entity]
            records = list(collection.find({"record_id": {"$in": matching_record_ids["MongoDB"]}}))
            results["MongoDB"] = [
                {"_id": str(r["_id"]), **{k: v for k, v in r.items() if k != "_id"}}
                for r in records
            ]
            print(f"[MongoDB] Fetched {len(results['MongoDB'])} full documents")
        except Exception as e:
            print(f"[MongoDB] Error in Phase 2: {e}")
            results["MongoDB"] = []
    else:
        results["MongoDB"] = []
    
    # Fetch from Unknown
    if "Unknown" in databases_needed and matching_record_ids.get("Unknown"):
        try:
            unknown_file = os.path.join(DATA_DIR, "unknown_data.json")
            if os.path.exists(unknown_file):
                with open(unknown_file, 'r') as f:
                    data = json.load(f)
                
                all_records = data if isinstance(data, list) else [data]
                results["Unknown"] = [
                    r for r in all_records if r.get("record_id") in matching_record_ids["Unknown"]
                ]
                print(f"[Unknown] Fetched {len(results['Unknown'])} full records")
        except Exception as e:
            print(f"[Unknown] Error in Phase 2: {e}")
            results["Unknown"] = []
    else:
        results["Unknown"] = []
    
    print(f"\n{'='*60}")
    print("[SUMMARY] Total records fetched:")
    print(f"  SQL: {len(results.get('SQL', []))}")
    print(f"  MongoDB: {len(results.get('MongoDB', []))}")
    print(f"  Unknown: {len(results.get('Unknown', []))}")
    print(f"{'='*60}\n")
    
    return {"operation": "READ", "entity": entity, "results": results}
    
def create_operation(parsed_query, db_analysis):
    """Create new records."""
    print(f"\n{'='*60}")
    print("[CREATE OPERATION]")
    print(f"{'='*60}")
    return {"operation": "CREATE", "status": "not_implemented"}
    
def update_operation(parsed_query, db_analysis):
    """Update records."""
    print(f"\n{'='*60}")
    print("[UPDATE OPERATION]")
    print(f"{'='*60}")
    return {"operation": "UPDATE", "status": "not_implemented"} 
    
def delete_operation(parsed_query, db_analysis):
    """Delete records."""
    print(f"\n{'='*60}")
    print("[DELETE OPERATION]")
    print(f"{'='*60}")
    return {"operation": "DELETE", "status": "not_implemented"}
    
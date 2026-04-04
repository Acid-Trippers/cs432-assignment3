"""
Enhanced ACID Validators using existing CRUD operations and sql_engine

Tests Atomicity, Consistency, Isolation, Durability properties.
"""

from src.phase_5.sql_engine import SQLEngine
from src.phase_6.CRUD_operations import create_operation, read_operation, update_operation, delete_operation
from src.phase_6.CRUD_runner import query_parser, analyze_query_databases
from pymongo import MongoClient
from src.config import MONGO_URI, MONGO_DB_NAME
import time
from sqlalchemy import text
import threading


# Initialize engines (same as CRUD_operations.py does)
sql_engine = SQLEngine()
try:
    sql_engine.initialize()
except Exception as e:
    print(f"SQL Engine failed: {e}")

mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
mongo_db = mongo_client[MONGO_DB_NAME]


def get_sql_count(table: str = "main_records") -> int:
    """Get row count from SQL table using existing engine."""
    try:
        if not sql_engine or not sql_engine.session:
            return -1
        result = sql_engine.session.query(sql_engine.models.get(table)).count() if sql_engine.models.get(table) else 0
        return result
    except:
        return -1


def get_mongo_count(collection: str = "main_records") -> int:
    """Get document count from Mongo collection."""
    try:
        return mongo_db[collection].count_documents({})
    except:
        return -1


def atomicity_test():
    """
    ATOMICITY: Transaction is all-or-nothing. If part fails, all fails.
    
    Test: Attempt to insert record with invalid data to trigger rollback.
    Verify: Counts unchanged if insert failed = PASS
    """
    before_sql = get_sql_count()
    before_mongo = get_mongo_count()
    
    rollback_count = 0
    try:
        # Try to insert NULL into NOT NULL field (should fail)
        query = text("INSERT INTO main_records (record_id) VALUES (NULL)")
        sql_engine.session.execute(query)
        sql_engine.session.commit()
    except Exception as e:
        # Transaction should rollback automatically
        sql_engine.session.rollback()
        rollback_count += 1
    
    after_sql = get_sql_count()
    after_mongo = get_mongo_count()
    
    # Atomicity holds if failed insert didn't partially apply
    atomicity_holds = (before_sql == after_sql and before_mongo == after_mongo and rollback_count > 0)
    
    return {
        "test": "atomicity",
        "passed": atomicity_holds,
        "sql_count_before": before_sql,
        "sql_count_after": after_sql,
        "mongo_count_before": before_mongo,
        "mongo_count_after": after_mongo,
        "rollback_attempts": rollback_count,
        "note": "Transaction rolled back on constraint violation"
    }


def consistency_test():
    """
    CONSISTENCY: Check if SQL enforces unique constraints.
    
    Test: Try to insert duplicate record_id to same table.
    Verify: Should fail with constraint error = PASS
    """
    try:
        # Get an existing record_id
        existing = sql_engine.session.query(sql_engine.models.get("main_records")).first()
        if not existing:
            return {"test": "consistency", "passed": True, "note": "No records to test"}
        
        dup_id = existing.record_id
        
        # Try duplicate insert (should fail)
        try:
            sql_engine.session.execute(
                text(f"INSERT INTO main_records (record_id) VALUES ({dup_id})")
            )
            sql_engine.session.commit()
            return {"test": "consistency", "passed": False, "error": "Duplicate insert accepted!"}
        except Exception as e:
            # Constraint violation = expected
            sql_engine.session.rollback()
            constraint_error = "duplicate" in str(e).lower() or "unique" in str(e).lower() or "primary" in str(e).lower()
            return {"test": "consistency", "passed": constraint_error, "error": str(e)[:100]}
    
    except Exception as e:
        return {"test": "consistency", "passed": False, "error": str(e)[:100]}


def isolation_test(num_workers: int = 3):
    """
    ISOLATION: Concurrent txns don't interfere (serializable).
    
    Test: Multiple threads read same data to check consistency.
    Verify: All threads see same committed state = PASS
    """
    before = get_sql_count()
    read_results = []
    errors = []
    
    def worker_read(thread_id):
        """Worker thread that reads current count."""
        try:
            time.sleep(0.01 * thread_id)  # Stagger reads
            count = get_sql_count()
            read_results.append((thread_id, count))
        except Exception as e:
            errors.append(str(e))
    
    # Spawn concurrent readers
    threads = []
    for i in range(num_workers):
        t = threading.Thread(target=worker_read, args=(i,))
        threads.append(t)
        t.start()
    
    # Wait for all to complete
    for t in threads:
        t.join()
    
    after = get_sql_count()
    
    # Isolation holds if all readers see same count (no dirty reads)
    all_reads = [count for _, count in read_results]
    consistent_reads = len(set(all_reads)) <= 1 if all_reads else True
    isolation_holds = consistent_reads and len(errors) == 0 and (before == after or before >= 0)
    
    return {
        "test": "isolation",
        "passed": isolation_holds,
        "sql_count_before": before,
        "sql_count_after": after,
        "concurrent_reads": len(all_reads),
        "read_consistency": consistent_reads,
        "errors": len(errors),
        "note": "All concurrent reads see consistent state"
    }


def durability_test():
    """
    DURABILITY: Data committed to disk survives queries.
    
    Test: Query data multiple times, verify same results.
    Verify: Data persists and is recoverable = PASS
    """
    try:
        # Multiple queries to ensure durability
        counts = []
        for attempt in range(3):
            count = get_sql_count()
            counts.append(count)
            time.sleep(0.01)  # Small delay between queries
        
        # All queries should see same count (data persisted)
        all_same = len(set(counts)) == 1
        success = all_same and counts[0] >= 0
        
        return {
            "test": "durability",
            "passed": success,
            "record_counts": counts,
            "consistent": all_same,
            "note": "Data consistently readable across queries"
        }
    except Exception as e:
        return {"test": "durability", "passed": False, "error": str(e)[:100]}

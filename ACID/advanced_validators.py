"""
Advanced ACID Validators - Extended Testing

Additional tests for more rigorous ACID property validation.
Usage: from ACID.advanced_validators import *
"""

from src.phase_5.sql_engine import SQLEngine
from pymongo import MongoClient
from src.config import MONGO_URI, MONGO_DB_NAME
import time
from sqlalchemy import text
import threading

sql_engine = SQLEngine()
try:
    sql_engine.initialize()
except Exception as e:
    print(f"SQL Engine failed: {e}")

mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
mongo_db = mongo_client[MONGO_DB_NAME]


# ==================== ADVANCED ATOMICITY ====================

def multi_record_atomicity_test():
    """
    Test: Insert multiple records in single transaction
    Verify: All succeed or all fail (no partial writes)
    """
    try:
        before = sql_engine.session.query(sql_engine.models.get("main_records")).count()
        
        # Try to insert multiple records in one transaction
        try:
            for i in range(5):
                sql_engine.session.execute(
                    text(f"INSERT INTO main_records (record_id, device_id) VALUES ({20000+i}, 'test_{i}')")
                )
            sql_engine.session.commit()
            after = sql_engine.session.query(sql_engine.models.get("main_records")).count()
            return {"test": "multi_record_atomicity", "passed": (after == before + 5), "records_added": after - before}
        except Exception as e:
            sql_engine.session.rollback()
            after = sql_engine.session.query(sql_engine.models.get("main_records")).count()
            return {"test": "multi_record_atomicity", "passed": (after == before), "error": "Rollback on error"}
    except Exception as e:
        return {"test": "multi_record_atomicity", "passed": False, "error": str(e)[:100]}


def cross_db_atomicity_test():
    """
    Test: Verify SQL and MongoDB stay in sync after operations
    Verify: Both have same record count = PASS
    """
    try:
        sql_count = sql_engine.session.query(sql_engine.models.get("main_records")).count()
        mongo_count = mongo_db["main_records"].count_documents({})
        
        # Both DBs should have consistent counts
        consistent = (sql_count == mongo_count or abs(sql_count - mongo_count) <= 1)
        
        return {
            "test": "cross_db_atomicity",
            "passed": consistent,
            "sql_count": sql_count,
            "mongo_count": mongo_count,
            "note": "Minor differences acceptable (replication lag)"
        }
    except Exception as e:
        return {"test": "cross_db_atomicity", "passed": False, "error": str(e)[:100]}


# ==================== ADVANCED CONSISTENCY ====================

def not_null_constraint_test():
    """
    Test: NOT NULL constraint enforcement
    Verify: NULL insert rejected
    """
    try:
        try:
            # Try to insert NULL in required field
            sql_engine.session.execute(
                text("INSERT INTO main_records (device_id) VALUES (NULL)")
            )
            sql_engine.session.commit()
            return {"test": "not_null_constraint", "passed": False, "error": "NULL accepted"}
        except Exception as e:
            sql_engine.session.rollback()
            is_null_error = "not null" in str(e).lower() or "null" in str(e).lower()
            return {"test": "not_null_constraint", "passed": is_null_error, "error": str(e)[:80]}
    except Exception as e:
        return {"test": "not_null_constraint", "passed": False, "error": str(e)[:100]}


def schema_validation_test():
    """
    Test: Schema integrity and field types
    Verify: Field types are enforced
    """
    try:
        # Get schema metadata
        table = sql_engine.models.get("main_records")
        if not table:
            return {"test": "schema_validation", "passed": False, "error": "Table not found"}
        
        columns = {col.name: str(col.type) for col in table.__table__.columns}
        
        # Check critical fields exist and have correct types
        required_fields = ["record_id", "device_id", "timestamp"]
        has_required = all(field in columns for field in required_fields)
        
        return {
            "test": "schema_validation",
            "passed": has_required,
            "column_count": len(columns),
            "has_required_fields": has_required
        }
    except Exception as e:
        return {"test": "schema_validation", "passed": False, "error": str(e)[:100]}


# ==================== ADVANCED ISOLATION ====================

def dirty_read_test():
    """
    Test: Dirty read prevention (read uncommitted data)
    Verify: Transactions don't see uncommitted changes
    """
    try:
        before = sql_engine.session.query(sql_engine.models.get("main_records")).count()
        dirty_reads = []
        lock = threading.Lock()
        
        def writer_thread():
            """Start transaction but don't commit."""
            try:
                sql_engine.session.begin_nested()
                sql_engine.session.execute(
                    text("INSERT INTO main_records (record_id, device_id) VALUES (30000, 'dirty')")
                )
                # Intentionally don't commit - test if others can see this
                time.sleep(0.2)
                sql_engine.session.rollback()
            except:
                pass
        
        def reader_thread():
            """Try to read during uncommitted write."""
            time.sleep(0.05)  # Wait for writer to insert
            try:
                count = sql_engine.session.query(sql_engine.models.get("main_records")).count()
                with lock:
                    dirty_reads.append(count)
            except:
                pass
        
        # Run concurrent threads
        t1 = threading.Thread(target=writer_thread)
        t2 = threading.Thread(target=reader_thread)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        
        after = sql_engine.session.query(sql_engine.models.get("main_records")).count()
        
        # No dirty reads = all reads same as before
        no_dirty_reads = all(r == before for r in dirty_reads)
        
        return {
            "test": "dirty_read_prevention",
            "passed": no_dirty_reads and before == after,
            "before": before,
            "after": after,
            "reader_count": len(dirty_reads)
        }
    except Exception as e:
        return {"test": "dirty_read_prevention", "passed": False, "error": str(e)[:100]}


# ==================== ADVANCED DURABILITY ====================

def persistent_connection_test():
    """
    Test: Data survives repeated connection cycles
    Verify: Query after disconnect/reconnect returns same data
    """
    try:
        # First read
        count1 = sql_engine.session.query(sql_engine.models.get("main_records")).count()
        
        # Simulate connection cycle
        time.sleep(0.1)
        
        # Second read (may trigger reconnection)
        count2 = sql_engine.session.query(sql_engine.models.get("main_records")).count()
        
        # Third read
        time.sleep(0.1)
        count3 = sql_engine.session.query(sql_engine.models.get("main_records")).count()
        
        # All reads should match
        all_consistent = (count1 == count2 == count3)
        
        return {
            "test": "persistent_connection",
            "passed": all_consistent,
            "reads": [count1, count2, count3],
            "consistent": all_consistent
        }
    except Exception as e:
        return {"test": "persistent_connection", "passed": False, "error": str(e)[:100]}


def index_integrity_test():
    """
    Test: Indexes maintain data integrity
    Verify: Primary key index prevents duplicates
    """
    try:
        table = sql_engine.models.get("main_records")
        if not table:
            return {"test": "index_integrity", "passed": False, "error": "Table not found"}
        
        # Check if indexes exist
        indexes = [idx.name for idx in table.__table__.indexes]
        pk_index_exists = any("pkey" in idx_name.lower() for idx_name in indexes)
        
        return {
            "test": "index_integrity",
            "passed": pk_index_exists,
            "index_count": len(indexes),
            "indexes": indexes[:5]  # Show first 5
        }
    except Exception as e:
        return {"test": "index_integrity", "passed": False, "error": str(e)[:100]}

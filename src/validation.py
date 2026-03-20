import json
import os
from config import INITIAL_SCHEMA_FILE, ANALYZED_SCHEMA_FILE, METADATA_FILE

def merge_metadata():
    if not os.path.exists(INITIAL_SCHEMA_FILE):
        print(f"[!] {INITIAL_SCHEMA_FILE} not found.")
        return
    if not os.path.exists(ANALYZED_SCHEMA_FILE):
        print(f"[!] {ANALYZED_SCHEMA_FILE} not found.")
        return

    with open(INITIAL_SCHEMA_FILE, 'r') as f:
        initial_schema = json.load(f)
    
    with open(ANALYZED_SCHEMA_FILE, 'r') as f:
        analyzed_schema = json.load(f)

    # Flatten initial_schema to match analyzed_schema's field-based list
    # Analyzed schema uses dot notation for paths (e.g. "socials.twitter")
    user_constraints = {}
    
    def flatten_schema(schema_node, path=""):
        if isinstance(schema_node, dict):
            if path:
                user_constraints[path] = {"user_type": "object", "is_required": True}
            for k, v in schema_node.items():
                new_path = f"{path}.{k}" if path else k
                flatten_schema(v, new_path)
        elif isinstance(schema_node, list) and schema_node:
            # Array itself
            if path:
                user_constraints[path] = {"user_type": "array", "is_required": True}
            # Array template
            new_path = f"{path}[]"
            flatten_schema(schema_node[0], new_path)
        else:
            # Leaf node (primitive type)
            if path:
                user_constraints[path] = {
                    "user_type": schema_node,
                    "is_required": True
                }

    flatten_schema(initial_schema)

    # Merge into analyzed_schema fields
    for field in analyzed_schema.get('fields', []):
        f_name = field['field_name']
        if f_name in user_constraints:
            field['user_constraints'] = user_constraints[f_name]
        else:
            field['user_constraints'] = None
            # If it's in analyzed but not in initial, it was likely ripped to buffer
            # but cleaner.py might have let it through if it fuzzy matched?
            # Actually, cleaner.py pads missing fields and rips extra fields.
            # So everything in cleaned_data (and thus analyzed_schema) SHOULD be in initial_schema.

    with open(METADATA_FILE, 'w') as f:
        json.dump(analyzed_schema, f, indent=4)
    
    print(f"\n[SUCCESS] Pipeline Phase 3c Complete.")
    print(f"[+] Consolidated Metadata: {METADATA_FILE}")

if __name__ == "__main__":
    merge_metadata()

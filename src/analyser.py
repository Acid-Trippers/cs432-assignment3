import re
import os
import json
from typing import Dict, Any, List
from collections import defaultdict
from config import DATA_DIR, NORMALIZED_DATA_FILE, ANALYZED_DATA_FILE


class DataAnalyzer:
    def __init__(self):
        self.total_records = 0
        self.field_counts = defaultdict(int)
        self.field_types = defaultdict(lambda: defaultdict(int))
        self.field_values = defaultdict(set)
        self.value_count_limit = 10000
        self.nested_fields = set()
        self.array_fields = set()

    def _get_type_name(self, value: Any) -> str:
        if value is None: return 'null'
        if isinstance(value, bool): return 'boolean'
        if isinstance(value, int): return 'integer'
        if isinstance(value, float): return 'float'
        if isinstance(value, str): return 'string'
        if isinstance(value, list): return 'array'
        if isinstance(value, dict): return 'object'
        return type(value).__name__

    def _detect_pattern(self, value: Any) -> str:
        if not isinstance(value, str): return 'none'
        if re.search(r'\d', value) and re.search(r'[.\-_]', value):
            mask = re.sub(r'\d+', 'd', value)
            return f"{mask}"
        return 'none'

    def _analyze_value(self, field_name: str, value: Any):
        self.field_counts[field_name] += 1
        type_name = self._get_type_name(value)
        self.field_types[field_name][type_name] += 1
        if isinstance(value, dict):
            self.nested_fields.add(field_name)
        elif isinstance(value, list):
            self.array_fields.add(field_name)
        else:
            if len(self.field_values[field_name]) < self.value_count_limit:
                self.field_values[field_name].add(str(value))
            if isinstance(value, str):
                pattern = self._detect_pattern(value)

    def analyze_records(self, records: List[Dict]):
        for record in records:
            self.total_records += 1
            for field_name, value in record.items():
                self._analyze_value(field_name, value)

    def save_analysis(self, output_file: str = ANALYZED_DATA_FILE):
        if not os.path.exists(DATA_DIR):
            os.makedirs(DATA_DIR)

        fields_summary = []
        for f in sorted(self.field_counts.keys()):
            count = self.field_counts[f]
            freq = count / self.total_records
            type_counts = self.field_types[f]
            dom_type, type_val = max(type_counts.items(), key=lambda x: x[1])
            if dom_type == "string" and self.field_values[f]:
            # Get one sample value from our stored set to check the pattern
                sample_value = next(iter(self.field_values[f]))
                pattern = self._detect_pattern(sample_value)
            
            # If it matches your mask logic (e.g., 'pattern_d.d.d.d'), use it as the type
                if pattern != 'none':
                    dom_type = pattern
            stability = type_val / sum(type_counts.values())
            cardinality = len(self.field_values[f]) / count if count > 0 else 0
            
            fields_summary.append({
                'field_name': f,
                'frequency': freq,
                'dominant_type': dom_type,
                'type_stability': stability,
                'cardinality': cardinality,
                'is_nested': f in self.nested_fields,
                'is_array': f in self.array_fields,
            })

        summary = {
            'total_records': self.total_records,
            'fields': fields_summary
        }
        with open(output_file, 'w') as f:
            json.dump(summary, f, indent=4)
        print(f"Analysis saved to {output_file}")
        return summary


    
def run_data_analysis():
    INPUT_FILE = NORMALIZED_DATA_FILE
    ANALYSIS_FILE = ANALYZED_DATA_FILE
    
    if os.path.exists(INPUT_FILE):
        with open(INPUT_FILE, 'r') as f:
            data = json.load(f)
        
        # 1. Run the Analyzer (No changes to logic)
        analyzer = DataAnalyzer()
        analyzer.analyze_records(data)
        analysis_summary = analyzer.save_analysis(ANALYSIS_FILE)

        # 2. Extract batch info for TimestampManager
        latest_ts = "unknown"
        for rec in data:
            if 'timestamp' in rec:
                if latest_ts == "unknown" or rec['timestamp'] > latest_ts:
                    latest_ts = rec['timestamp']

        print(f"Pipeline State Updated: {len(data)} records analyzed and logged in history.")
    else:
        print(f"No data found at {INPUT_FILE}. Run client.py first.")

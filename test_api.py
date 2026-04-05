import requests
import json

print("Checking API responses after reset...")
print("=" * 60)

try:
    # Assuming dashboard is running on localhost:5000
    base_url = "http://localhost:5000"
    
    response = requests.get(f"{base_url}/api/pipeline/stats", timeout=5)
    if response.status_code == 200:
        data = response.json()
        print("API Response:")
        print(json.dumps(data, indent=2))
    else:
        print(f"API Error: {response.status_code}")
except Exception as e:
    print(f"Could not connect to API: {e}")
    print("\nMake sure the dashboard server is running:")
    print("  python dashboard/run.py")

print("\n" + "=" * 60)

import requests
import json
import csv

# API endpoint - removed the search filter to get all records
url = 'https://catalog.disaster.go.th/api/3/action/datastore_search?resource_id=0eef0c17-a771-4c74-9599-c1852631b3c4&limit=1000'

# Step 1: Fetch data from API
response = requests.get(url)
response.raise_for_status()

# Step 2: Parse JSON response
data = response.json()

# Step 3: Extract records
if data['success']:
    records = data['result']['records']
    total = data['result']['total']
    
    print(f"Total records available: {total}")
    print(f"Records fetched: {len(records)}")
    
    if len(records) > 0:
        # Step 4: Write to CSV
        csv_filename = "disaster_data.csv"
        
        # Get all field names from the first record
        fieldnames = list(records[0].keys())
        
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            # Write all records
            for record in records:
                writer.writerow(record)
        
        print(f"\nData saved to {csv_filename}")
        print(f"Columns: {', '.join(fieldnames)}")
        print(f"\nFirst record preview:")
        print(json.dumps(records[0], indent=2, ensure_ascii=False))
    else:
        print("No records found")
else:
    print("API request failed")
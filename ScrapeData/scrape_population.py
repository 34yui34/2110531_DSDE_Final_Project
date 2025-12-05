import urllib.request
import json
import pandas as pd

# -----------------------------------------
# 1. API endpoint (your query)
# -----------------------------------------
url = "https://opend.data.go.th/get-ckan/datastore_search?resource_id=0e77f0fd-17c7-4bd3-ba53-1fc9c46a8c3e&q"

# -----------------------------------------
# 2. Add your API key
# -----------------------------------------
headers = {
    "api-key": "H6yAVDOXkPBtknNIzhlLfquhJClbkujX"
}

request = urllib.request.Request(url, headers=headers)

# -----------------------------------------
# 3. Send request and decode JSON
# -----------------------------------------
with urllib.request.urlopen(request) as response:
    raw_data = response.read().decode("utf-8")

json_data = json.loads(raw_data)

# -----------------------------------------
# 4. Extract records (table rows)
# -----------------------------------------
records = json_data["result"]["records"]

# Convert to DataFrame
df = pd.DataFrame(records)

# -----------------------------------------
# 5. Save to CSV
# -----------------------------------------
output_filename = "thai_open_data_output.csv"
df.to_csv(output_filename, index=False, encoding="utf-8-sig")

print(f"Saved CSV file: {output_filename}")
print(df.head())

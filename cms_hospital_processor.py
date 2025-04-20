import os, requests, pandas as pd, json, re
from datetime import datetime

OUTPUT_DIR = "cms_hospital_datasets"
os.makedirs(OUTPUT_DIR, exist_ok=True)

datasets = {
    "Hospital General Information": "https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0/download?format=csv"
   
}

metadata_path = "last_run_metadata.json"
last_run_time = None
if os.path.exists(metadata_path):
    with open(metadata_path, 'r') as meta_file:
        try:
            metadata = json.load(meta_file)
            last_run_iso = metadata.get("last_run")
            if last_run_iso:
                last_run_time = datetime.fromisoformat(last_run_iso)
        except json.JSONDecodeError:
            print("Warning: Could not parse metadata file. Proceeding as first run.")

def to_snake_case(name):
    name = name.strip().lower()
    name = name.replace(' ', '_').replace('/', '_').replace('-', '_')
    name = re.sub(r'[^a-z0-9_]', '_', name)
    name = re.sub(r'_+', '_', name)
    return name

new_metadata = {}
for dataset_name, url in datasets.items():
    print(f"\nProcessing dataset: {dataset_name}")
    needs_download = True
    last_mod_time = None

    if last_run_time:
        try:
            head_resp = requests.head(url)
        except Exception as e:
            head_resp = None
            print(f" - HEAD request failed for {dataset_name}: {e}")
        if head_resp and head_resp.status_code == 200:
            last_mod_str = head_resp.headers.get('Last-Modified')
            if last_mod_str:
                try:
                    last_mod_time = datetime.strptime(last_mod_str, "%a, %d %b %Y %H:%M:%S %Z")
                except Exception as e:
                    print(f" - Could not parse Last-Modified for {dataset_name}: {e}")
                    last_mod_time = None
                if last_mod_time and last_mod_time <= last_run_time:
                    needs_download = False
                    print(f" - Skipping {dataset_name}, no changes since last run.")

    if needs_download:
        print(f" - Downloading {dataset_name}...")
        resp = requests.get(url)
        if resp.status_code != 200:
            print(f" ! Failed to download {dataset_name} (status {resp.status_code})")
            continue
        from io import StringIO
        df = pd.read_csv(StringIO(resp.text))
        df.rename(columns=lambda c: to_snake_case(c), inplace=True)
        file_path = os.path.join(OUTPUT_DIR, to_snake_case(dataset_name) + ".csv")
        df.to_csv(file_path, index=False)
        print(f"   Saved to {file_path}")
    # Record the dataset's last modified time if available, else use current time
    if last_mod_time:
        new_metadata[dataset_name] = last_mod_time.strftime("%Y-%m-%dT%H:%M:%S")

# Update metadata file
new_metadata["last_run"] = datetime.now().isoformat(timespec='seconds')
with open(metadata_path, 'w') as meta_file:
    json.dump(new_metadata, meta_file, indent=4)
print(f"\nUpdated {metadata_path}.")

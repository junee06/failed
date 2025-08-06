import requests
import json
import time
import os
from pathlib import Path

BATCH_SIZE = 4000
INPUT_FILE = "failed_slugs.json"
OUTPUT_DIR = "output_resolved"
FAILED_LOG = "failed_resolved_log.json"
PROGRESS_FILE = "resolved_progress.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

Path(OUTPUT_DIR).mkdir(exist_ok=True)
Path(FAILED_LOG).touch(exist_ok=True)
Path(PROGRESS_FILE).touch(exist_ok=True)

# Load progress
if os.path.exists(PROGRESS_FILE):
    with open(PROGRESS_FILE, "r") as f:
        done_slugs = set(json.load(f))
else:
    done_slugs = set()

# Load failed log
if os.path.exists(FAILED_LOG):
    with open(FAILED_LOG, "r") as f:
        failed_log = json.load(f)
else:
    failed_log = []

# Load input data
with open(INPUT_FILE, "r") as f:
    data = json.load(f)

batch = []
batch_num = len(list(Path(OUTPUT_DIR).glob("*.json"))) + 1

for entry in data:
    slug_url = entry["url"]
    if slug_url in done_slugs:
        continue

    try:
        slug = slug_url.strip("/").split("/")[-1]
        parts = slug.split("-")
        id_part = parts[0]
        title_part = "-".join(parts[1:])
        query = title_part.replace("-", " ")

        # Search for corrected slug
        search_url = f"https://mdl-pi.vercel.app/search/q/{query}"
        search_resp = requests.get(search_url, headers=HEADERS)
        if search_resp.status_code != 200:
            raise Exception(f"Search failed: {search_resp.status_code}")
        search_data = search_resp.json()

        results = search_data.get("results", {}).get("dramas", [])
        match = next((d for d in results if d.get("mdl_id") == f"mdl-{id_part}"), None)

        if not match:
            raise Exception("ID not found in search results")

        corrected_slug = match["slug"]
        final_url = f"https://mdl-pi.vercel.app/id/{corrected_slug}"
        detail_resp = requests.get(final_url, headers=HEADERS)
        if detail_resp.status_code != 200:
            raise Exception(f"Detail fetch failed: {detail_resp.status_code}")

        result_json = detail_resp.json()
        batch.append(result_json)
        done_slugs.add(slug_url)

        print(f"[✓] {slug_url} → {corrected_slug}")

        # Save batch
        if len(batch) >= BATCH_SIZE:
            out_file = f"{OUTPUT_DIR}/resolved_batch_{batch_num}.json"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(batch, f, ensure_ascii=False, indent=2)
            batch_num += 1
            batch = []

        # Update progress
        with open(PROGRESS_FILE, "w") as f:
            json.dump(list(done_slugs), f)

    except Exception as e:
        print(f"[x] Failed: {slug_url} → {str(e)}")
        failed_log.append({
            "url": slug_url,
            "error": str(e)
        })
        with open(FAILED_LOG, "w", encoding="utf-8") as f:
            json.dump(failed_log, f, ensure_ascii=False, indent=2)

    time.sleep(1.5)  # Avoid rate-limiting

# Final save
if batch:
    out_file = f"{OUTPUT_DIR}/resolved_batch_{batch_num}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(batch, f, ensure_ascii=False, indent=2)
with open(PROGRESS_FILE, "w") as f:
    json.dump(list(done_slugs), f)

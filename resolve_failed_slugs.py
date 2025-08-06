import os
import json
import time
import requests

BATCH_SIZE = 4000
MAX_SLUGS = 10000
RETRY_COUNT = 2
RETRY_DELAY = 1

INPUT_FILE = "failed_slugs.json"
DONE_FILE = "done_failed/done_slugs.json"
PROGRESS_FILE = "resolved_progress.json"
FAILED_LOG_FILE = "failed_resolved_log.json"
OUTPUT_DIR = "output_resolved"

os.makedirs("done_failed", exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === Load input slugs ===
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    all_slugs = json.load(f)

# === Load done slugs ===
done_slugs = set()
if os.path.exists(DONE_FILE):
    try:
        with open(DONE_FILE, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if content:
                done_slugs = set(json.loads(content))
            else:
                print(f"{DONE_FILE} is empty.")
    except json.JSONDecodeError:
        print(f"{DONE_FILE} contains invalid JSON.")

# === Load progress ===
start = 0
if os.path.exists(PROGRESS_FILE):
    try:
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            progress_data = json.load(f)
            start = progress_data.get("last_index", 0)
    except Exception:
        print("Invalid or empty progress file, starting from 0.")

end = min(start + BATCH_SIZE, MAX_SLUGS)
batch = all_slugs[start:end]

print(f"Processing slugs {start} to {end}")

results = []
failed_slugs = []

# === Main processing loop ===
for slug in batch:
    if slug in done_slugs:
        continue

    query = slug.replace("-", " ")
    url = f"https://mdl-pi.vercel.app/search/q/{query}"

    success = False
    for attempt in range(1, RETRY_COUNT + 2):
        try:
            res = requests.get(url, timeout=10)
            if res.status_code == 200:
                data = res.json()
                if data:
                    match = next((d for d in data if d.get("slug") == slug), None)
                    if match:
                        results.append(match)
                        done_slugs.add(slug)
                        success = True
                        break
        except Exception as e:
            print(f"Error on attempt {attempt} for {slug}: {e}")
        time.sleep(RETRY_DELAY)

    if not success:
        failed_slugs.append(slug)

# === Save output ===
output_path = f"{OUTPUT_DIR}/batch_{start}-{end}.json"
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2, ensure_ascii=False)

# === Save updated progress ===
with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
    json.dump({"last_index": end}, f)

# === Save failed slugs ===
with open(FAILED_LOG_FILE, "w", encoding="utf-8") as f:
    json.dump(failed_slugs, f, indent=2)

# === Save updated done slugs ===
with open(DONE_FILE, "w", encoding="utf-8") as f:
    json.dump(list(done_slugs), f)

print(f"Batch completed: {len(results)} resolved, {len(failed_slugs)} failed.")

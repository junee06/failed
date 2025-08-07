import json
import requests
import time
import os

BATCH_SIZE = 4000
MAX_RETRIES = 3
DELAY_BETWEEN_RETRIES = 2
DELAY_BETWEEN_REQUESTS = 1

BASE_SEARCH_URL = 'https://mdl-pi.vercel.app/search/q/'
BASE_ID_URL = 'https://mdl-pi.vercel.app/id/'
HEADERS = { 'User-Agent': 'Mozilla/5.0' }

# Load input dramas
with open('drama_ids.json', 'r', encoding='utf-8') as f:
    drama_list = json.load(f)

# Load progress
progress_file = 'progress.json'
if os.path.exists(progress_file):
    with open(progress_file, 'r') as f:
        progress = json.load(f)
else:
    progress = {"batch_index": 0}

batch_index = progress['batch_index']
start = batch_index * BATCH_SIZE
end = start + BATCH_SIZE
current_batch = drama_list[start:end]

print(f"\n🚀 Starting batch {batch_index} | Entries: {start} to {end - 1}")

def normalize_title(title):
    return title.lower().replace(' ', '-')

def request_with_retries(url, retries=MAX_RETRIES):
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            if response.status_code == 200:
                return response.json()
            else:
                print(f'⚠️ Status {response.status_code} on attempt {attempt} for {url}')
        except Exception as e:
            print(f'❌ Exception on attempt {attempt} for {url}: {e}')
        time.sleep(DELAY_BETWEEN_RETRIES)
    return None

results = []
failed_log = []

for entry in current_batch:
    drama_id = entry['id']
    title = entry['title']
    print(f'\n🔍 Searching: "{title}" (ID: {drama_id})')

    search_url = BASE_SEARCH_URL + normalize_title(title)
    search_data = request_with_retries(search_url)

    if not search_data:
        failed_log.append({ "id": drama_id, "title": title, "reason": "search_failed" })
        continue

    dramas = search_data.get('results', {}).get('dramas', [])
    matched = next((d for d in dramas if d.get('mdl_id') == f'mdl-{drama_id}'), None)

    if not matched:
        failed_log.append({ "id": drama_id, "title": title, "reason": "id_not_matched" })
        continue

    slug = matched['slug']
    print(f'✅ Slug matched: {slug}')

    detail_url = BASE_ID_URL + slug
    detail_data = request_with_retries(detail_url)

    if not detail_data:
        failed_log.append({ "id": drama_id, "title": title, "slug": slug, "reason": "details_failed" })
        continue

    results.append(detail_data)
    time.sleep(DELAY_BETWEEN_REQUESTS)

# Save results
with open(f'output_batch_{batch_index}.json', 'w', encoding='utf-8') as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

with open(f'failed_batch_{batch_index}.json', 'w', encoding='utf-8') as f:
    json.dump(failed_log, f, ensure_ascii=False, indent=2)

print(f'\n✅ Batch {batch_index} complete — {len(results)} successes, {len(failed_log)} failed.')

# Update progress
progress['batch_index'] += 1
with open(progress_file, 'w') as f:
    json.dump(progress, f)

"""
FINRA OTC 'blocksSummary' Downloader — Research Version
-------------------------------------------------------
One-time bulk historical download using synchronous paging.
Pulls all monthly partitions (2019–2021) with 5 000-row pages.
Respects FINRA rate limits (≈ 20 calls/min).
"""

import requests
import json
import time
import os
import pandas as pd

# =======================
# CONFIGURATION
# =======================
USER = "API_connection_key" #can be changed depending on account
PASSWORD = "Client_secret"

GROUP = "otcMarket"
DATASET = "blocksSummary"
SAVE_DIR = "/Users/dimeres/Documents/Thesis/Data"

LIMIT = 5000          # FINRA max for sync requests
CALL_DELAY = 6        # seconds between requests (≈15 calls/min)
START_YEAR = 2019
END_YEAR = 2021

# =======================
# AUTHENTICATION
# =======================
def get_access_token(user, password):
    url = "https://ews.fip.finra.org/fip/rest/ews/oauth2/access_token?grant_type=client_credentials"
    r = requests.post(url, auth=(user, password), headers={"Accept": "application/json"})
    r.raise_for_status()
    print("Token obtained")
    return r.json()["access_token"]

# =======================
# PARTITIONS
# =======================
def get_partitions(token):
    url = f"https://api.finra.org/partitions/group/{GROUP}/name/{DATASET}"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    j = r.json()

    parts = []

    # Handle new FINRA partition structure
    if "availablePartitions" in j:
        for item in j["availablePartitions"]:
            if "partitions" in item:
                parts.extend(item["partitions"])
    elif "partitions" in j:
        parts = j["partitions"]
    elif "data" in j:
        parts = [p["monthStartDate"] for p in j["data"]]
    else:
        raise ValueError("Unexpected partitions JSON:\n" + json.dumps(j, indent=2))

    # Filter by year range
    parts = sorted(p for p in parts if str(START_YEAR) <= p[:4] <= str(END_YEAR))
    print(f"Found {len(parts)} monthly partitions ({parts[0]} → {parts[-1]})")
    return parts


# =======================
# MONTHLY DOWNLOAD
# =======================
def download_month(token, month):
    base_url = f"https://api.finra.org/data/group/{GROUP}/name/{DATASET}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    all_rows, offset, page = [], 0, 1
    while True:
        payload = {
            "limit": LIMIT,
            "offset": offset,
            "fields": [
                "MPID","marketParticipantName","atsOtc","monthStartDate",
                "summaryTypeCode","totalTradeCount","totalShareQuantity",
                "averageTradeSize","ATSBlockCount","ATSBlockQuantity",
                "averageBlockSize","ATSBlockBusinessTradePercent","ATSBlockBusinessSharePercent"
            ],
            "compareFilters": [
                {"compareType": "EQUAL", "fieldName": "monthStartDate", "fieldValue": month}
            ]
        }

        r = requests.post(base_url, headers=headers, json=payload, timeout=90)
        if r.status_code != 200:
            print(f"{month} page {page}: {r.status_code} — {r.text[:120]}")
            break

        try:
            data = r.json()
            if isinstance(data, dict):
                data = data.get("data", [])
            elif not isinstance(data, list):
                print(f"Unexpected response type for {month}: {type(data)}")
                data = []
        except Exception as e:
            print(f"Failed to parse JSON for {month}: {e}")
            data = []

        n = len(data)
        print(f"{month} page {page}: {n} rows")
        all_rows.extend(data)

        if n < LIMIT:
            break
        offset += LIMIT
        page += 1
        time.sleep(CALL_DELAY)

    time.sleep(CALL_DELAY)   # pause between months
    return all_rows

# =======================
# MAIN EXECUTION
# =======================
if __name__ == "__main__":
    token = get_access_token(USER, PASSWORD)
    months = get_partitions(token)

    all_data = []
    for m in months:
        print(f"\nDownloading {m} ...")
        rows = download_month(token, m)
        all_data.extend(rows)
        print(f"{m}: {len(rows)} rows added (total {len(all_data)})")

    if all_data:
        df = pd.json_normalize(all_data)
        os.makedirs(SAVE_DIR, exist_ok=True)
        path = os.path.join(SAVE_DIR, f"{GROUP}_{DATASET}_{START_YEAR}_{END_YEAR}.csv")
        df.to_csv(path, index=False)
        print(f"\nSaved {len(df)} total rows to {path}")
    else:
        print("No data collected.")

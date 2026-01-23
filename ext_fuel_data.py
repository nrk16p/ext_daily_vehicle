import requests
import zipfile
import io
import pandas as pd
import time
import random
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
import os
def run_terminus_fetch(date_start: str, date_end: str):
    print(f"🚀 Running Terminus fetch for {date_start} → {date_end}")
    # ==========================
    # ⚙️ CONFIG
    # ==========================
    load_dotenv()

    MONGO_URI = os.getenv("MONGO_URI")
    DB_NAME = "terminus"
    COLLECTION_NAME = "driving_log"

    # Connect MongoDB
    mongo_client = MongoClient(MONGO_URI)
    mongo_collection = mongo_client[DB_NAME][COLLECTION_NAME]

    # ==========================
    # 📅 PARAMETERS
    # ==========================
    vehicle_ids = [
        184,192,202,203,204,205,206,221,222,223,224,228,231,233,234,236,263,316,318,
        319,320,326,328,372,373,374,375,376,377,382,384,393,394,584,585,586,589,599,
        600,601,602,713,930,1072,1073,1074,1075,1085,1086,1088,1089,1091,1096,1100,
        1122,1391,1392,1595,1611,1612,1860,2022,2036,2047,2081,2082,2083,2084,2085,
        2185,2186,2226,2327,2328,2329,2330,2331,2337,2338,2339,2340,2341,2342,2343,
        2345,2346,2347,2350,2351,2357,2358,2692,2694,2700,2837,2855,2868,2888,3034,
        3180,3281,3285,4172,4173,4174,4175,4232,4233,5086,5087,5088,5089,5096,5097,
        5098,5099,5100,5101,5102,5103,5104,5150,5152,5161,5162,5163,5164,5165,5166,
        5167,5168,5172,5173,5174,5175,5176,5177,5179,5180,5190,5191,5192,5193,5194,
        5195,5196,5197,5198,5199,5200,5202,5203,5207,5208,5209,5210,5211,5212,5213,
        5214,5215,5216,5217,5218,5241,5242,5243,5246,5248,5250,5251,5252,5253,5257,
        5258,5259,5261,5262,5263,5264,5273,5301,5306,5309,5310,5348,5349,5350,5351,
        5352,5353,5354,5355,5358,5359,5360,5361,5363,5364,5369,5370,5371,5372,5373,
        5374,5375,5376,5381,5382,5383,5384,5385,5386,5387,5388,5389,5390,5391,5394,
        5395,5398,5399,5400,5401,5402,5403,5404,5430,5455,5472,5473,5474,5475,5476,
        5497,5498,5499,5500,5501,5558,6097,6128,6130,6248,6421,6422,6423,6424,6511,
        6512,6513,8282,8283,8284,8285,8286,8747,8748,8749,8750,8751,8757,8758,8926,
        8927,8928,8929,8930,9943,9944,9945,9946,9947,10093,10094,10095,10156,10175,
        10176,10177,10178,10179,10180,10181,10182,10283,10284,10285,10286,10287,
        10741,10742,10743,10744,10745,10848,10849,10850,10851,10852,10998,10999,
        11000,11001,11002,11150,11151,11152,11153,11154,12128,12129,12131,12423,
        12425,12427,12428,14781,14782,17001,18607,18608,18609,18610,18611,18722,
        18723,18724,18725,18726,23834,23835,23836,23837,23838,23847,23848,23880,
        23881,23882,24319,24441,24443,24445,24446,24447,24448,24449,24450,24451,
        24452,24453,24454,24455,24456,25298,25299,26913,26914,27873,27874,27875,
        37634,39673,39986,39987,40786,40794,43356,43373,43375,43376,44069,44073,
        44840,46068,46069,46070,46071,46072,46094,46095,46096,46114,46121,46122,
        46609,46610,47473,47474,47475,47476,47477,47478,47479,47480,47537,47546,
        47547,48843,49068,49069,49071,49072,49092,49093,49094,49095,49096,49097,
        49142,49143,49144,49145,49146,49147,49148,49149,49150,49151,49256,49257,
        49258,49260,49261,49333,49339,49340,49341,49342,49343,49469,49470,49471,
        49472,49473,49474,49475,49476,49652,49653,49654,49655,49659,49660,49663,
        49665,49666,49667,49677,49678,49682,49683,49684,49685,49772,49773,49774,
        49775,49788,49789,49790,49791,49873,49878,49886,49936,49940,49941,49942,
        49943,49944,49945,49947,50514,50515,50516,50517,50518,50523,50524,50526,
        50527,50528,50529,50530,50531,50532,50534,50535,50536,50537,50538,50539,
        50540,50541,50542,50559,50560,50561,50562,50563,50655,50656,50657,50658,
        50659,50684,50685,50686,50687,50688,50689,50690,50692,50693,50694,50708,
        50709,50710,50711,50712,50713,50714,50715,50716,50717,51344,51345,51346,
        51347,51349,51350,51351,51352,51407,51408,51409,51410,51411,51453,51454,
        51455,51456,51457,51458,51459,51460,51620,51622,51688,51690,51691,51692,
        51693,51694,51801,51802,51803,51804,51814,51815,51832,52776,52791,52792,
        52813,52814,52815,52816,52817,52819,52820,52821,52822,53407,53905,54073,
        55829,58184,58534,58948,59158,59236,59301,59323
    ] 



    url = "https://report-mongo.terminusfleet.com/reports/go/drivinglog2"

    base_payload = {
        "date_start": date_start,
        "date_end": date_end,
        "company_id": 31,
        "vehicle_type_id": "",
        "user_id": 230,
        "vehicle_status": "all",
        "type_file": "zip",
        "vehicle_visibility": "184,192,202,203,204,205,206,221,222,223,224,228,231,233,234,236,263,316,318,319,320",
        "policy_id": [],
        "event_type": [1001, 7007],
        "gps_not_fixed": 1,
    }

    user_agents = [
        # Windows 10/11 - Chrome
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",

        # Windows 11 - Chrome
        "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",

        # Windows - Edge (Chromium)
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0",
        "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0",
        "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0",
        "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0",

        # Windows - Firefox
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:134.0) Gecko/20100101 Firefox/134.0",
        "Mozilla/5.0 (Windows NT 11.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
        "Mozilla/5.0 (Windows NT 11.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",

        # macOS Sonoma 14.x - Chrome
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",

        # macOS - Safari
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",

        # macOS - Edge
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0",

        # macOS - Firefox
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.5; rv:136.0) Gecko/20100101 Firefox/136.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:135.0) Gecko/20100101 Firefox/135.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.3; rv:134.0) Gecko/20100101 Firefox/134.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.2; rv:133.0) Gecko/20100101 Firefox/133.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.1; rv:132.0) Gecko/20100101 Firefox/132.0",

        # Linux - Chrome
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",

        # Linux - Firefox
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:136.0) Gecko/20100101 Firefox/136.0",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:135.0) Gecko/20100101 Firefox/135.0",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:134.0) Gecko/20100101 Firefox/134.0",
        "Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0",
        "Mozilla/5.0 (X11; Linux x86_64; rv:132.0) Gecko/20100101 Firefox/132.0",

        # Linux - Edge
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0",

        # Opera (Windows)
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 OPR/104.0.0.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 OPR/103.0.0.0",
        "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 OPR/102.0.0.0",
        "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 OPR/101.0.0.0",
        "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 OPR/100.0.0.0",

        # Brave (Windows)
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Brave/136.0.0.0",
        "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Brave/135.0.0.0",
        "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Brave/134.0.0.0",
        "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Brave/133.0.0.0",
        "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Brave/132.0.0.0",

        # Brave (macOS)
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Brave/136.0.0.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Brave/135.0.0.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Brave/134.0.0.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Brave/133.0.0.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Brave/132.0.0.0",
    ]

    # ==========================
    # 🔁 RETRY HELPER
    # ==========================
    def retry_request(func, max_retries=5, delay=5, backoff=2):
        for attempt in range(1, max_retries + 1):
            try:
                return func()
            except Exception as e:
                print(f"⚠️ Attempt {attempt}/{max_retries} failed: {e}")
                if attempt < max_retries:
                    sleep_time = delay * (backoff ** (attempt - 1))
                    print(f"⏳ Retrying in {sleep_time:.1f}s...")
                    time.sleep(sleep_time)
                else:
                    print("❌ Max retries reached.")
                    return None

    # ==========================
    # 🚚 MAIN LOOP
    # ==========================
    df_all = []
    failed_ids = []
    start_time = time.time()
    total_vehicles = len(vehicle_ids)

    for idx, vid in enumerate(vehicle_ids, start=1):
        v_start = time.time()
        headers = {
            "User-Agent": random.choice(user_agents),
            "Referer": "https://app-v2.terminusfleet.com/",
            "Accept": "application/zip,application/octet-stream,*/*;q=0.8",
        }

        print(f"\n🚚 [{idx}/{total_vehicles}] Vehicle ID = {vid}")

        payload = base_payload.copy()
        payload["list_vehicle_id"] = [vid]

        # Step 1: request report link
        def get_report_link():
            resp = requests.post(url, json=payload, timeout=180)
            resp.raise_for_status()
            return resp.json().get("result")

        result_link = retry_request(get_report_link)
        if not result_link:
            failed_ids.append(vid)
            print(f"⚠️ Vehicle {vid}: No report link found, skipping.")
            continue

        # Step 2: download zip
        def download_zip():
            r = requests.get(result_link, headers=headers, timeout=300)
            r.raise_for_status()
            return r.content

        zip_content = retry_request(download_zip)
        if not zip_content:
            failed_ids.append(vid)
            print(f"❌ Vehicle {vid}: Download failed after retries.")
            continue

        # Step 3: read file
        try:
            with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
                file_name = z.namelist()[0]
                if file_name.endswith(".csv"):
                    with z.open(file_name) as f:
                        df = pd.read_csv(f, skiprows=4)
                elif file_name.endswith((".xlsx", ".xls")):
                    with z.open(file_name) as f:
                        df = pd.read_excel(f, engine="openpyxl", skiprows=4)
                else:
                    print(f"⚠️ Unsupported file type: {file_name}")
                    continue

            df["vehicle_id"] = vid
            df["date_start"] = date_start
            df["date_end"] = date_end
            df_all.append(df)

            # Insert into MongoDB
            records = df.to_dict(orient="records")
            if records:
                mongo_collection.insert_many(records)
                print(f"📦 Inserted {len(records)} records for vehicle {vid}")

            print(f"✅ Vehicle {vid}: {len(df)} rows")
        except Exception as e:
            print(f"❌ Vehicle {vid}: Parse error -> {e}")
            failed_ids.append(vid)
            continue

        # ETA + small sleep
        v_elapsed = time.time() - v_start
        elapsed_total = time.time() - start_time
        avg_time = elapsed_total / idx
        remaining = avg_time * (total_vehicles - idx)
        print(f"⏱️ Took {v_elapsed:.1f}s | ETA {remaining/60:.1f} min left")
        time.sleep(random.uniform(1.5, 3.5))  # avoid overload

    # ==========================
    # 🧾 SUMMARY
    # ==========================
    if df_all:
        df_final = pd.concat(df_all, ignore_index=True)
        print(f"\n✅ Finished! {df_final.shape[0]} total rows saved to MongoDB.")
    else:
        print("\n⚠️ No data collected.")

    if failed_ids:
        print("\n⚠️ Failed vehicle IDs:")
        print(failed_ids)
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Download and store Terminus driving logs")
    parser.add_argument("--date_start", type=str, default="2025-11-01 00:00")
    parser.add_argument("--date_end", type=str, default="2025-11-01 23:59")
    args = parser.parse_args()
    
    run_terminus_fetch(args.date_start, args.date_end)

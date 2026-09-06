import sys
from pathlib import Path
from datetime import datetime
import shutil
import subprocess
import requests
import py7zr
import time
import threading
import sqlite3
from pathlib import Path
import hashlib
import re
import unicodedata
import json
from collections import deque
from difflib import SequenceMatcher
from email.utils import parsedate_to_datetime

# TODO: TELEGRAM NOTIFICATION IF UNMATCHABLE REGIONS FOR MANUAL FIX
# TODO: ADD 'alias' INTO THE DB SO THAT MANUAL AMENDMENTS ARE STICKY & CAN BE REFERENCED ACROSS DATA

# PATHS
BASE_DIR = Path(__file__).resolve().parent
HOLIDAYS_DB_PATH = BASE_DIR / "holidays.db"
# db = sqlite3.connect(HOLIDAYS_DB_PATH); db.execute("DROP TABLE IF EXISTS regions"); db.commit(); db.close()
show_debug_map = True

import zipfile
# PREPARE GNS + GNIS + US CENSUS SOURCE DATA | GNS FILE AGE ALONE CONTROLS FULL REFRESH
GEO_DIR = BASE_DIR / "GEO"
GNS_EXPIRY_DAYS = 14
COUNTRY_INFO_URL = "https://download.geonames.org/export/dump/countryInfo.txt"
GNS_WORLD_URL = "https://geonames.nga.mil/geonames/GNSData/fc_files/Whole_World.7z"
GNIS_DOMESTIC_URL = "https://prd-tnm.s3.amazonaws.com/StagedProducts/GeographicNames/DomesticNames/DomesticNames_National_Text.zip"
CENSUS_GAZETTEER_BASE_URL = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/"
def prepare_geographic_source_data():
    GEO_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now().date()
    bar_width = 30

    def valid_gns(path):
        if not path or not path.exists() or path.stat().st_size < 1024**3: return False
        try:
            with open(path, "rb") as f: header = f.readline().decode("utf-8-sig", "replace").lower().split("\t")
            required = {"cc_ft", "lat_dd", "long_dd", "desig_cd", "adm1", "full_name", "full_nm_nd", "sort_name", "name_rank"}
            return required.issubset({x.strip() for x in header})
        except OSError:
            return False

    def valid_gnis(path):
        if not path or not path.exists() or path.stat().st_size < 10 * 1024**2: return False
        try:
            with open(path, "rb") as f: header = f.readline().decode("utf-8-sig", "replace").lower()
            return "feature" in header and "prim_lat" in header and "prim_long" in header
        except OSError:
            return False

    def valid_census_states(path):
        if not path or not path.exists() or path.stat().st_size < 1024: return False
        try:
            with open(path, "rb") as f: raw_header = f.readline().decode("utf-8-sig", "replace").strip().lower()
            delimiter = "|" if "|" in raw_header else "\t"
            header = {x.strip() for x in raw_header.split(delimiter)}
            return {"usps", "geoid", "name", "intptlat", "intptlong"}.issubset(header)
        except OSError:
            return False

    # > discover newest Census Gazetteer year that actually contains the national states archive
    def latest_census_states_source():
        try:
            response = requests.get(CENSUS_GAZETTEER_BASE_URL, timeout=30)
            response.raise_for_status()
            years = sorted({int(year) for year in re.findall(r'(\d{4})_Gazetteer/', response.text)}, reverse=True)
        except requests.RequestException as exc:
            raise RuntimeError(f"Could not discover Census Gazetteer versions: {exc}") from exc

        if not years: raise RuntimeError("No Census Gazetteer year directories found")

        for year in years:
            url = f"{CENSUS_GAZETTEER_BASE_URL}{year}_Gazetteer/{year}_Gaz_state_national.zip"
            try:
                with requests.get(url, stream=True, timeout=30) as response:
                    if response.status_code == 200:
                        print(f"CENSUS VERSION     | {year} | LATEST AVAILABLE")
                        return year, url
                    if response.status_code not in {403, 404}: response.raise_for_status()
            except requests.RequestException:
                continue

        raise RuntimeError("No valid Census national states Gazetteer archive found")

    CENSUS_STATES_SOURCE_YEAR, CENSUS_STATES_URL = latest_census_states_source()

    # > latest valid downloaded GNS file is authoritative refresh clock
    gns_files = []
    for path in GEO_DIR.glob("Whole_World_*.txt"):
        try:
            source_date = datetime.strptime(path.stem.replace("Whole_World_", ""), "%Y-%m-%d").date()
            if valid_gns(path): gns_files.append((source_date, path))
            else:
                print(f"GNS                | INVALID CACHE | {path.name} | {path.stat().st_size / 1024**2:.1f} MB")
                path.unlink(missing_ok=True)
        except ValueError:
            pass

    gns_files.sort(reverse=True)

    if gns_files:
        source_date, gns_path = gns_files[0]
        gns_age = (today - source_date).days
        full_refresh = gns_age > GNS_EXPIRY_DAYS
    else:
        source_date = today
        gns_path = None
        gns_age = None
        full_refresh = True

    stamp_date = today if full_refresh else source_date
    stamp = stamp_date.strftime("%Y-%m-%d")
    country_path = GEO_DIR / f"countryInfo_{stamp}.txt"
    gnis_path = GEO_DIR / f"DomesticNames_National_{stamp}.txt"
    census_states_path = GEO_DIR / f"Census_States_{CENSUS_STATES_SOURCE_YEAR}_{stamp}.txt"

    if not full_refresh:
        print(f"GNS                | CACHED | {source_date} | AGE {gns_age} DAYS")
    else:
        print(f"GNS                | {'MISSING/INVALID' if gns_path is None else 'EXPIRED'} | FULL REFRESH")

    # > common streamed requests download
    def download_requests(url, path, label):
        temp_path = path.with_suffix(path.suffix + ".download")
        temp_path.unlink(missing_ok=True)
        print(f"DOWNLOAD           | {label}")

        with requests.get(url, stream=True, timeout=120) as response:
            response.raise_for_status()
            total = int(response.headers.get("Content-Length") or 0)
            done = 0

            with open(temp_path, "wb") as f:
                for chunk in response.iter_content(1024 * 1024):
                    if not chunk: continue
                    f.write(chunk)
                    done += len(chunk)
                    pct = min(done / total, 1) if total else 0
                    filled = int(pct * bar_width)
                    size = f"{done / 1024**2:.1f}/{total / 1024**2:.1f} MB" if total else f"{done / 1024**2:.1f} MB"
                    print(f"\rDOWNLOAD           | [{'#' * filled}{'-' * (bar_width - filled)}] {pct * 100:6.2f}% | {size}", end="", flush=True)

        if not temp_path.exists() or temp_path.stat().st_size == 0: raise RuntimeError(f"{label} download produced an empty file")
        temp_path.replace(path)
        print()

    # > countryInfo refreshes with GNS, or repairs itself if missing
    if full_refresh or not country_path.exists():
        download_requests(COUNTRY_INFO_URL, country_path, "countryInfo.txt")
    else:
        print(f"COUNTRY INFO       | CACHED | {country_path.name}")

    # > GNS only downloads when its own source is missing, invalid or expired
    if full_refresh:
        gns_path = GEO_DIR / f"Whole_World_{stamp}.txt"
        gns_archive = GEO_DIR / "Whole_World.7z"
        gns_extract_dir = GEO_DIR / "_gns_extract"

        gns_path.unlink(missing_ok=True)
        gns_archive.unlink(missing_ok=True)
        shutil.rmtree(gns_extract_dir, ignore_errors=True)

        head_cmd = ["/usr/bin/curl", "-L", "--fail", "--silent", "--show-error", "--head", GNS_WORLD_URL]
        head = subprocess.run(head_cmd, capture_output=True, text=True)

        if head.returncode != 0:
            head_cmd = ["/usr/bin/curl", "-k", "-L", "--fail", "--silent", "--show-error", "--head", GNS_WORLD_URL]
            head = subprocess.run(head_cmd, capture_output=True, text=True)

        gns_total = 0
        for line in head.stdout.splitlines():
            if line.lower().startswith("content-length:"):
                try: gns_total = int(line.split(":", 1)[1].strip())
                except ValueError: pass

        print("DOWNLOAD           | Whole_World.7z")

        for insecure in (False, True):
            gns_archive.unlink(missing_ok=True)
            cmd = ["/usr/bin/curl", "-L", "--fail", "--retry", "3", "--silent", "--show-error", "-o", str(gns_archive), GNS_WORLD_URL]
            if insecure: cmd.insert(1, "-k")

            process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)

            while process.poll() is None:
                done = gns_archive.stat().st_size if gns_archive.exists() else 0
                pct = min(done / gns_total, 1) if gns_total else 0
                filled = int(pct * bar_width)
                size = f"{done / 1024**2:.1f}/{gns_total / 1024**2:.1f} MB" if gns_total else f"{done / 1024**2:.1f} MB"
                print(f"\rDOWNLOAD           | [{'#' * filled}{'-' * (bar_width - filled)}] {pct * 100:6.2f}% | {size}", end="", flush=True)
                time.sleep(0.25)

            error = process.stderr.read().strip()
            if process.returncode == 0: break
            if insecure: raise RuntimeError(f"GNS download failed: {error}")

        if not gns_archive.exists() or gns_archive.stat().st_size == 0: raise RuntimeError("GNS archive download is empty")

        done = gns_archive.stat().st_size
        pct = min(done / gns_total, 1) if gns_total else 1
        filled = int(pct * bar_width)
        size = f"{done / 1024**2:.1f}/{gns_total / 1024**2:.1f} MB" if gns_total else f"{done / 1024**2:.1f} MB"
        print(f"\rDOWNLOAD           | [{'#' * filled}{'-' * (bar_width - filled)}] {pct * 100:6.2f}% | {size}")

        gns_extract_dir.mkdir(parents=True, exist_ok=True)
        print("EXTRACT            | Whole_World.7z")

        with py7zr.SevenZipFile(gns_archive, "r") as archive:
            member = next((name for name in archive.getnames() if Path(name).name == "Whole_World.txt"), None)
            if not member: raise RuntimeError("Whole_World.txt not found in GNS archive")

            total = archive.archiveinfo().uncompressed
            extracted = gns_extract_dir / member
            worker = threading.Thread(target=archive.extractall, kwargs={"path": gns_extract_dir})
            worker.start()

            while worker.is_alive():
                done = extracted.stat().st_size if extracted.exists() else 0
                pct = min(done / total, 1) if total else 0
                filled = int(pct * bar_width)
                print(f"\rEXTRACT            | [{'#' * filled}{'-' * (bar_width - filled)}] {pct * 100:6.2f}% | {done / 1024**3:.2f}/{total / 1024**3:.2f} GB", end="", flush=True)
                time.sleep(0.25)

            worker.join()

        if not extracted.exists(): raise RuntimeError("Whole_World.txt extraction failed")
        extracted.replace(gns_path)

        if not valid_gns(gns_path):
            bad_size = gns_path.stat().st_size / 1024**2 if gns_path.exists() else 0
            gns_path.unlink(missing_ok=True)
            raise RuntimeError(f"Extracted GNS source is invalid | {bad_size:.1f} MB")

        print(f"\rEXTRACT            | [{'#' * bar_width}] 100.00% | {gns_path.stat().st_size / 1024**3:.2f} GB".ljust(120))

        gns_archive.unlink(missing_ok=True)
        shutil.rmtree(gns_extract_dir, ignore_errors=True)

    # > GNIS refreshes with GNS, or repairs itself independently if missing/invalid
    gnis_valid = valid_gnis(gnis_path)

    if full_refresh or not gnis_valid:
        if gnis_path.exists() and not gnis_valid:
            print(f"GNIS               | INVALID CACHE | {gnis_path.name} | {gnis_path.stat().st_size / 1024**2:.1f} MB")
            gnis_path.unlink(missing_ok=True)

        gnis_archive = GEO_DIR / "DomesticNames_National_Text.zip"
        gnis_extract_dir = GEO_DIR / "_gnis_extract"

        gnis_archive.unlink(missing_ok=True)
        shutil.rmtree(gnis_extract_dir, ignore_errors=True)

        download_requests(GNIS_DOMESTIC_URL, gnis_archive, "DomesticNames_National_Text.zip")
        gnis_extract_dir.mkdir(parents=True, exist_ok=True)

        print("EXTRACT            | DomesticNames_National_Text.zip")

        with zipfile.ZipFile(gnis_archive, "r") as archive:
            text_members = [info for info in archive.infolist() if not info.is_dir() and info.filename.lower().endswith(".txt") and "domesticnames" in Path(info.filename).name.casefold()]
            if not text_members: text_members = [info for info in archive.infolist() if not info.is_dir() and info.filename.lower().endswith(".txt")]
            if not text_members: raise RuntimeError("Domestic Names text file not found in GNIS archive")

            member = max(text_members, key=lambda info: info.file_size)
            extracted = gnis_extract_dir / Path(member.filename).name
            total = member.file_size
            done = 0

            with archive.open(member) as source, open(extracted, "wb") as target:
                while True:
                    chunk = source.read(1024 * 1024)
                    if not chunk: break
                    target.write(chunk)
                    done += len(chunk)
                    pct = min(done / total, 1) if total else 0
                    filled = int(pct * bar_width)
                    print(f"\rEXTRACT            | [{'#' * filled}{'-' * (bar_width - filled)}] {pct * 100:6.2f}% | {done / 1024**2:.1f}/{total / 1024**2:.1f} MB", end="", flush=True)

        if not extracted.exists(): raise RuntimeError("GNIS Domestic Names extraction failed")
        extracted.replace(gnis_path)

        if not valid_gnis(gnis_path):
            bad_size = gnis_path.stat().st_size / 1024**2 if gnis_path.exists() else 0
            gnis_path.unlink(missing_ok=True)
            raise RuntimeError(f"Extracted GNIS source is invalid | {bad_size:.1f} MB")

        print(f"\rEXTRACT            | [{'#' * bar_width}] 100.00% | {gnis_path.stat().st_size / 1024**2:.1f} MB".ljust(120))

        gnis_archive.unlink(missing_ok=True)
        shutil.rmtree(gnis_extract_dir, ignore_errors=True)
    else:
        print(f"GNIS               | CACHED | {gnis_path.name}")

    # > Census states independently tracks the newest Gazetteer year that actually publishes a states archive
    census_states_valid = valid_census_states(census_states_path)

    if full_refresh or not census_states_valid:
        if census_states_path.exists() and not census_states_valid:
            print(f"CENSUS STATES      | INVALID CACHE | {census_states_path.name} | {census_states_path.stat().st_size / 1024:.1f} KB")
            census_states_path.unlink(missing_ok=True)

        census_archive = GEO_DIR / f"{CENSUS_STATES_SOURCE_YEAR}_Gaz_state_national.zip"
        census_extract_dir = GEO_DIR / "_census_states_extract"

        census_archive.unlink(missing_ok=True)
        shutil.rmtree(census_extract_dir, ignore_errors=True)

        download_requests(CENSUS_STATES_URL, census_archive, f"{CENSUS_STATES_SOURCE_YEAR}_Gaz_state_national.zip")
        census_extract_dir.mkdir(parents=True, exist_ok=True)

        print(f"EXTRACT            | {CENSUS_STATES_SOURCE_YEAR}_Gaz_state_national.zip")

        with zipfile.ZipFile(census_archive, "r") as archive:
            text_members = [info for info in archive.infolist() if not info.is_dir() and info.filename.lower().endswith(".txt")]
            if not text_members: raise RuntimeError("Census States text file not found in archive")

            member = max(text_members, key=lambda info: info.file_size)
            extracted = census_extract_dir / Path(member.filename).name
            total = member.file_size
            done = 0

            with archive.open(member) as source, open(extracted, "wb") as target:
                while True:
                    chunk = source.read(1024 * 1024)
                    if not chunk: break
                    target.write(chunk)
                    done += len(chunk)
                    pct = min(done / total, 1) if total else 0
                    filled = int(pct * bar_width)
                    print(f"\rEXTRACT            | [{'#' * filled}{'-' * (bar_width - filled)}] {pct * 100:6.2f}% | {done / 1024:.1f}/{total / 1024:.1f} KB", end="", flush=True)

        if not extracted.exists(): raise RuntimeError("Census States extraction failed")
        extracted.replace(census_states_path)

        if not valid_census_states(census_states_path):
            bad_size = census_states_path.stat().st_size / 1024 if census_states_path.exists() else 0
            census_states_path.unlink(missing_ok=True)
            raise RuntimeError(f"Extracted Census States source is invalid | {bad_size:.1f} KB")

        print(f"\rEXTRACT            | [{'#' * bar_width}] 100.00% | {census_states_path.stat().st_size / 1024:.1f} KB".ljust(120))

        census_archive.unlink(missing_ok=True)
        shutil.rmtree(census_extract_dir, ignore_errors=True)

        # > retain only the newest Census states source after a successful version change/repair
        for path in GEO_DIR.glob("Census_States_*.txt"):
            if path != census_states_path: path.unlink()
    else:
        print(f"CENSUS STATES      | CACHED | {census_states_path.name}")

    # > after successful full refresh retain only newest synchronized set
    if full_refresh:
        for path in GEO_DIR.glob("countryInfo_*.txt"):
            if path != country_path: path.unlink()
        for path in GEO_DIR.glob("Whole_World_*.txt"):
            if path != gns_path: path.unlink()
        for path in GEO_DIR.glob("DomesticNames_National_*.txt"):
            if path != gnis_path: path.unlink()
        for path in GEO_DIR.glob("Census_States_*.txt"):
            if path != census_states_path: path.unlink()
        print(f"GEO DATA           | REFRESHED | {stamp}")
    else:
        print(f"GEO DATA           | CACHED | {stamp} | GNS AGE {gns_age} DAYS")

    if not country_path.exists(): raise RuntimeError(f"Country info source missing: {country_path}")
    if not valid_gns(gns_path): raise RuntimeError(f"GNS source invalid: {gns_path}")
    if not valid_gnis(gnis_path): raise RuntimeError(f"GNIS source invalid: {gnis_path}")
    if not valid_census_states(census_states_path): raise RuntimeError(f"Census States source invalid: {census_states_path}")

    print(f"GNS SOURCE         | {gns_path.name} | {gns_path.stat().st_size / 1024**3:.2f} GB")
    print(f"GNIS SOURCE        | {gnis_path.name} | {gnis_path.stat().st_size / 1024**2:.1f} MB")
    print(f"CENSUS STATES      | {census_states_path.name} | {census_states_path.stat().st_size / 1024:.1f} KB")

    return country_path, gns_path, gnis_path, census_states_path, full_refresh
COUNTRY_INFO_PATH, GNS_WORLD_PATH, GNIS_DOMESTIC_PATH, CENSUS_STATES_PATH, GEO_DATA_REFRESHED = prepare_geographic_source_data()
# SYNC DISTINCT DEAL REGIONS INTO REGIONS TABLE
def sync_regions():
    db = sqlite3.connect(HOLIDAYS_DB_PATH)
    db.execute("""
        CREATE TABLE IF NOT EXISTS regions (
            country TEXT NOT NULL COLLATE NOCASE,
            region TEXT NOT NULL COLLATE NOCASE,
            latitude REAL,
            longitude REAL,
            coordinate_source TEXT,
            coordinate_match TEXT,
            coordinate_feature TEXT,
            coordinate_confidence TEXT,
            PRIMARY KEY(country,region)
        )
    """)

    # > migrate existing regions table without losing stored coordinates
    existing_columns = {row[1] for row in db.execute("PRAGMA table_info(regions)").fetchall()}
    for column in ("coordinate_match", "coordinate_feature", "coordinate_confidence"):
        if column not in existing_columns: db.execute(f"ALTER TABLE regions ADD COLUMN {column} TEXT")

    before = db.execute("SELECT COUNT(*) FROM regions").fetchone()[0]
    db.execute("""
        INSERT OR IGNORE INTO regions (country,region)
        SELECT DISTINCT TRIM(country),TRIM(region)
        FROM TSM_data
        WHERE country IS NOT NULL AND TRIM(country)<>''
          AND region IS NOT NULL AND TRIM(region)<>''
    """)
    db.commit()
    after = db.execute("SELECT COUNT(*) FROM regions").fetchone()[0]
    db.close()
    print(f"REGIONS            | {after:,} TOTAL | {after - before:,} NEW")
    return after, after - before
REGION_COUNT, NEW_REGIONS = sync_regions()

# BUILD REUSABLE GNS INDEX + MATCH ALL NON-US REGIONS | INDEX REBUILDS ONLY WHEN GNS SOURCE CHANGES
GNS_INDEX_DB_PATH = GEO_DIR / "gns_index.db"
GNS_INDEX_VERSION = "1"
GNS_REGION_CLUSTER_KM = 50.0
GNS_REGION_FEATURES = {
    "PCLI", "PCLIX", "ADM1", "ADM2", "ADMD", "RGN",
    "ISL", "ISLET", "ISLS", "PEN", "CAPE", "CST", "LK",
    "PPLC", "PPLCD", "PPLA", "PPLA2", "PPLA3", "PPLA4",
    "PPL", "PPLL", "PPLX", "RSRT", "BCH", "BAY", "BAYS"
}
US_COUNTRY_NAMES = {"united states", "united states of america", "usa", "us", "u s", "u s a"}
def prepare_gns_index_and_match_regions(print_diagnostics=True):
    import math

    norm = lambda v: " ".join(re.sub(r"\bsaint\b", "st", re.sub(r"[^a-z0-9]+", " ", "".join(c for c in unicodedata.normalize("NFKD", str(v or "")) if not unicodedata.combining(c)).casefold().replace("’", "").replace("'", ""))).split())
    compact = lambda v: norm(v).replace(" ", "")
    clean = lambda v: re.sub(r"\([^)]*\)", "", str(v or "")).strip(" ,-")
    is_us = lambda country: norm(country) in US_COUNTRY_NAMES

    # > explicit supplier tourism/transliteration aliases; used only when exact/clean finds nothing
    region_aliases = {
        ("greece", "halkidiki"): ["Chalkidiki"],
        ("greece", "kefalonia"): ["Cephalonia", "Kefallinia"],
        ("italy", "amalfi coast"): ["Amalfi"],
        ("italy", "neapolitan riviera"): ["Naples", "Napoli"],
        ("italy", "venetian riviera"): ["Venice", "Venezia"],
        ("portugal", "costa verde"): ["Porto"],
        ("portugal", "madeira and porto santo"): ["Madeira"],
        ("spain", "costa de almeria"): ["Almería"]
    }

    # > supplier-name variants only; no fuzzy guessing
    def region_variants(region):
        original = clean(region)
        variants = [(original, "EXACT")]
        simplified = re.sub(r"\s*(?:&|and)\s+surrounding\s+area\s*$", "", original, flags=re.I).strip(" ,-")
        simplified = re.sub(r"\s+area\s*$", "", simplified, flags=re.I).strip(" ,-")
        if simplified and norm(simplified) != norm(original): variants.append((simplified, "CLEAN"))
        return variants

    def haversine_km(lat1, lon1, lat2, lon2):
        r = 6371.0088
        p1, p2 = math.radians(lat1), math.radians(lat2)
        dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
        a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
        return 2 * r * math.asin(min(1.0, math.sqrt(a)))

    def cluster_candidates(candidates):
        groups = []
        for candidate in candidates:
            touching = []
            for i, group in enumerate(groups):
                if any(haversine_km(candidate["latitude"], candidate["longitude"], other["latitude"], other["longitude"]) <= GNS_REGION_CLUSTER_KM for other in group): touching.append(i)
            if not touching:
                groups.append([candidate])
                continue
            merged = [candidate]
            for i in reversed(touching): merged.extend(groups.pop(i))
            groups.append(merged)

        changed = True
        while changed and len(groups) > 1:
            changed = False
            for i in range(len(groups)):
                if changed: break
                for j in range(i + 1, len(groups)):
                    if any(haversine_km(a["latitude"], a["longitude"], b["latitude"], b["longitude"]) <= GNS_REGION_CLUSTER_KM for a in groups[i] for b in groups[j]):
                        groups[i].extend(groups[j])
                        del groups[j]
                        changed = True
                        break
        return groups

    def feature_priority(country, region, feature):
        region_norm = norm(region)
        country_norm = norm(country)
        features = [x.strip().upper() for x in str(feature or "").split(",") if x.strip()]

        def one(value):
            if value not in GNS_REGION_FEATURES: return 99

            # > Abu Dhabi supplier region means the Emirate, not Abu Dhabi Island
            if country_norm == "united arab emirates" and region_norm == "abu dhabi":
                if value == "ADM1": return 0
                if value in {"ADM2", "ADMD", "RGN"}: return 1
                if value in {"PPLC", "PPLCD", "PPLA", "PPLA2", "PPLA3", "PPLA4"}: return 2
                if value in {"ISL", "ISLS", "PEN"}: return 4
                if value in {"PPL", "PPLL", "PPLX", "RSRT"}: return 5
                return 8

            if region_norm == country_norm:
                if value in {"PCLI", "PCLIX"}: return 0
                if value in {"ISL", "ISLS", "PEN"}: return 1
                if value == "ADM1": return 2
                if value in {"ADM2", "ADMD", "RGN"}: return 3
                if value in {"PPLC", "PPLCD"}: return 4
                return 8

            if any(word in region_norm for word in ("coast", "costa", "riviera")):
                if value == "CST": return 0
                if value in {"ADM1", "ADM2", "ADMD", "RGN"}: return 1
                if value in {"ISL", "ISLS", "PEN", "CAPE", "BAY", "BAYS", "BCH"}: return 2
                if value in {"PPLC", "PPLA", "PPLA2", "PPLA3", "PPLA4", "PPL", "PPLL", "PPLX", "RSRT"}: return 4
                return 8

            if "lake" in region_norm:
                if value == "LK": return 0
                if value in {"ADM1", "ADM2", "ADMD", "RGN"}: return 2
                if value in {"PPLC", "PPLA", "PPLA2", "PPLA3", "PPLA4", "PPL"}: return 4
                return 8

            if value in {"ISL", "ISLS"}: return 0
            if value == "ADM1": return 1
            if value in {"ADM2", "ADMD", "RGN"}: return 2
            if value in {"PPLC", "PPLCD", "PPLA", "PPLA2", "PPLA3", "PPLA4"}: return 3
            if value in {"PEN", "CST", "CAPE", "BAY", "BAYS", "LK", "BCH"}: return 4
            if value in {"PPL", "PPLL", "PPLX", "RSRT"}: return 5
            return 8

        return min((one(value) for value in features), default=99)

    def select_candidate(country, region, candidates):
        credible = []
        for candidate in candidates:
            candidate["_priority"] = feature_priority(country, region, candidate["feature"])
            candidate["_credible"] = candidate["_priority"] < 99
            if candidate["_credible"]: credible.append(candidate)

        if not credible: return None, "NO CREDIBLE"

        best_priority = min(x["_priority"] for x in credible)
        top = [x for x in credible if x["_priority"] == best_priority]
        groups = cluster_candidates(top)

        if len(groups) > 1: return None, f"AMBIGUOUS {len(groups)} CLUSTERS"

        group = groups[0]
        mode_priority = {"EXACT":0, "CLEAN":1, "ALIAS":2}

        if len(group) == 1: return group[0], group[0]["match_mode"]

        def centrality(candidate):
            return sum(haversine_km(candidate["latitude"], candidate["longitude"], other["latitude"], other["longitude"]) for other in group if other is not candidate)

        selected = min(group, key=lambda x: (mode_priority.get(x["match_mode"], 99), int(x.get("rank") or 999), centrality(x), x["name"]))
        return selected, "CLUSTER"

    # > classify provenance without making matching stricter
    def coordinate_confidence(country, region, selected, selection_reason=None):
        region_norm = norm(region)
        country_norm = norm(country)
        match_mode = selected["match_mode"]
        features = {x.strip().upper() for x in str(selected.get("feature") or "").split(",") if x.strip()}

        if match_mode == "ALIAS": return "REVIEW"
        if country_norm == "united arab emirates" and region_norm == "abu dhabi": return "REVIEW"
        if any(word in region_norm for word in ("coast", "costa", "riviera")) or "lake" in region_norm: return "REVIEW"
        if features.intersection({"PEN", "CST", "CAPE", "BAY", "BAYS", "LK", "BCH"}): return "REVIEW"
        if selection_reason and str(selection_reason).startswith("AMBIGUOUS"): return "REVIEW"
        if match_mode == "CLEAN": return "HIGH"
        if region_norm == country_norm and features.intersection({"PCLI", "PCLIX"}): return "TRUSTED"
        if features.intersection({"ISL", "ISLS"}): return "TRUSTED"
        if "ADM1" in features: return "TRUSTED"
        if match_mode == "EXACT": return "HIGH"
        return "REVIEW"

    # > country names -> GNS country codes
    country_codes = {}
    with open(COUNTRY_INFO_PATH, "r", encoding="utf-8-sig", errors="replace") as f:
        for raw in f:
            if not raw.strip() or raw.startswith("#"): continue
            parts = raw.rstrip("\r\n").split("\t")
            if len(parts) < 5: continue
            iso2, iso3, country_name = parts[0].strip().upper(), parts[1].strip().upper(), parts[4].strip()
            country_codes.setdefault(norm(country_name), set()).update(x for x in (iso2, iso3) if x)

    country_aliases = {
        "cape verde":{"cape verde", "cabo verde"},
        "czech republic":{"czech republic", "czechia"},
        "republic of ireland":{"republic of ireland", "ireland"},
        "uae":{"uae", "united arab emirates"},
        "turkiye":{"turkiye", "turkey"},
        "turkey":{"turkey", "turkiye"},
        "netherlands":{"netherlands", "netherlands the", "the netherlands"},
        "the netherlands":{"netherlands", "netherlands the", "the netherlands"}
    }

    def codes_for_country(country):
        country_norm = norm(country)
        names = set(country_aliases.get(country_norm, {country_norm}))
        names.add(country_norm)
        codes = set()
        for name in names: codes.update(country_codes.get(norm(name), set()))
        return codes

    # > reusable index contains the full useful GNS source and is never tied to current/missing regions
    source_signature = f"{GNS_WORLD_PATH.name}|{GNS_WORLD_PATH.stat().st_size}"
    index_valid = False

    if GNS_INDEX_DB_PATH.exists():
        try:
            index_db = sqlite3.connect(GNS_INDEX_DB_PATH)
            meta = dict(index_db.execute("SELECT key,value FROM meta").fetchall())
            row_count = index_db.execute("SELECT COUNT(*) FROM names").fetchone()[0]
            index_valid = meta.get("source_signature") == source_signature and meta.get("index_version") == GNS_INDEX_VERSION
            if index_valid: print(f"GNS INDEX          | CACHED | {row_count:,} NAME ROWS | {GNS_WORLD_PATH.name}")
            index_db.close()
        except sqlite3.Error:
            index_valid = False

    if not index_valid:
        GNS_INDEX_DB_PATH.unlink(missing_ok=True)
        index_db = sqlite3.connect(GNS_INDEX_DB_PATH)
        index_db.execute("PRAGMA journal_mode=OFF")
        index_db.execute("PRAGMA synchronous=OFF")
        index_db.execute("PRAGMA temp_store=MEMORY")
        index_db.execute("PRAGMA cache_size=-200000")
        index_db.execute("""
            CREATE TABLE names (
                country_code TEXT NOT NULL,
                name_norm TEXT NOT NULL,
                name_compact TEXT NOT NULL,
                name TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                feature TEXT NOT NULL,
                adm1 TEXT,
                name_rank INTEGER
            )
        """)
        index_db.execute("CREATE TABLE meta (key TEXT PRIMARY KEY,value TEXT)")

        total_bytes = GNS_WORLD_PATH.stat().st_size
        done_bytes = 0
        bar_width = 30
        started = time.monotonic()
        batch = []
        inserted = 0

        print(f"GNS INDEX          | BUILDING | {GNS_WORLD_PATH.name}")

        with open(GNS_WORLD_PATH, "rb") as f:
            header_raw = f.readline()
            done_bytes += len(header_raw)
            header = [x.strip().lower() for x in header_raw.decode("utf-8-sig", "replace").rstrip("\r\n").split("\t")]
            idx = {name:i for i, name in enumerate(header)}
            required = {"cc_ft", "lat_dd", "long_dd", "desig_cd", "adm1", "full_name", "full_nm_nd", "sort_name", "name_rank"}
            missing = required - idx.keys()
            if missing: raise RuntimeError(f"GNS fields missing: {', '.join(sorted(missing))}")
            max_idx = max(idx[x] for x in required)

            for row_count, raw in enumerate(f, 1):
                done_bytes += len(raw)

                if row_count % 100000 == 0:
                    pct = min(done_bytes / total_bytes, 1)
                    filled = int(pct * bar_width)
                    elapsed = time.monotonic() - started
                    print(f"\rGNS INDEX          | [{'#' * filled}{'-' * (bar_width - filled)}] {pct * 100:6.2f}% | {done_bytes / 1024**3:.2f}/{total_bytes / 1024**3:.2f} GB | {inserted + len(batch):,} NAME ROWS | {elapsed:.0f}s", end="", flush=True)

                parts = raw.rstrip(b"\r\n").split(b"\t")
                if len(parts) <= max_idx: continue

                feature = parts[idx["desig_cd"]].decode("utf-8", "replace").strip()
                feature_parts = {x.strip().upper() for x in feature.split(",") if x.strip()}
                if not feature_parts.intersection(GNS_REGION_FEATURES): continue

                codes = [x.strip().upper() for x in parts[idx["cc_ft"]].decode("utf-8", "replace").split(",") if x.strip()]
                codes = [x for x in codes if x not in {"US", "USA"}]
                if not codes: continue

                try:
                    latitude = float(parts[idx["lat_dd"]])
                    longitude = float(parts[idx["long_dd"]])
                    rank_raw = parts[idx["name_rank"]].decode("utf-8", "replace").strip()
                    rank = int(rank_raw) if rank_raw else 999
                except (ValueError, TypeError):
                    continue

                adm1 = parts[idx["adm1"]].decode("utf-8", "replace").strip()
                source_names = [
                    parts[idx["full_name"]].decode("utf-8", "replace").strip(),
                    parts[idx["full_nm_nd"]].decode("utf-8", "replace").strip(),
                    parts[idx["sort_name"]].decode("utf-8", "replace").strip()
                ]

                unique_names = {}
                for name in source_names:
                    if not name: continue
                    name_norm = norm(name)
                    if not name_norm: continue
                    unique_names.setdefault((name_norm, compact(name)), name)

                for code in codes:
                    for (name_norm, name_compact), name in unique_names.items():
                        batch.append((code, name_norm, name_compact, name, latitude, longitude, feature, adm1, rank))

                if len(batch) >= 50000:
                    index_db.executemany("INSERT INTO names VALUES (?,?,?,?,?,?,?,?,?)", batch)
                    inserted += len(batch)
                    batch.clear()

        if batch:
            index_db.executemany("INSERT INTO names VALUES (?,?,?,?,?,?,?,?,?)", batch)
            inserted += len(batch)

        print(f"\rGNS INDEX          | [{'#' * bar_width}] 100.00% | {total_bytes / 1024**3:.2f}/{total_bytes / 1024**3:.2f} GB | {inserted:,} NAME ROWS".ljust(180))
        print("GNS INDEX          | CREATING LOOKUP INDEXES")

        index_db.execute("CREATE INDEX idx_gns_name_norm ON names(country_code,name_norm)")
        index_db.execute("CREATE INDEX idx_gns_name_compact ON names(country_code,name_compact)")
        index_db.executemany("INSERT INTO meta VALUES (?,?)", [
            ("source_signature", source_signature),
            ("index_version", GNS_INDEX_VERSION)
        ])
        index_db.commit()
        index_db.close()
        print(f"GNS INDEX          | READY | {inserted:,} NAME ROWS")

    # > all non-US regions exist independently of matching; missing coordinates are matched and existing GNS rows missing provenance are classified without moving them
    db = sqlite3.connect(HOLIDAYS_DB_PATH)
    all_region_rows = db.execute("""
        SELECT country,region,latitude,longitude,coordinate_source,coordinate_match,coordinate_feature,coordinate_confidence
        FROM regions
        ORDER BY country,region
    """).fetchall()

    all_gns_regions = [(country, region) for country, region, latitude, longitude, source, match, feature, confidence in all_region_rows if not is_us(country)]
    already_resolved = sum(1 for country, region, latitude, longitude, source, match, feature, confidence in all_region_rows if not is_us(country) and latitude is not None and longitude is not None)
    gns_regions = [
        (country, region, latitude, longitude, source, match, feature, confidence)
        for country, region, latitude, longitude, source, match, feature, confidence in all_region_rows
        if not is_us(country)
        and str(source or "").upper() != "MANUAL"
        and (
            latitude is None or longitude is None
            or (
                str(source or "").upper() == "GNS"
                and (match is None or feature is None or confidence is None)
            )
        )
    ]
    to_match = sum(1 for country, region, latitude, longitude, source, match, feature, confidence in gns_regions if latitude is None or longitude is None)
    to_classify = len(gns_regions) - to_match
    index_db = sqlite3.connect(GNS_INDEX_DB_PATH)

    stats = {"total":len(gns_regions), "resolved":0, "classified":0, "exact":0, "clean":0, "alias":0, "cluster":0, "ambiguous":0, "no_match":0, "no_country":0, "metadata_failed":0}

    print(f"GNS REGIONS        | {len(all_gns_regions):,} TOTAL | {already_resolved:,} HAVE COORDS | {to_match:,} TO MATCH | {to_classify:,} TO CLASSIFY")

    if print_diagnostics and gns_regions:
        print()
        print("=" * 190)
        print("GNS REGION RESULTS")
        print("=" * 190)

    for index, (country, region, existing_latitude, existing_longitude, existing_source, existing_match, existing_feature, existing_confidence) in enumerate(gns_regions, 1):
        codes = codes_for_country(country)
        candidates_by_key = {}
        candidates = []
        classification_only = existing_latitude is not None and existing_longitude is not None and str(existing_source or "").upper() == "GNS"

        def lookup_variants(variants):
            found = {}
            for variant, match_mode in variants:
                variant_norm = norm(variant)
                variant_compact = compact(variant)

                for code in codes:
                    rows = index_db.execute("""
                        SELECT name,latitude,longitude,feature,adm1,name_rank
                        FROM names
                        WHERE country_code=? AND name_norm=?
                    """, (code, variant_norm)).fetchall()

                    if not rows and variant_compact:
                        rows = index_db.execute("""
                            SELECT name,latitude,longitude,feature,adm1,name_rank
                            FROM names
                            WHERE country_code=? AND name_compact=?
                        """, (code, variant_compact)).fetchall()

                    for name, latitude, longitude, feature, adm1, rank in rows:
                        key = (round(float(latitude), 6), round(float(longitude), 6), feature, adm1 or "", norm(name))
                        candidate = {
                            "name":name,
                            "latitude":float(latitude),
                            "longitude":float(longitude),
                            "feature":feature or "",
                            "adm1":adm1 or "",
                            "rank":int(rank or 999),
                            "match_mode":match_mode
                        }
                        old = found.get(key)
                        mode_priority = {"EXACT":0, "CLEAN":1, "ALIAS":2}
                        if old is None or (mode_priority.get(match_mode, 99), candidate["rank"]) < (mode_priority.get(old["match_mode"], 99), old["rank"]): found[key] = candidate
            return found

        if not codes:
            selected, reason = None, "NO COUNTRY CODE"
            stats["no_country"] += 1
        else:
            # > normal exact/clean matching always gets first opportunity
            candidates_by_key = lookup_variants(region_variants(region))

            # > explicit aliases are only consulted when normal matching found nothing
            if not candidates_by_key:
                aliases = region_aliases.get((norm(country), norm(region)), [])
                if aliases: candidates_by_key = lookup_variants([(alias, "ALIAS") for alias in aliases])

            candidates = list(candidates_by_key.values())

            if not candidates:
                selected, reason = None, "NO MATCH"
                if classification_only: stats["metadata_failed"] += 1
                else: stats["no_match"] += 1
            else:
                proposed, selection_reason = select_candidate(country, region, candidates)

                if classification_only:
                    existing_candidates = [
                        candidate for candidate in candidates
                        if abs(candidate["latitude"] - float(existing_latitude)) <= 0.00001
                        and abs(candidate["longitude"] - float(existing_longitude)) <= 0.00001
                    ]
                    if existing_candidates:
                        mode_priority = {"EXACT":0, "CLEAN":1, "ALIAS":2}
                        selected = min(existing_candidates, key=lambda x: (mode_priority.get(x["match_mode"], 99), x["rank"], x["name"]))
                        reason = selection_reason
                    else:
                        selected, reason = None, "EXISTING COORD NOT FOUND"
                        stats["metadata_failed"] += 1
                else:
                    selected, reason = proposed, selection_reason
                    if selected:
                        stats["resolved"] += 1
                        if reason == "CLUSTER": stats["cluster"] += 1
                    elif reason.startswith("AMBIGUOUS"):
                        stats["ambiguous"] += 1
                    else:
                        stats["no_match"] += 1

        if selected:
            confidence = coordinate_confidence(country, region, selected, reason)

            if classification_only:
                db.execute("""
                    UPDATE regions
                    SET coordinate_match=?,coordinate_feature=?,coordinate_confidence=?
                    WHERE country=? COLLATE NOCASE AND region=? COLLATE NOCASE
                      AND latitude IS NOT NULL AND longitude IS NOT NULL
                      AND UPPER(COALESCE(coordinate_source,''))='GNS'
                """, (selected["match_mode"], selected["feature"], confidence, country, region))
                stats["classified"] += 1
                status = f"CLASSIFIED {selected['match_mode']}"
            else:
                db.execute("""
                    UPDATE regions
                    SET latitude=?,longitude=?,coordinate_source='GNS',coordinate_match=?,coordinate_feature=?,coordinate_confidence=?
                    WHERE country=? COLLATE NOCASE AND region=? COLLATE NOCASE
                      AND (latitude IS NULL OR longitude IS NULL)
                      AND UPPER(COALESCE(coordinate_source,'')) <> 'MANUAL'
                """, (selected["latitude"], selected["longitude"], selected["match_mode"], selected["feature"], confidence, country, region))
                status = "AUTO CLUSTER" if reason == "CLUSTER" else f"AUTO {selected['match_mode']}"

            if selected["match_mode"] == "EXACT": stats["exact"] += 1
            elif selected["match_mode"] == "CLEAN": stats["clean"] += 1
            elif selected["match_mode"] == "ALIAS": stats["alias"] += 1
        else:
            confidence = None
            status = reason

        if print_diagnostics:
            credible_count = sum(1 for candidate in candidates if candidate.get("_credible"))

            print()
            print(f"{index:>3}/{len(gns_regions)} | REGION:{region} | COUNTRY:{country}")
            print(f"STATUS             | {status} | CANDIDATES:{len(candidates)} | CREDIBLE:{credible_count}")

            if selected:
                print(f"SELECTED           | {selected['latitude']:>10.6f},{selected['longitude']:>11.6f} | TYPE:{selected['feature']:<8} | MATCH:{selected['match_mode']:<5} | CONF:{confidence:<7} | NAME:{selected['name']} | ADM1:{selected['adm1'] or '-'} | RANK:{selected['rank']}")
            else:
                print("SELECTED           | NONE")

            for candidate_index, candidate in enumerate(sorted(candidates, key=lambda x: (x.get("_priority", 99), {"EXACT":0, "CLEAN":1, "ALIAS":2}.get(x["match_mode"], 99), x["rank"], x["name"])), 1):
                state = "SELECTED" if selected is candidate else "CREDIBLE" if candidate.get("_credible") else "REJECT"
                print(f"GNS {candidate_index:<3}          | {state:<8} | {candidate['latitude']:>10.6f},{candidate['longitude']:>11.6f} | TYPE:{candidate['feature']:<8} | PR:{candidate.get('_priority', 99):<2} | MATCH:{candidate['match_mode']:<5} | NAME:{candidate['name'][:46]:<46} | ADM1:{candidate['adm1'] or '-':<12} | RANK:{candidate['rank']}")

    db.commit()

    remaining_rows = db.execute("""
        SELECT country,region,latitude,longitude,coordinate_source
        FROM regions
        WHERE latitude IS NULL OR longitude IS NULL
        ORDER BY country,region
    """).fetchall()
    remaining = [(country, region) for country, region, latitude, longitude, source in remaining_rows if not is_us(country) and str(source or "").upper() != "MANUAL"]

    db.close()
    index_db.close()

    failed_matches = to_match - stats["resolved"]

    print()
    print("=" * 90)
    print("GNS REGION SUMMARY")
    print("=" * 90)
    print(f"ALL REGIONS        | {len(all_gns_regions):>4}")
    print(f"ALREADY MATCHED    | {already_resolved:>4}")
    print(f"NEWLY MATCHED      | {stats['resolved']:>4}")
    print(f"CLASSIFIED         | {stats['classified']:>4}")
    print(f"FAILED MATCH       | {failed_matches:>4}")
    print(f"METADATA FAILED    | {stats['metadata_failed']:>4}")

    # > always show every unresolved non-US region regardless of diagnostics setting
    if remaining:
        print()
        print("UNRESOLVED NON-US REGIONS")
        for country, region in remaining: print(f"{country:<24} | {region}")

    return GNS_INDEX_DB_PATH
GNS_INDEX_DB = prepare_gns_index_and_match_regions(print_diagnostics=False)

# BUILD REUSABLE GNIS INDEX + MATCH US REGIONS | STATES -> CENSUS | OTHER US REGIONS -> GNIS
GNIS_INDEX_DB_PATH = GEO_DIR / "gnis_index.db"
GNIS_INDEX_VERSION = "1"
GNIS_REGION_CLUSTER_KM = 50.0
GNIS_REGION_FEATURES = {
    "Populated Place", "Census", "Civil", "Island", "Park", "Reserve", "Locale", "Area",
    "Beach", "Bay", "Cape", "Harbor", "Lake", "Forest", "Range", "Valley"
}
def prepare_gnis_index_and_match_regions(print_diagnostics=False):
    import math

    norm = lambda v: " ".join(re.sub(r"\bsaint\b", "st", re.sub(r"[^a-z0-9]+", " ", "".join(c for c in unicodedata.normalize("NFKD", str(v or "")) if not unicodedata.combining(c)).casefold().replace("’", "").replace("'", ""))).split())
    compact = lambda v: norm(v).replace(" ", "")
    clean = lambda v: re.sub(r"\([^)]*\)", "", str(v or "")).strip(" ,-")
    is_us = lambda country: norm(country) in US_COUNTRY_NAMES

    # > explicit supplier-region disambiguation where the name alone is genuinely ambiguous
    gnis_state_hints = {
        "las vegas":"Nevada",
        "new york":"New York"
    }

    # > these names are intentionally treated as cities rather than same-named Census states
    force_gnis_regions = set(gnis_state_hints)

    # > supplier-name variants only; no fuzzy guessing
    def region_variants(region):
        original = clean(region)
        variants = [(original, "EXACT")]
        simplified = re.sub(r"\s*(?:&|and)\s+surrounding\s+area\s*$", "", original, flags=re.I).strip(" ,-")
        simplified = re.sub(r"\s+area\s*$", "", simplified, flags=re.I).strip(" ,-")
        if simplified and norm(simplified) != norm(original): variants.append((simplified, "CLEAN"))
        return variants

    def haversine_km(lat1, lon1, lat2, lon2):
        r = 6371.0088
        p1, p2 = math.radians(lat1), math.radians(lat2)
        dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
        a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
        return 2 * r * math.asin(min(1.0, math.sqrt(a)))

    def cluster_candidates(candidates):
        groups = []
        for candidate in candidates:
            touching = []
            for i, group in enumerate(groups):
                if any(haversine_km(candidate["latitude"], candidate["longitude"], other["latitude"], other["longitude"]) <= GNIS_REGION_CLUSTER_KM for other in group): touching.append(i)
            if not touching:
                groups.append([candidate])
                continue
            merged = [candidate]
            for i in reversed(touching): merged.extend(groups.pop(i))
            groups.append(merged)

        changed = True
        while changed and len(groups) > 1:
            changed = False
            for i in range(len(groups)):
                if changed: break
                for j in range(i + 1, len(groups)):
                    if any(haversine_km(a["latitude"], a["longitude"], b["latitude"], b["longitude"]) <= GNIS_REGION_CLUSTER_KM for a in groups[i] for b in groups[j]):
                        groups[i].extend(groups[j])
                        del groups[j]
                        changed = True
                        break
        return groups

    def feature_priority(feature_class):
        priorities = {
            "Populated Place":0,
            "Census":1,
            "Civil":1,
            "Island":2,
            "Park":3,
            "Reserve":3,
            "Locale":3,
            "Area":4,
            "Beach":5,
            "Bay":5,
            "Cape":5,
            "Harbor":5,
            "Lake":5,
            "Forest":5,
            "Range":5,
            "Valley":5
        }
        return priorities.get(str(feature_class or "").strip(), 99)

    def select_candidate(candidates):
        credible = []
        for candidate in candidates:
            candidate["_priority"] = feature_priority(candidate["feature_class"])
            candidate["_credible"] = candidate["_priority"] < 99
            if candidate["_credible"]: credible.append(candidate)

        if not credible: return None, "NO CREDIBLE"

        best_priority = min(x["_priority"] for x in credible)
        top = [x for x in credible if x["_priority"] == best_priority]
        groups = cluster_candidates(top)

        if len(groups) > 1: return None, f"AMBIGUOUS {len(groups)} CLUSTERS"

        group = groups[0]
        mode_priority = {"EXACT":0, "CLEAN":1}

        if len(group) == 1: return group[0], group[0]["match_mode"]

        def centrality(candidate):
            return sum(haversine_km(candidate["latitude"], candidate["longitude"], other["latitude"], other["longitude"]) for other in group if other is not candidate)

        selected = min(group, key=lambda x: (mode_priority.get(x["match_mode"], 99), centrality(x), int(x["feature_id"])))
        return selected, "CLUSTER"

    # > classify provenance without making matching stricter
    def coordinate_confidence(region, selected, source, selection_reason=None):
        if source == "CENSUS": return "TRUSTED"

        match_mode = selected["match_mode"]
        feature_class = str(selected.get("feature_class") or "").strip()
        state_hint = gnis_state_hints.get(norm(region))

        if match_mode == "CLEAN": return "HIGH"
        if match_mode == "EXACT" and state_hint and feature_class == "Populated Place": return "TRUSTED"
        if match_mode == "EXACT" and feature_class == "Island": return "TRUSTED"
        if match_mode == "EXACT": return "HIGH"
        return "REVIEW"

    # > load official Census state representative coordinates
    census_states = {}
    with open(CENSUS_STATES_PATH, "r", encoding="utf-8-sig", errors="replace") as f:
        header_raw = f.readline().rstrip("\r\n")
        delimiter = "|" if "|" in header_raw else "\t"
        header = [x.strip().lower() for x in header_raw.split(delimiter)]
        idx = {name:i for i, name in enumerate(header)}
        required = {"usps", "geoid", "name", "intptlat", "intptlong"}
        missing = required - idx.keys()
        if missing: raise RuntimeError(f"Census States fields missing: {', '.join(sorted(missing))}")

        for raw in f:
            parts = raw.rstrip("\r\n").split(delimiter)
            if len(parts) <= max(idx[x] for x in required): continue
            try:
                name = parts[idx["name"]].strip()
                census_states[norm(name)] = {
                    "name":name,
                    "usps":parts[idx["usps"]].strip(),
                    "geoid":parts[idx["geoid"]].strip(),
                    "latitude":float(parts[idx["intptlat"]].strip()),
                    "longitude":float(parts[idx["intptlong"]].strip())
                }
            except (ValueError, TypeError):
                continue

    # > reusable GNIS index is tied only to the downloaded GNIS source
    source_signature = f"{GNIS_DOMESTIC_PATH.name}|{GNIS_DOMESTIC_PATH.stat().st_size}"
    index_valid = False

    if GNIS_INDEX_DB_PATH.exists():
        try:
            index_db = sqlite3.connect(GNIS_INDEX_DB_PATH)
            meta = dict(index_db.execute("SELECT key,value FROM meta").fetchall())
            row_count = index_db.execute("SELECT COUNT(*) FROM names").fetchone()[0]
            index_valid = meta.get("source_signature") == source_signature and meta.get("index_version") == GNIS_INDEX_VERSION
            if index_valid: print(f"GNIS INDEX         | CACHED | {row_count:,} NAME ROWS | {GNIS_DOMESTIC_PATH.name}")
            index_db.close()
        except sqlite3.Error:
            index_valid = False

    if not index_valid:
        GNIS_INDEX_DB_PATH.unlink(missing_ok=True)
        index_db = sqlite3.connect(GNIS_INDEX_DB_PATH)
        index_db.execute("PRAGMA journal_mode=OFF")
        index_db.execute("PRAGMA synchronous=OFF")
        index_db.execute("PRAGMA temp_store=MEMORY")
        index_db.execute("PRAGMA cache_size=-100000")
        index_db.execute("""
            CREATE TABLE names (
                feature_id INTEGER NOT NULL,
                name_norm TEXT NOT NULL,
                name_compact TEXT NOT NULL,
                name TEXT NOT NULL,
                feature_class TEXT NOT NULL,
                state_name TEXT,
                county_name TEXT,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL
            )
        """)
        index_db.execute("CREATE TABLE meta (key TEXT PRIMARY KEY,value TEXT)")

        total_bytes = GNIS_DOMESTIC_PATH.stat().st_size
        done_bytes = 0
        bar_width = 30
        started = time.monotonic()
        inserted = 0
        batch = []

        print(f"GNIS INDEX         | BUILDING | {GNIS_DOMESTIC_PATH.name}")

        with open(GNIS_DOMESTIC_PATH, "rb") as f:
            header_raw = f.readline()
            done_bytes += len(header_raw)
            header_text = header_raw.decode("utf-8-sig", "replace").rstrip("\r\n")
            delimiter = "|" if "|" in header_text else "\t"
            delimiter_bytes = delimiter.encode()
            header = [x.strip().lower() for x in header_text.split(delimiter)]
            idx = {name:i for i, name in enumerate(header)}
            required = {"feature_id", "feature_name", "feature_class", "state_name", "county_name", "prim_lat_dec", "prim_long_dec"}
            missing = required - idx.keys()
            if missing: raise RuntimeError(f"GNIS fields missing: {', '.join(sorted(missing))}")
            max_idx = max(idx[x] for x in required)

            for row_count, raw in enumerate(f, 1):
                done_bytes += len(raw)

                if row_count % 50000 == 0:
                    pct = min(done_bytes / total_bytes, 1)
                    filled = int(pct * bar_width)
                    elapsed = time.monotonic() - started
                    print(f"\rGNIS INDEX         | [{'#' * filled}{'-' * (bar_width - filled)}] {pct * 100:6.2f}% | {done_bytes / 1024**2:.1f}/{total_bytes / 1024**2:.1f} MB | {inserted + len(batch):,} NAME ROWS | {elapsed:.0f}s", end="", flush=True)

                parts = raw.rstrip(b"\r\n").split(delimiter_bytes)
                if len(parts) <= max_idx: continue

                feature_class = parts[idx["feature_class"]].decode("utf-8", "replace").strip()
                if feature_class not in GNIS_REGION_FEATURES: continue

                try:
                    feature_id = int(parts[idx["feature_id"]].decode("utf-8", "replace").strip())
                    latitude = float(parts[idx["prim_lat_dec"]])
                    longitude = float(parts[idx["prim_long_dec"]])
                except (ValueError, TypeError):
                    continue

                name = parts[idx["feature_name"]].decode("utf-8", "replace").strip()
                if not name: continue

                name_norm = norm(name)
                if not name_norm: continue

                state_name = parts[idx["state_name"]].decode("utf-8", "replace").strip()
                county_name = parts[idx["county_name"]].decode("utf-8", "replace").strip()

                batch.append((feature_id, name_norm, compact(name), name, feature_class, state_name, county_name, latitude, longitude))

                if len(batch) >= 50000:
                    index_db.executemany("INSERT INTO names VALUES (?,?,?,?,?,?,?,?,?)", batch)
                    inserted += len(batch)
                    batch.clear()

        if batch:
            index_db.executemany("INSERT INTO names VALUES (?,?,?,?,?,?,?,?,?)", batch)
            inserted += len(batch)

        print(f"\rGNIS INDEX         | [{'#' * bar_width}] 100.00% | {total_bytes / 1024**2:.1f}/{total_bytes / 1024**2:.1f} MB | {inserted:,} NAME ROWS".ljust(180))
        print("GNIS INDEX         | CREATING LOOKUP INDEXES")

        index_db.execute("CREATE INDEX idx_gnis_name_norm ON names(name_norm)")
        index_db.execute("CREATE INDEX idx_gnis_name_compact ON names(name_compact)")
        index_db.execute("CREATE INDEX idx_gnis_name_state ON names(name_norm,state_name)")
        index_db.executemany("INSERT INTO meta VALUES (?,?)", [
            ("source_signature", source_signature),
            ("index_version", GNIS_INDEX_VERSION)
        ])
        index_db.commit()
        index_db.close()
        print(f"GNIS INDEX         | READY | {inserted:,} NAME ROWS")

    # > missing US coordinates are matched; existing Census/GNIS rows missing provenance are classified without moving them
    db = sqlite3.connect(HOLIDAYS_DB_PATH)
    all_region_rows = db.execute("""
        SELECT country,region,latitude,longitude,coordinate_source,coordinate_match,coordinate_feature,coordinate_confidence
        FROM regions
        ORDER BY country,region
    """).fetchall()

    all_us_regions = [(country, region) for country, region, latitude, longitude, source, match, feature, confidence in all_region_rows if is_us(country)]
    already_resolved = sum(1 for country, region, latitude, longitude, source, match, feature, confidence in all_region_rows if is_us(country) and latitude is not None and longitude is not None)
    us_regions = [
        (country, region, latitude, longitude, source, match, feature, confidence)
        for country, region, latitude, longitude, source, match, feature, confidence in all_region_rows
        if is_us(country)
        and str(source or "").upper() != "MANUAL"
        and (
            latitude is None or longitude is None
            or (
                str(source or "").upper() in {"CENSUS", "GNIS"}
                and (match is None or feature is None or confidence is None)
            )
        )
    ]

    to_match = sum(1 for country, region, latitude, longitude, source, match, feature, confidence in us_regions if latitude is None or longitude is None)
    to_classify = len(us_regions) - to_match
    index_db = sqlite3.connect(GNIS_INDEX_DB_PATH)

    stats = {"total":len(us_regions), "resolved":0, "classified":0, "census":0, "gnis":0, "exact":0, "clean":0, "cluster":0, "ambiguous":0, "no_match":0, "metadata_failed":0}

    print(f"US REGIONS         | {len(all_us_regions):,} TOTAL | {already_resolved:,} HAVE COORDS | {to_match:,} TO MATCH | {to_classify:,} TO CLASSIFY")

    if print_diagnostics and us_regions:
        print()
        print("=" * 190)
        print("US REGION RESULTS")
        print("=" * 190)

    for index, (country, region, existing_latitude, existing_longitude, existing_source, existing_match, existing_feature, existing_confidence) in enumerate(us_regions, 1):
        region_norm = norm(region)
        classification_only = existing_latitude is not None and existing_longitude is not None and str(existing_source or "").upper() in {"CENSUS", "GNIS"}
        candidates = []
        selected = None
        reason = None
        source = None

        def lookup_gnis_candidates():
            candidates_by_key = {}
            state_hint = gnis_state_hints.get(region_norm)

            for variant, match_mode in region_variants(region):
                variant_norm = norm(variant)
                variant_compact = compact(variant)

                rows = index_db.execute("""
                    SELECT feature_id,name,feature_class,state_name,county_name,latitude,longitude
                    FROM names
                    WHERE name_norm=?
                """, (variant_norm,)).fetchall()

                if not rows and variant_compact:
                    rows = index_db.execute("""
                        SELECT feature_id,name,feature_class,state_name,county_name,latitude,longitude
                        FROM names
                        WHERE name_compact=?
                    """, (variant_compact,)).fetchall()

                for feature_id, name, feature_class, state_name, county_name, latitude, longitude in rows:
                    if state_hint and norm(state_name) != norm(state_hint): continue

                    key = (feature_id, round(float(latitude), 6), round(float(longitude), 6))
                    candidate = {
                        "feature_id":feature_id,
                        "name":name,
                        "feature_class":feature_class or "",
                        "state_name":state_name or "",
                        "county_name":county_name or "",
                        "latitude":float(latitude),
                        "longitude":float(longitude),
                        "match_mode":match_mode
                    }
                    old = candidates_by_key.get(key)
                    if old is None or (0 if match_mode == "EXACT" else 1) < (0 if old["match_mode"] == "EXACT" else 1): candidates_by_key[key] = candidate

            return list(candidates_by_key.values())

        # > existing Census provenance is recovered from the exact stored coordinate without moving it
        if classification_only and str(existing_source or "").upper() == "CENSUS":
            census_state = census_states.get(region_norm)

            if census_state and abs(census_state["latitude"] - float(existing_latitude)) <= 0.00001 and abs(census_state["longitude"] - float(existing_longitude)) <= 0.00001:
                selected = {
                    "name":census_state["name"],
                    "latitude":census_state["latitude"],
                    "longitude":census_state["longitude"],
                    "feature_class":"STATE",
                    "state_name":census_state["name"],
                    "county_name":"",
                    "feature_id":census_state["geoid"],
                    "match_mode":"EXACT"
                }
                reason = "CENSUS STATE"
                source = "CENSUS"
            else:
                reason = "EXISTING CENSUS COORD NOT FOUND"
                stats["metadata_failed"] += 1

        # > existing GNIS provenance is recovered from the matching GNIS feature at the exact stored coordinate
        elif classification_only and str(existing_source or "").upper() == "GNIS":
            candidates = lookup_gnis_candidates()

            if not candidates:
                reason = "NO MATCH"
                stats["metadata_failed"] += 1
            else:
                proposed, selection_reason = select_candidate(candidates)
                existing_candidates = [
                    candidate for candidate in candidates
                    if abs(candidate["latitude"] - float(existing_latitude)) <= 0.00001
                    and abs(candidate["longitude"] - float(existing_longitude)) <= 0.00001
                ]

                if existing_candidates:
                    selected = min(existing_candidates, key=lambda x: (0 if x["match_mode"] == "EXACT" else 1, feature_priority(x["feature_class"]), int(x["feature_id"])))
                    selected["_priority"] = feature_priority(selected["feature_class"])
                    selected["_credible"] = selected["_priority"] < 99
                    reason = selection_reason
                    source = "GNIS"
                else:
                    reason = "EXISTING GNIS COORD NOT FOUND"
                    stats["metadata_failed"] += 1

        else:
            # > exact Census state-name match unless supplier semantics explicitly say this is a city
            census_state = census_states.get(region_norm)

            if census_state and region_norm not in force_gnis_regions:
                selected = {
                    "name":census_state["name"],
                    "latitude":census_state["latitude"],
                    "longitude":census_state["longitude"],
                    "feature_class":"STATE",
                    "state_name":census_state["name"],
                    "county_name":"",
                    "feature_id":census_state["geoid"],
                    "match_mode":"EXACT"
                }
                reason = "CENSUS STATE"
                source = "CENSUS"
                stats["resolved"] += 1
                stats["census"] += 1

            else:
                candidates = lookup_gnis_candidates()

                if not candidates:
                    reason = "NO MATCH"
                    stats["no_match"] += 1
                else:
                    selected, reason = select_candidate(candidates)

                    if selected:
                        source = "GNIS"
                        stats["resolved"] += 1
                        stats["gnis"] += 1
                        if reason == "CLUSTER": stats["cluster"] += 1
                        if selected["match_mode"] == "EXACT": stats["exact"] += 1
                        else: stats["clean"] += 1
                    elif reason.startswith("AMBIGUOUS"):
                        stats["ambiguous"] += 1
                    else:
                        stats["no_match"] += 1

        if selected:
            confidence = coordinate_confidence(region, selected, source, reason)

            if classification_only:
                db.execute("""
                    UPDATE regions
                    SET coordinate_match=?,coordinate_feature=?,coordinate_confidence=?
                    WHERE country=? COLLATE NOCASE AND region=? COLLATE NOCASE
                      AND latitude IS NOT NULL AND longitude IS NOT NULL
                      AND UPPER(COALESCE(coordinate_source,''))=?
                """, (selected["match_mode"], selected["feature_class"], confidence, country, region, source))
                stats["classified"] += 1
                if source == "CENSUS": stats["census"] += 1
                else:
                    stats["gnis"] += 1
                    if selected["match_mode"] == "EXACT": stats["exact"] += 1
                    else: stats["clean"] += 1
                status = f"CLASSIFIED {source} {selected['match_mode']}"
            else:
                db.execute("""
                    UPDATE regions
                    SET latitude=?,longitude=?,coordinate_source=?,coordinate_match=?,coordinate_feature=?,coordinate_confidence=?
                    WHERE country=? COLLATE NOCASE AND region=? COLLATE NOCASE
                      AND (latitude IS NULL OR longitude IS NULL)
                      AND UPPER(COALESCE(coordinate_source,'')) <> 'MANUAL'
                """, (selected["latitude"], selected["longitude"], source, selected["match_mode"], selected["feature_class"], confidence, country, region))

                if source == "CENSUS":
                    status = "AUTO CENSUS STATE"
                else:
                    status = "AUTO CLUSTER" if reason == "CLUSTER" else f"AUTO {selected['match_mode']}"
        else:
            confidence = None
            status = reason

        if print_diagnostics:
            credible_count = sum(1 for candidate in candidates if candidate.get("_credible"))

            print()
            print(f"{index:>3}/{len(us_regions)} | REGION:{region} | COUNTRY:{country}")
            print(f"STATUS             | {status} | CANDIDATES:{len(candidates)} | CREDIBLE:{credible_count}")

            if selected:
                if source == "CENSUS":
                    print(f"SELECTED           | {selected['latitude']:>10.6f},{selected['longitude']:>11.6f} | SOURCE:CENSUS | TYPE:STATE | MATCH:{selected['match_mode']:<5} | CONF:{confidence:<7} | NAME:{selected['name']}")
                else:
                    print(f"SELECTED           | {selected['latitude']:>10.6f},{selected['longitude']:>11.6f} | SOURCE:GNIS | TYPE:{selected['feature_class']:<16} | MATCH:{selected['match_mode']:<5} | CONF:{confidence:<7} | NAME:{selected['name']} | STATE:{selected['state_name'] or '-'} | COUNTY:{selected['county_name'] or '-'} | ID:{selected['feature_id']}")
            else:
                print("SELECTED           | NONE")

            for candidate_index, candidate in enumerate(sorted(candidates, key=lambda x: (x.get("_priority", 99), 0 if x["match_mode"] == "EXACT" else 1, x["state_name"], x["name"])), 1):
                state = "SELECTED" if selected is candidate else "CREDIBLE" if candidate.get("_credible") else "REJECT"
                print(f"GNIS {candidate_index:<3}         | {state:<8} | {candidate['latitude']:>10.6f},{candidate['longitude']:>11.6f} | TYPE:{candidate['feature_class']:<16} | PR:{candidate.get('_priority', 99):<2} | MATCH:{candidate['match_mode']:<5} | NAME:{candidate['name'][:38]:<38} | STATE:{candidate['state_name'][:20] or '-':<20} | COUNTY:{candidate['county_name'][:24] or '-':<24} | ID:{candidate['feature_id']}")

    db.commit()

    remaining_rows = db.execute("""
        SELECT country,region,latitude,longitude,coordinate_source
        FROM regions
        WHERE latitude IS NULL OR longitude IS NULL
        ORDER BY country,region
    """).fetchall()
    remaining = [(country, region) for country, region, latitude, longitude, source in remaining_rows if is_us(country) and str(source or "").upper() != "MANUAL"]

    db.close()
    index_db.close()

    failed_matches = to_match - stats["resolved"]

    print()
    print("=" * 90)
    print("US REGION SUMMARY")
    print("=" * 90)
    print(f"ALL REGIONS        | {len(all_us_regions):>4}")
    print(f"ALREADY MATCHED    | {already_resolved:>4}")
    print(f"NEWLY MATCHED      | {stats['resolved']:>4}")
    print(f"CLASSIFIED         | {stats['classified']:>4}")
    print(f"FAILED MATCH       | {failed_matches:>4}")
    print(f"METADATA FAILED    | {stats['metadata_failed']:>4}")

    if remaining:
        print()
        print("UNRESOLVED US REGIONS")
        for country, region in remaining: print(f"{country:<24} | {region}")

    return GNIS_INDEX_DB_PATH
GNIS_INDEX_DB = prepare_gnis_index_and_match_regions(print_diagnostics=False)

# INSPECT ACCURACY OF REGIONAL COORDINATES
import time
import webbrowser
OUTPUT_HTML = Path("region_coordinate_map.html")
def build_region_map():
    db = sqlite3.connect(HOLIDAYS_DB_PATH)
    db.row_factory = sqlite3.Row

    rows = db.execute("""
        SELECT country,region,latitude,longitude,coordinate_source,coordinate_match,coordinate_feature,coordinate_confidence
        FROM regions
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        ORDER BY country,region
    """).fetchall()

    db.close()

    markers = []
    for r in rows:
        country = r["country"] or ""
        region = r["region"] or ""
        source = r["coordinate_source"] or ""
        match = r["coordinate_match"] or ""
        feature = r["coordinate_feature"] or ""
        confidence = r["coordinate_confidence"] or ""

        flags = []
        if match.upper() == "ALIAS": flags.append("ALIAS")
        if region.lower().endswith(" area"): flags.append("AREA")
        if feature.upper() not in {"PCLI","PCLIX","ADM1","ADM2","ISL","PPLC","PPL","PPLA","PPLA2","PPLA3","PPLA4","CIVIL","CENSUS","POPULATED PLACE"}:
            flags.append("CHECK FEATURE")

        markers.append({
            "country": country,
            "region": region,
            "lat": float(r["latitude"]),
            "lon": float(r["longitude"]),
            "source": source,
            "match": match,
            "feature": feature,
            "confidence": confidence,
            "flags": flags
        })

    marker_json = json.dumps(markers, ensure_ascii=False)

    html = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Region Coordinate Validation</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<style>
html,body,#map{{height:100%;margin:0}}
body{{font-family:Arial,sans-serif}}
.panel{{position:absolute;z-index:1000;top:10px;left:50px;background:white;padding:10px 12px;border-radius:8px;box-shadow:0 1px 6px #0004;font-size:13px;max-width:320px}}
.panel b{{font-size:14px}}
.flag{{display:inline-block;padding:2px 5px;margin:3px 3px 0 0;border-radius:4px;background:#eee;font-size:11px}}
.flag.warn{{background:#ffd7a8}}
.popup-table{{border-collapse:collapse;font-size:12px}}
.popup-table td{{padding:2px 7px 2px 0;vertical-align:top}}
.popup-table td:first-child{{font-weight:bold}}
</style>
</head>
<body>
<div id="map"></div>
<div class="panel">
<b>Region Coordinate Validation</b><br>
{len(markers)} resolved regions<br>
Click markers to inspect source/match/feature.<br>
Orange markers = alias, Area or unusual feature.
</div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
const regions = {marker_json};

const map = L.map("map", {{worldCopyJump:true}}).setView([25,10],2);

L.tileLayer("https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png", {{
    maxZoom:19,
    attribution:'&copy; OpenStreetMap contributors'
}}).addTo(map);

const bounds = [];

for (const r of regions) {{
    const needsCheck = r.flags.length > 0;

    const marker = L.circleMarker([r.lat,r.lon], {{
        radius: needsCheck ? 7 : 5,
        color: needsCheck ? "#d97706" : "#2563eb",
        fillColor: needsCheck ? "#f59e0b" : "#3b82f6",
        fillOpacity:0.8,
        weight:2
    }}).addTo(map);

    const flags = r.flags.map(x =>
        `<span class="flag warn">${{x}}</span>`
    ).join("");

    marker.bindPopup(`
        <div style="min-width:240px">
            <b>${{escapeHtml(r.region)}}</b><br>
            ${{escapeHtml(r.country)}}<br><br>
            <table class="popup-table">
                <tr><td>Latitude</td><td>${{r.lat}}</td></tr>
                <tr><td>Longitude</td><td>${{r.lon}}</td></tr>
                <tr><td>Source</td><td>${{escapeHtml(r.source)}}</td></tr>
                <tr><td>Match</td><td>${{escapeHtml(r.match)}}</td></tr>
                <tr><td>Feature</td><td>${{escapeHtml(r.feature)}}</td></tr>
                <tr><td>Confidence</td><td>${{escapeHtml(r.confidence)}}</td></tr>
            </table>
            ${{flags ? "<br>"+flags : ""}}
        </div>
    `);

    marker.bindTooltip(`${{r.region}}, ${{r.country}}`);
    bounds.push([r.lat,r.lon]);
}}

if (bounds.length) map.fitBounds(bounds, {{padding:[30,30]}});

function escapeHtml(v) {{
    return String(v ?? "")
        .replaceAll("&","&amp;")
        .replaceAll("<","&lt;")
        .replaceAll(">","&gt;")
        .replaceAll('"',"&quot;")
        .replaceAll("'","&#039;");
}}
</script>
</body>
</html>"""

    OUTPUT_HTML.write_text(html,encoding="utf-8")
    webbrowser.open(OUTPUT_HTML.resolve().as_uri())
    time.sleep(2)
    try:OUTPUT_HTML.unlink()
    except FileNotFoundError:pass
    print(f"MAP OPENED | REGIONS:{len(markers)} | TEMP FILE DELETED")
if show_debug_map: build_region_map()
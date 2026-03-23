"""
GHCN-Daily Ingestion Pipeline
==============================
Downloads yearly weather data from NOAA's Global Historical Climatology
Network Daily (GHCN-D) dataset via public HTTP (no AWS credentials needed),
converts to Parquet format, and uploads to Google Cloud Storage.

Source: http://noaa-ghcn-pds.s3.amazonaws.com/
Docs:   https://registry.opendata.aws/noaa-ghcn/

Usage:
    python3 ingest_ghcn.py                    # ingest all years
    python3 ingest_ghcn.py --years 2020 2021  # ingest specific years
    python3 ingest_ghcn.py --years 2020 --force  # re-download even if exists
"""

import argparse
import io
import logging
import os
import sys

import pandas as pd
import requests
from google.cloud import storage
from tqdm import tqdm

# ── Logging setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────
PROJECT_ID      = "mythic-altar-485103-v8"
GCS_BUCKET      = "ghcn-weather-datalake-mythic-altar-485103-v8"
GCS_RAW_PREFIX  = "raw/yearly"
GCS_META_PREFIX = "raw/metadata"

# 5 years of data — enough for trend analysis, manageable for pipeline
DEFAULT_YEARS = [2020, 2021, 2022, 2023, 2024]

# Path to GCP service account key
CREDENTIALS = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "terraform",
    "ghcn-sa-key.json",
)

# NOAA public HTTP endpoints — no authentication required
BASE_URL     = "http://noaa-ghcn-pds.s3.amazonaws.com/csv/by_year"
STATIONS_URL = "http://noaa-ghcn-pds.s3.amazonaws.com/ghcnd-stations.txt"

# Raw CSV column names (no header in yearly files after 2024 format check)
COLUMNS = [
    "station_id",
    "date",
    "element",
    "data_value",
    "m_flag",
    "q_flag",
    "s_flag",
    "obs_time",
]

# Data types for efficient memory usage
DTYPES = {
    "station_id": "string",
    "date":       "string",
    "element":    "string",
    "data_value": "Int64",    # nullable integer — handles missing values
    "m_flag":     "string",
    "q_flag":     "string",
    "s_flag":     "string",
    "obs_time":   "string",
}


# ── GCS helpers ────────────────────────────────────────────────────────────────
def get_gcs_client() -> storage.Client:
    """Create an authenticated GCS client using service account credentials."""
    log.info("Connecting to GCS with service account credentials...")
    return storage.Client.from_service_account_json(CREDENTIALS)


def blob_exists(client: storage.Client, blob_path: str) -> bool:
    """Check if a file already exists in GCS."""
    return client.bucket(GCS_BUCKET).blob(blob_path).exists()


def upload_to_gcs(client: storage.Client, buf: io.BytesIO, blob_path: str):
    """Upload a BytesIO buffer to GCS."""
    blob = client.bucket(GCS_BUCKET).blob(blob_path)
    buf.seek(0)
    blob.upload_from_file(buf, content_type="application/octet-stream")
    size_mb = buf.getbuffer().nbytes / (1024 * 1024)
    log.info(f"Uploaded {size_mb:.1f} MB → gs://{GCS_BUCKET}/{blob_path}")


# ── Download helpers ───────────────────────────────────────────────────────────
def download_file(url: str, label: str) -> bytes:
    """
    Download a file from a public HTTP URL with a progress bar.
    No authentication needed — NOAA bucket is publicly accessible.
    """
    log.info(f"Downloading: {url}")
    response = requests.get(url, stream=True, timeout=300)
    response.raise_for_status()

    total_bytes = int(response.headers.get("content-length", 0))
    buf = io.BytesIO()

    with tqdm(
        total=total_bytes,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        desc=label,
        ncols=80,
    ) as bar:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            buf.write(chunk)
            bar.update(len(chunk))

    return buf.getvalue()


# ── Stations metadata ──────────────────────────────────────────────────────────
def parse_stations_file(raw_bytes: bytes) -> pd.DataFrame:
    """
    Parse the fixed-width ghcnd-stations.txt file.

    Format (positional columns, not CSV):
        ID          chars  1-11
        LATITUDE    chars 13-20
        LONGITUDE   chars 22-30
        ELEVATION   chars 32-37
        STATE       chars 39-40
        NAME        chars 42-71
        GSN FLAG    chars 73-75
        WMO ID      chars 81-85

    Country code = first 2 characters of station ID.
    e.g. station AE000041196 → country code AE (United Arab Emirates)
    """
    rows = []
    for line in raw_bytes.decode("utf-8").splitlines():
        if len(line) < 11:
            continue
        station_id   = line[0:11].strip()
        country_code = station_id[0:2]
        try:
            lat  = float(line[12:20].strip())
            lon  = float(line[21:30].strip())
            elev = float(line[31:37].strip())
        except ValueError:
            continue
        name = line[41:71].strip() if len(line) > 41 else ""
        rows.append({
            "station_id":   station_id,
            "country_code": country_code,
            "latitude":     lat,
            "longitude":    lon,
            "elevation_m":  elev,
            "station_name": name,
        })

    df = pd.DataFrame(rows)
    log.info(
        f"Parsed {len(df):,} stations across "
        f"{df['country_code'].nunique()} country codes"
    )
    return df


def ingest_stations(client: storage.Client, force: bool = False):
    """Download, parse, and upload stations metadata to GCS."""
    log.info("=" * 55)
    log.info("STEP 1: Stations metadata")
    log.info("=" * 55)

    blob_path = f"{GCS_META_PREFIX}/ghcnd_stations.parquet"

    if not force and blob_exists(client, blob_path):
        log.info(f"Already exists — skipping (use --force to re-download)")
        return

    raw = download_file(STATIONS_URL, "stations.txt")
    df  = parse_stations_file(raw)

    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    upload_to_gcs(client, buf, blob_path)


# ── Yearly weather data ────────────────────────────────────────────────────────
def get_data_quality_report(df: pd.DataFrame) -> dict:
    """
    Analyse raw data quality issues.
    This feeds directly into our dbt cleaning strategy.
    """
    total        = len(df)
    missing_vals = int(df["data_value"].isna().sum())
    q_flagged    = int(
        (df["q_flag"].notna() & df["q_flag"].str.strip().ne("")).sum()
    )
    elements     = df["element"].value_counts().head(5).to_dict()

    return {
        "total_rows":     total,
        "missing_values": missing_vals,
        "missing_pct":    round(missing_vals / total * 100, 2),
        "q_flagged_rows": q_flagged,
        "q_flagged_pct":  round(q_flagged / total * 100, 2),
        "top_elements":   elements,
    }


def ingest_year(client: storage.Client, year: int, force: bool = False):
    """
    Download one year of GHCN-Daily data, convert to Parquet, upload to GCS.

    Raw CSV columns: station_id, date, element, data_value,
                     m_flag, q_flag, s_flag, obs_time

    We store the FULL unfiltered year in GCS (raw zone).
    Country filtering happens downstream in BigQuery + dbt.
    This follows the medallion architecture:
        raw (GCS) → filtered (BigQuery external) → clean (dbt models)
    """
    log.info("=" * 55)
    log.info(f"STEP 2: Year {year}")
    log.info("=" * 55)

    blob_path = f"{GCS_RAW_PREFIX}/{year}.parquet"

    if not force and blob_exists(client, blob_path):
        log.info(f"Already exists — skipping (use --force to re-download)")
        return

    # Download
    url = f"{BASE_URL}/{year}.csv"
    raw = download_file(url, f"{year}.csv")

    # Parse CSV
    log.info("Parsing CSV into DataFrame...")
    df = pd.read_csv(
        io.BytesIO(raw),
        header=0,
        names=COLUMNS,
        dtype=DTYPES,
    )

    # Data quality report — important for dbt strategy
    report = get_data_quality_report(df)
    log.info(f"Rows            : {report['total_rows']:,}")
    log.info(f"Missing values  : {report['missing_values']:,} "
             f"({report['missing_pct']}%)")
    log.info(f"Quality-flagged : {report['q_flagged_rows']:,} "
             f"({report['q_flagged_pct']}%)")
    log.info(f"Top elements    : {report['top_elements']}")

    # Convert to Parquet — columnar format, much smaller than CSV
    log.info("Converting to Parquet (columnar format)...")
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    parquet_mb = buf.getbuffer().nbytes / (1024 * 1024)
    csv_mb     = len(raw) / (1024 * 1024)
    log.info(
        f"Size: {csv_mb:.0f} MB CSV → {parquet_mb:.0f} MB Parquet "
        f"({100 - parquet_mb/csv_mb*100:.0f}% smaller)"
    )

    # Upload to GCS
    upload_to_gcs(client, buf, blob_path)


# ── CLI ────────────────────────────────────────────────────────────────────────
def parse_args():
    parser = argparse.ArgumentParser(
        description="Ingest NOAA GHCN-Daily data to GCS"
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=DEFAULT_YEARS,
        help=f"Years to ingest (default: {DEFAULT_YEARS})",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if file already exists in GCS",
    )
    return parser.parse_args()


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    args = parse_args()

    log.info("=" * 55)
    log.info("GHCN-Daily Ingestion Pipeline")
    log.info(f"Bucket : gs://{GCS_BUCKET}")
    log.info(f"Years  : {args.years}")
    log.info(f"Force  : {args.force}")
    log.info("=" * 55)

    client = get_gcs_client()

    # Step 1: stations metadata
    ingest_stations(client, force=args.force)

    # Step 2: yearly weather files
    for year in args.years:
        ingest_year(client, year, force=args.force)

    log.info("=" * 55)
    log.info("Ingestion complete!")
    log.info(f"Raw data: gs://{GCS_BUCKET}/raw/")
    log.info("Next step: create BigQuery external tables")
    log.info("=" * 55)


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
adls_shipments_clt_export.py

Extract JSON events from ADLS Gen2 (container 'shipmentsestesprod02') for a date
range, filter on current terminal == "010-CLT", and export one CSV.

Folder layout (as shown in screenshots):
/shipmentsestesprod02/
  YYYY-M-D/                 <-- date folders (not zero-padded)
    <PRO_ID>/               <-- subfolder per PRO
      *.json                <-- one JSON per state change

Auth (pick one via env):
- AZURE_STORAGE_CONNECTION_STRING
- AZURE_ACCOUNT_NAME + AZURE_ACCOUNT_KEY
- AZURE_ACCOUNT_NAME + AZURE_SAS_TOKEN   (starts with "?sv=")

Requires:
    pip install pandas fsspec adlfs python-dateutil

Note:
- Read-only; script only reads from ADLS and writes local CSV by default.
"""

from __future__ import annotations

import os
import re
import json
import logging
import argparse
from datetime import datetime, date

from typing import Dict, Iterable, Iterator, List, Optional, Tuple

import pandas as pd
from dateutil import parser as dateparser

import fsspec

# Add Azure Blob Storage SDK import for optimization
try:
    from azure.storage.blob import BlobServiceClient
    AZURE_SDK_AVAILABLE = True
except ImportError:
    AZURE_SDK_AVAILABLE = False



STORAGE_PROTOCOL = "abfs"


# --------------------------- Auth / Config ---------------------------
# TODO: Adding an Anik Comment
class ADLSConfig:
    def __init__(
        self,
        container: str,
        connection_string: Optional[str] = None,
        account_name: Optional[str] = None,
        account_key: Optional[str] = None,
        sas_token: Optional[str] = None,
    ) -> None:
        self.container = container
        self.connection_string = connection_string or os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.account_name = account_name or os.getenv("AZURE_ACCOUNT_NAME")
        self.account_key = account_key or os.getenv("AZURE_ACCOUNT_KEY")
        self.sas_token = sas_token or os.getenv("AZURE_SAS_TOKEN")

    @property
    def storage_options(self) -> Dict[str, str]:
        if self.connection_string:
            return {"connection_string": self.connection_string}
        if self.account_name and self.account_key:
            return {"account_name": self.account_name, "account_key": self.account_key}
        if self.account_name and self.sas_token:
            token = self.sas_token if self.sas_token.startswith("?") else f"?{self.sas_token}"
            return {"account_name": self.account_name, "sas_token": token}
        raise ValueError(
            "No valid Azure auth found. Set either AZURE_STORAGE_CONNECTION_STRING "
            "or AZURE_ACCOUNT_NAME + (AZURE_ACCOUNT_KEY | AZURE_SAS_TOKEN)."
        )

    def filesystem(self):
        return fsspec.filesystem(STORAGE_PROTOCOL, **self.storage_options)


def _abfs_uri(container: str, path: str) -> str:
    path = path.lstrip("/")
    return f"{STORAGE_PROTOCOL}://{container}/{path}" if path else f"{STORAGE_PROTOCOL}://{container}"


# --------------------------- Helpers ---------------------------

_DATE_DIR_RE = re.compile(r"^\d{4}-\d{1,2}-\d{1,2}$")  # YYYY-M-D


def _parse_date_dir(name: str) -> Optional[date]:
    """Return date for a folder name like '2025-8-11'; else None."""
    if not _DATE_DIR_RE.match(name):
        return None
    try:
        dt = dateparser.parse(name).date()
        # quick sanity check by formatting back without leading zeros accepted
        return dt
    except Exception:
        return None


def _iter_date_dirs(fs, container: str, root: str) -> Iterator[Tuple[str, date]]:
    """
    Yield (dir_uri, dir_date) for all immediate subdirs in root that look like YYYY-M-D.
    """
    base_uri = _abfs_uri(container, root)
    for entry in fs.ls(base_uri, detail=True):
        if entry.get("type") == "directory":
            name = entry["name"].split("/")[-1]
            d = _parse_date_dir(name)
            if d:
                yield entry["name"], d  # entry["name"] is already a full abfs path


def _get_pro_dirs(fs, date_dir_uri: str, pro_limit: Optional[int] = None) -> List[str]:
    """Return list of immediate subdirectories (PRO IDs) under a date directory, optionally limited."""
    pro_dirs = []
    for entry in fs.ls(date_dir_uri, detail=True):
        if entry.get("type") == "directory":
            pro_dirs.append(entry["name"])
            if pro_limit and pro_limit > 0 and len(pro_dirs) >= pro_limit:
                break
    return pro_dirs


def _get_pro_dirs_optimized(cfg: 'ADLSConfig', container: str, date_path: str, pro_limit: Optional[int] = None) -> List[str]:
    """
    More efficient PRO directory fetching using Azure SDK directly.
    This allows us to limit results at the API level instead of fetching everything.
    """
    if not AZURE_SDK_AVAILABLE or not pro_limit or pro_limit <= 0:
        # Fall back to original fsspec method
        fs = cfg.filesystem()
        date_dir_uri = f"{STORAGE_PROTOCOL}://{container}/{date_path}"
        return _get_pro_dirs(fs, date_dir_uri, pro_limit)
    
    try:
        # Create BlobServiceClient using the same credentials
        if cfg.connection_string:
            client = BlobServiceClient.from_connection_string(cfg.connection_string)
        elif cfg.account_name and cfg.account_key:
            client = BlobServiceClient(
                account_url=f"https://{cfg.account_name}.blob.core.windows.net",
                credential=cfg.account_key
            )
        elif cfg.account_name and cfg.sas_token:
            token = cfg.sas_token if cfg.sas_token.startswith("?") else f"?{cfg.sas_token}"
            client = BlobServiceClient(
                account_url=f"https://{cfg.account_name}.blob.core.windows.net{token}"
            )
        else:
            raise ValueError("No valid Azure credentials for direct SDK access")
        
        container_client = client.get_container_client(container)
        
        # List blobs with prefix and delimiter to get "directory-like" structure
        # This uses Azure's pagination and we can limit results
        pro_dirs = []
        blob_list = container_client.walk_blobs(
            name_starts_with=f"{date_path}/",
            delimiter="/"
        )
        
        for blob_prefix in blob_list:
            if hasattr(blob_prefix, 'name') and blob_prefix.name.endswith('/'):
                # This is a directory-like prefix
                full_path = f"{STORAGE_PROTOCOL}://{container}/{blob_prefix.name.rstrip('/')}"
                pro_dirs.append(full_path)
                
                if len(pro_dirs) >= pro_limit:
                    break
        
        return pro_dirs
        
    except Exception as ex:
        # Fall back to original method if Azure SDK approach fails
        logging.warning(f"Azure SDK approach failed ({ex}), falling back to fsspec method")
        fs = cfg.filesystem()
        date_dir_uri = f"{STORAGE_PROTOCOL}://{container}/{date_path}"
        return _get_pro_dirs(fs, date_dir_uri, pro_limit)


def _get_json_files(fs, pro_dir_uri: str, file_limit: Optional[int] = None) -> List[str]:
    """Return list of JSON file paths inside a PRO folder, optionally limited."""
    json_files = []
    for entry in fs.ls(pro_dir_uri, detail=True):
        if entry.get("type") == "file" and entry["name"].lower().endswith(".json"):
            json_files.append(entry["name"])
            if file_limit and file_limit > 0 and len(json_files) >= file_limit:
                break
    return json_files


def _extract_current_terminal(payload: Dict) -> Optional[str]:
    """
    Robustly extract current terminal. Handles both 'currentTerminal' and a possible
    misspelling 'currentTermminal', and either at root or under 'Data'.
    """
    candidates = [payload]
    data = payload.get("Data")
    if isinstance(data, dict):
        candidates.append(data)

    keys = ["currentTerminal", "currentTermminal"]
    for obj in candidates:
        # case-sensitive first
        for k in keys:
            if k in obj:
                return obj.get(k)
        # then case-insensitive
        lower_map = {k.lower(): v for k, v in obj.items()}
        for k in [kk.lower() for kk in keys]:
            if k in lower_map:
                return lower_map[k]
    return None


def _normalize(payload: Dict) -> Dict:
    """Flatten JSON to a single level dict for CSV export."""
    # pandas.json_normalize keeps lists as lists (stringified in CSV), which is OK.
    rec = pd.json_normalize(payload, sep=".").to_dict(orient="records")[0]
    return rec


# --------------------------- Core logic ---------------------------

def collect_events(
    container: str,
    root: str,
    start_date: str,
    end_date: str,
    terminal: str = "010-CLT",
    pro_limit: Optional[int] = 20,  # Default to 20 PRO folders per date
    files_limit: Optional[int] = 10,   # Default to 10 JSON files per PRO folder
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Scan date folders in [start_date, end_date], walk PRO subfolders, read JSON files,
    keep only those with current terminal == `terminal`. Return a normalized DataFrame.

    Limits are for dev/testing convenience.
    """
    log = logger or logging.getLogger(__name__)
    cfg = ADLSConfig(container=container)
    fs = cfg.filesystem()

    start_dt = dateparser.parse(start_date).date()
    end_dt = dateparser.parse(end_date).date()
    if end_dt < start_dt:
        raise ValueError("end_date must be on/after start_date")

    rows: List[Dict] = []
    total_files = 0

    # Iterate date directories
    for date_dir_uri, dir_date in sorted(_iter_date_dirs(fs, container, root), key=lambda x: x[1]):
        if dir_date < start_dt or dir_date > end_dt:
            continue

        # Extract just the date path (e.g., "2025-8-20" from the full URI)
        date_path = date_dir_uri.split('/')[-1] if '/' in date_dir_uri else date_dir_uri
        
        # Use optimized method that limits at Azure API level when possible
        pro_dirs = _get_pro_dirs_optimized(cfg, container, date_path, pro_limit)
        log.info("Processing %d PRO directories for date %s", len(pro_dirs), dir_date.isoformat())
        
        for pro_dir_uri in pro_dirs:

            json_files = _get_json_files(fs, pro_dir_uri, files_limit)
            files_count = len(json_files)
            total_files += files_count
            
            if files_limit and files_limit > 0 and files_count >= files_limit:
                log.debug("Limited to %d JSON files in PRO folder %s", files_limit, pro_dir_uri.split("/")[-1])
            
            for json_path in json_files:

                try:
                    with fs.open(json_path, "r") as f:
                        payload = json.load(f)
                except Exception as ex:
                    log.warning("Skipping unreadable JSON: %s (%s)", json_path, ex)
                    continue

                curr = _extract_current_terminal(payload)
                if curr == terminal:
                    rec = _normalize(payload)
                    rec["_file_date"] = dir_date.isoformat()
                    rec["_pro_folder"] = pro_dir_uri.split("/")[-1]
                    rec["_source_path"] = json_path
                    rows.append(rec)

    df = pd.DataFrame(rows)
    return df


def main(argv: Optional[List[str]] = None) -> int:
    # Load terminal from config file
    try:
        with open("config.json", 'r') as f:
            config = json.load(f)
        default_terminal = config["terminal"]
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        default_terminal = "010-CLT"  # fallback
    
    # Default date range: 1 day ending on 2025-08-01
    from datetime import datetime, timedelta
    end_date_obj = datetime.strptime("2025-08-01", "%Y-%m-%d").date()
    start_date_obj = end_date_obj - timedelta(days=1)
    
    default_end = end_date_obj.isoformat()
    default_start = start_date_obj.isoformat()
    
    parser = argparse.ArgumentParser(description="Export CLT shipment JSONs to one CSV.")
    parser.add_argument("--container", default="shipmentsestesprod02", help="ADLS container name")
    parser.add_argument("--root", default="", help="Path prefix in the container (if any)")
    parser.add_argument("--start-date", default=default_start, help=f"Inclusive start date, e.g., 2025-08-01. Default: {default_start}")
    parser.add_argument("--end-date", default=default_end, help=f"Inclusive end date, e.g., 2025-08-12. Default: {default_end}")
    parser.add_argument("--terminal", default=default_terminal, help="Terminal code to filter on")
    parser.add_argument("--out", default=None, help="Output CSV path (local). Defaults to auto-name in CWD.")
    parser.add_argument("--pro-limit", type=int, default=20, help="Limit number of PRO folders per date. Default: 20. Set to 0 for unlimited.")
    parser.add_argument("--files-limit", type=int, default=10, help="Limit JSON files per PRO folder. Default: 10. Set to 0 for unlimited.")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(message)s")

    df = collect_events(
        container=args.container,
        root=args.root,
        start_date=args.start_date,
        end_date=args.end_date,
        terminal=args.terminal,
        pro_limit=args.pro_limit,
        files_limit=args.files_limit,
    )

    if args.out is None:
        safe_term = args.terminal.replace("/", "-")
        args.out = f"shipments_{safe_term}_{args.start_date}_to_{args.end_date}.csv"

    if df.empty:
        logging.warning("No matching records found. Creating an empty CSV: %s", args.out)
        df.to_csv(args.out, index=False)
    else:
        df.to_csv(args.out, index=False)
        logging.info("Wrote %d rows to %s", len(df), args.out)

    print(args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

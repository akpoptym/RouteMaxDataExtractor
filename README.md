# ADLS Gen2 Shipments CLT Export - Guide

**Created By:** Abhishek Prajapati
**Version:** v1.0  
**Team:** DockAi — Optym, Inc.  
**Reference script:** `adls_shipments_clt_export.py`

---

## Table of Contents
- [Overview](#overview)
- [Folder Layout](#folder-layout)
- [Prerequisites](#prerequisites)
- [Authentication (pick one)](#authentication-pick-one)
- [Required Path Information](#required-path-information)
- [Quick Connection Test](#quick-connection-test)
- [How the Script Works](#how-the-script-works)
- [CLI Usage](#cli-usage)
- [Examples](#examples)
- [Configuration Flags](#configuration-flags)
- [Troubleshooting](#troubleshooting)
- [Security & Ops Notes](#security--ops-notes)

---

## Overview
This guide documents the reference Python implementation for extracting shipment movement events from **Azure Data Lake Storage Gen2 (ADLS)**, filtering to records where the **current terminal is `010-CLT`**, and exporting all matching JSON payloads as a **single CSV** for analytics.

**Key capabilities**
- Reads directly from ADLS Gen2 via `fsspec`/`adlfs`.
- Walks **date-partitioned folders** (`YYYY-M-D`, non‑zero‑padded accepted).
- Iterates **PRO** folders and reads all JSONs within each PRO.
- Robustly extracts **`currentTerminal`** (also tolerates the typo **`currentTermminal`**) both at the root and under a `Data` wrapper.
- Flattens nested JSON to columnar rows and writes one CSV.
- Optional sampling limits for fast dev iteration.

---

## Folder Layout
```
/shipmentsestesprod02/
  YYYY-M-D/                 # e.g., 2025-8-11
    <PRO_ID>/               # e.g., 0014730846
      *.json                # one JSON per state change for that PRO
```
The script parses any `YYYY-M-D` style date folder (month/day may be 1–2 digits).

---

## Prerequisites
- **Python** 3.9+
- **Packages**
  ```bash
  pip install pandas fsspec adlfs python-dateutil
  ```
- Network access to the storage account endpoint.
- Read-only permission to the storage account/container.

---

## Authentication (pick one)
Set **one** of the following methods as environment variables:

1) **Connection String (simplest)**
   ```bash
   export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=...;AccountName=<name>;AccountKey=<key>;EndpointSuffix=core.windows.net"
   ```

2) **Account Name + Account Key**
   ```bash
   export AZURE_ACCOUNT_NAME="<account_name>"
   export AZURE_ACCOUNT_KEY="<account_key>"
   ```

3) **Account Name + SAS Token (read-only)**
   ```bash
   export AZURE_ACCOUNT_NAME="<account_name>"
   export AZURE_SAS_TOKEN="?sv=..."   # must start with ?sv=
   ```
   **SAS requirements:** Service **Blob**; Resource types **Container,Object**; Permissions **Read (r)** and **List (l)**; Valid time window.

> **Windows PowerShell** equivalents:
> ```powershell
> $env:AZURE_STORAGE_CONNECTION_STRING="..."
> $env:AZURE_ACCOUNT_NAME="..."
> $env:AZURE_ACCOUNT_KEY="..."
> $env:AZURE_SAS_TOKEN="?sv=..."
> ```

---

## Required Path Information
- **Container:** `shipmentsestesprod02`
- **Root/prefix:** (blank if date folders are at container root)
- **URIs accessed:** `abfs://shipmentsestesprod02/<YYYY-M-D>/<PRO_ID>/*.json`

---

## Quick Connection Test
```python
import os, fsspec
fs = fsspec.filesystem(
    "abfs",
    connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    or None,
    account_name=os.getenv("AZURE_ACCOUNT_NAME") or None,
    account_key=os.getenv("AZURE_ACCOUNT_KEY") or None,
    sas_token=os.getenv("AZURE_SAS_TOKEN") or None,
)
print(fs.ls("abfs://shipmentsestesprod02/")[:5])
```

---

## How the Script Works
1. **Discover date folders** beneath the container (or optional root) and keep only those within `[start_date, end_date]` inclusive.
2. For each date folder, **list PRO subfolders**.
3. In each PRO folder, **read every `.json`**.
4. Parse JSON and extract **current terminal** using a robust getter that checks both `currentTerminal` and `currentTermminal`, at the root and under `Data` if present.
5. If current terminal equals the requested terminal (default `010-CLT`), **flatten** the payload and append metadata:
   - `_file_date` — the date folder value
   - `_pro_folder` — the PRO ID folder name
   - `_source_path` — the fully qualified `abfs://` path
6. Concatenate rows into a DataFrame and **write a single CSV**.

---

## CLI Usage
Main script: **`adls_shipments_clt_export.py`**

```bash
python adls_shipments_clt_export.py \
  --container shipmentsestesprod02 \
  --start-date 2025-08-01 \
  --end-date 2025-08-12 \
  --terminal 010-CLT \
  --out clt_2025-08-01_to_2025-08-12.csv
```

> **Note:** You can set `--root` if date folders are under a subpath.

---

## Examples
**Export a two‑week window for CLT**
```bash
python adls_shipments_clt_export.py \
  --container shipmentsestesprod02 \
  --start-date 2025-08-01 --end-date 2025-08-12 \
  --terminal 010-CLT \
  --out clt_2025-08-01_to_2025-08-12.csv
```

**Sample while developing (limit folders/files)**
```bash
python adls_shipments_clt_export.py \
  --container shipmentsestesprod02 \
  --start-date 2025-08-01 --end-date 2025-08-12 \
  --pro-limit 50 --files-limit 5
```

---

## Configuration Flags
| Flag | Description |
|---|---|
| `--container` | ADLS container name (default: `shipmentsestesprod02`) |
| `--root` | Optional path prefix inside the container |
| `--start-date` | Inclusive start date (e.g., `2025-08-01`) |
| `--end-date` | Inclusive end date (e.g., `2025-08-12`) |
| `--terminal` | Target terminal (default: `010-CLT`) |
| `--out` | Output CSV path (defaults to autogenerated name) |
| `--pro-limit` | Max PRO directories per date (dev only) |
| `--files-limit` | Max JSON files per PRO (dev only) |
| `--log-level` | Logging verbosity (`DEBUG`/`INFO`/`WARNING`/`ERROR`) |

---

## Troubleshooting
- **Auth error** — Ensure exactly one auth method is set; SAS must start with `?sv=` and include `r,l` permissions.
- **No files found** — Verify container name and that date folders exist in the range. Remember folder names are `YYYY-M-D` (not zero‑padded).
- **Empty CSV** — Likely no JSON matched the terminal filter. Check terminal code or test a single date.
- **JSON parse failures** — Script skips unreadable files with a warning. Confirm contents are valid JSON.
- **Performance tips** — Use `--pro-limit`/`--files-limit` during development; for large ranges, run compute in the storage account region.

---

## Security & Ops Notes
- Never commit secrets; use env vars or a secrets manager.
- Prefer **SAS** with least privilege and short expiry for ad‑hoc exports.
- For production pipelines, consider **Entra ID / Managed Identity** and writing outputs back to ADLS or a database.

#!/usr/bin/env python3

import argparse
import csv
import io
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "mgnify-csv-downloads/1.0",
    "Accept": "text/csv,*/*",
})

CSV_URL_TEMPLATE = "https://www.ebi.ac.uk/metagenomics/api/v1/analyses/{accession}/downloads?format=csv"


def read_accessions(path: Path):
    out = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            out.append(line.split()[0])
    return out


def fetch_csv_text(url: str, retries: int = 5, timeout: int = 120):
    last_err = None
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=timeout)
            r.raise_for_status()
            return r.text
        except requests.RequestException as e:
            last_err = e
            if attempt == retries - 1:
                raise
            wait = 2 ** attempt
            print(f"CSV fetch failed for {url}: {e}; retrying in {wait}s")
            time.sleep(wait)
    raise last_err


def parse_csv_rows(csv_text: str):
    fh = io.StringIO(csv_text)
    reader = csv.DictReader(fh)
    return list(reader)


def choose_processed_contigs_fasta(rows):
    matches = []

    for row in rows:
        label = (row.get("description.label") or "").strip().lower()
        fmt = (row.get("file_format.name") or "").strip().lower()
        url = (row.get("url") or "").strip()
        alias = (row.get("alias") or "").strip()

        if not url:
            continue

        if label == "processed contigs" and fmt == "fasta":
            matches.append(row)

    if not matches:
        return None

    # usually one row, but take the first deterministically
    matches.sort(key=lambda r: ((r.get("alias") or ""), (r.get("url") or "")))
    return matches[0]


def safe_filename(row, accession: str):
    alias = (row.get("alias") or "").strip()
    if alias:
        return alias

    url = (row.get("url") or "").strip()
    tail = url.rstrip("/").split("/")[-1]
    if tail:
        return tail

    return f"{accession}.fasta.gz"


def download_file(url: str, dest: Path, retries: int = 5):
    dest.parent.mkdir(parents=True, exist_ok=True)

    if dest.exists() and dest.stat().st_size > 0:
        return "exists"

    tmp = dest.with_suffix(dest.suffix + ".part")

    for attempt in range(retries):
        try:
            with SESSION.get(url, stream=True, timeout=300) as r:
                r.raise_for_status()
                with open(tmp, "wb") as fh:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            fh.write(chunk)
            tmp.replace(dest)
            return "downloaded"
        except requests.RequestException as e:
            if tmp.exists():
                tmp.unlink(missing_ok=True)
            if attempt == retries - 1:
                raise
            wait = 2 ** attempt
            print(f"Download failed for {url}: {e}; retrying in {wait}s")
            time.sleep(wait)


def process_one(accession: str, outdir: Path, sleep_seconds: float = 0.5):
    csv_url = CSV_URL_TEMPLATE.format(accession=accession)

    try:
        csv_text = fetch_csv_text(csv_url)
    except Exception as e:
        return {
            "accession": accession,
            "status": f"csv_fetch_error: {e}",
            "csv_url": csv_url,
            "download_url": "",
            "filename": "",
            "path": "",
        }

    rows = parse_csv_rows(csv_text)
    hit = choose_processed_contigs_fasta(rows)

    if not hit:
        return {
            "accession": accession,
            "status": "skipped_no_processed_contigs_fasta",
            "csv_url": csv_url,
            "download_url": "",
            "filename": "",
            "path": "",
        }

    download_url = hit["url"]
    filename = safe_filename(hit, accession)
    dest = outdir / accession / filename

    print(f"[{accession}] CSV: {csv_url}")
    print(f"[{accession}] URL: {download_url}")
    print(f"[{accession}] DEST: {dest}")

    try:
        status = download_file(download_url, dest)
    except Exception as e:
        return {
            "accession": accession,
            "status": f"download_error: {e}",
            "csv_url": csv_url,
            "download_url": download_url,
            "filename": filename,
            "path": str(dest),
        }

    if sleep_seconds > 0:
        time.sleep(sleep_seconds)

    return {
        "accession": accession,
        "status": status,
        "csv_url": csv_url,
        "download_url": download_url,
        "filename": filename,
        "path": str(dest),
    }


def main():
    p = argparse.ArgumentParser(
        description="Download MGnify metagenome assemblies by reading the downloads CSV for each MGYA accession"
    )
    p.add_argument("-i", "--input", required=True, help="Text file with one MGYA accession per line")
    p.add_argument("-o", "--outdir", default="mgnify_contigs_from_csv", help="Output directory")
    p.add_argument("-t", "--threads", type=int, default=4, help="Parallel workers")
    p.add_argument("--sleep", type=float, default=0.5, help="Sleep after each successful download")
    p.add_argument("--manifest", default="manifest.tsv", help="Manifest filename")
    args = p.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    accessions = read_accessions(Path(args.input))
    print(f"Loaded {len(accessions)} accessions")

    rows = []
    with ThreadPoolExecutor(max_workers=args.threads) as pool:
        futures = {
            pool.submit(process_one, acc, outdir, args.sleep): acc
            for acc in accessions
        }

        done = 0
        for fut in as_completed(futures):
            row = fut.result()
            rows.append(row)
            done += 1
            print(f"[{done}/{len(accessions)}] {row['accession']}: {row['status']}")

    manifest_path = outdir / args.manifest
    with open(manifest_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["accession", "status", "csv_url", "download_url", "filename", "path"],
            delimiter="\t",
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Done. Manifest written to {manifest_path}")


if __name__ == "__main__":
    main()

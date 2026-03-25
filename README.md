# MGnify Metagenome Assembly Downloader (CSV-based)

This script downloads **metagenome assemblies (processed contigs, FASTA)** from the MGnify database using the official CSV download endpoint.

It avoids slow JSON parsing and unreliable HTML scraping by using:

```
https://www.ebi.ac.uk/metagenomics/api/v1/analyses/<MGYA>/downloads?format=csv
```

---

## 🔍 What it does

For each `MGYA...` accession:

1. Fetches the downloads CSV  
2. Selects the row where:  
   - `description.label == "Processed contigs"`  
   - `file_format.name == "FASTA"`  
3. Extracts the download URL  
4. Downloads the `.fasta.gz` file  
5. Stores results per accession  

Entries without a valid assembly are **skipped automatically**.

---

## 📦 Installation

Requires Python ≥3.8

Install dependencies:

```bash
pip install requests
```

---

## 📥 Usage

Prepare a file with MGnify analysis accessions:

```
MGYA00585209
MGYA00383253
```

Run:

```bash
python download_mgnify_contigs_from_csv.py \
    -i mgya_ids.txt \
    -o mgya_contigs \
    -t 4
```

---

## ⚙️ Parameters

| Argument | Description |
|----------|-------------|
| `-i / --input` | Input file with MGYA accessions (required) |
| `-o / --outdir` | Output directory (default: `mgnify_contigs_from_csv`) |
| `-t / --threads` | Number of parallel downloads (default: 4) |
| `--sleep` | Delay between downloads (default: 0.5s) |
| `--manifest` | Output summary file (default: `manifest.tsv`) |

---

## 📁 Output structure

```
mgnify_contigs/
├── MGYA00585209/
│   └── example.fasta.gz
└── manifest.tsv
```

---

## 📊 Manifest file

| column | description |
|--------|------------|
| accession | MGYA ID |
| status | downloaded / exists / skipped / error |
| csv_url | CSV endpoint used |
| download_url | direct file URL |
| filename | downloaded file |
| path | local path |

---

## 🚫 Skipped cases

The script skips accessions when:

- no `Processed contigs` entry exists  
- only reads/amplicon data is available  
- CSV endpoint fails  

---

## ⚡ Notes

- Faster and more robust than scraping MGnify web pages (JavaScript-rendered)  
- More reliable than raw JSON parsing  
- Safe for moderate parallelisation (recommended ≤8 threads)  

---

## 🧪 Debugging tip

Inspect available files for one accession:

```bash
curl "https://www.ebi.ac.uk/metagenomics/api/v1/analyses/MGYA00585209/downloads?format=csv"
```

---

## 🧠 Rationale

MGnify distinguishes between:

- **Processed contigs (assembly)** ✅  
- **Processed nucleotide reads / amplicon** ❌  

This script explicitly filters for assemblies only.

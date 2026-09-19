# Wisdom of Crowds — Sweets in a Jar

> *"In these democratic days, any investigation into the trustworthiness and
> peculiarities of popular judgments is of interest."*
> — Francis Galton, *Vox Populi*, Nature 75:450 (1907)

A faithful replication of Galton's 1907 wisdom-of-crowds experiment, applied to
60 guesses of how many sweets are in a jar. The pipeline is written once and
runs identically in two places:

- **Locally**, in a Jupyter notebook backed by local PySpark 4.0.0.
- **On Databricks**, in a notebook attached to a DBR 17.3 LTS cluster (Spark 4.0.0).

The dataset is tiny (60 rows, forever) — the point is not scale but *fidelity*
to Galton's method, expressed as clean, testable data-engineering code.

---

## What the notebook does, step by step

Each cell corresponds to one stage of Galton's original method, quoted from
[*Vox Populi*, Nature 75:450–451 (1907)][vox-populi]:

| # | Stage                | Galton's own words |
|---|----------------------|--------------------|
| 1 | Config               | — |
| 2 | Environment bootstrap | — |
| 3 | Schema definition    | — |
| 4 | Load                 | *"About 800 tickets were issued, which were kindly lent me for examination…"* |
| 5 | Quality flags        | Flag "defective or illegible" rows without dropping yet |
| 6 | Clean                | *"After weeding thirteen cards out of the collection, as being defective or illegible, there remained 787 for discussion."* |
| 7 | Crowd statistic      | *"the middlemost estimate expresses the vox populi"* |
| 8 | Error vs truth       | *"9 lb., or 0.8 per cent, of the whole weight too high"* |
| 9 | Distribution         | Quartiles + IQR, matching Galton's centile table |
| 10 | Persist             | Delta table on Databricks, Parquet locally |
| 11 | Summary             | Human-readable printout |

---

## Project layout

```
wisdom-of-crowds/
├── README.md                        This file
├── pyproject.toml                   Path A dependencies (local PySpark)
├── requirements-connect.txt         Path B dependencies (Databricks Connect)
├── .python-version                  Python 3.11
├── .gitignore
├── data/
│   └── sweets-jar-guesses.csv       The 60 guesses
├── notebooks/
│   └── wisdom_of_crowds.ipynb       The 11-cell experiment
├── src/wisdom_of_crowds/
│   ├── __init__.py
│   ├── config.py                    Parameter resolution (config → widget)
│   └── transforms.py                Pure functions the notebook calls
└── tests/
    └── test_transforms.py           Unit tests
```

The notebook is thin. All logic lives in `src/wisdom_of_crowds/transforms.py`
as pure functions with tests. This keeps the notebook readable and lets the
code be checked without Spark.

---

## Two ways to run it

### Path A — Local PySpark (no Databricks account required)

```bash
cd /path/to/wisdom-of-crowds
python3.11 -m venv .venv-local
source .venv-local/bin/activate
pip install -e ".[local]"
jupyter lab notebooks/wisdom_of_crowds.ipynb
```

Requires Java 17. Set `JAVA_HOME` if needed. Confirmed by [Apache Spark docs][pyspark-install]:
*"PySpark requires Java 17 or later with JAVA_HOME properly set."*

### Path B — Databricks Repos (native notebook on a cluster)

Push this repository to GitHub (or Azure DevOps / Bitbucket / GitLab), then
in Databricks:

1. **Workspace → Repos → Add Repo** and point at the URL.
2. Open `notebooks/wisdom_of_crowds.ipynb` inside the Repo. The `sys.path`
   bootstrap in cell 2 resolves `../src` from the notebook's own folder, so
   `from wisdom_of_crowds…` imports resolve without any extra install.
3. Attach the notebook to a cluster running **DBR 17.3 LTS** (or any DBR
   with Spark ≥ 3.4).
4. The **widget-bootstrap cell** runs automatically and creates three
   widgets — `input_path`, `true_count`, `output_table`. Set the values:
   - `input_path` → e.g. `/Volumes/main/default/wisdom_of_crowds/sweets-jar-guesses.csv`
     (upload the CSV to that Volume first).
   - `true_count` → the number of sweets in the jar.
   - `output_table` → either `catalog.schema.table` for Delta, or a Volume
     path for Parquet.
5. Run all cells.

No Databricks-only APIs live inside `src/`; everything under that folder is
plain PySpark and portable to any Spark 3.4+ / 4.x environment.

### Path B alternative — Databricks Connect from a local IDE

```bash
python3.11 -m venv .venv-connect
source .venv-connect/bin/activate
pip install -r requirements-connect.txt
# Set DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_CLUSTER_ID
jupyter lab notebooks/wisdom_of_crowds.ipynb
```

The two virtual environments must stay separate:
[Databricks' own docs][dbx-connect-troubleshoot] state
*"The databricks-connect package conflicts with PySpark. Having both installed
will cause errors when initializing the Spark context in Python."*

---

## Version pins (all latest stable, no betas, all mutually compatible)

| Component            | Version | Why |
|----------------------|---------|-----|
| Databricks Runtime   | 17.3 LTS | Latest LTS, supported through Oct 2028 ([release notes][dbr-runtimes]) |
| Apache Spark         | 4.0.0   | Ships in DBR 17.3 LTS |
| PySpark (Path A)     | 4.0.0   | Exact match to cluster Spark |
| databricks-connect (Path B) | 17.3.* | Must match runtime major/minor |
| Python               | 3.11    | Meets PySpark 4.x floor of 3.10+ |
| Java                 | 17      | Required by PySpark 4.x |

---

## Method — a faithful Galton replication

**Crowd statistic:** the **median**, computed with
[`pyspark.sql.functions.median()`][pyspark-median] (added in Spark 3.4, present
in 4.0). Galton argued explicitly against the mean, on the grounds that a mean
*"would give a voting power to 'cranks' in proportion to their crankiness. One
absurdly large or small estimate would leave a greater impress on the result
than one of reasonable amount"* ([*One Vote, One Value*, Nature 75:414, 1907][one-vote]).

**Data cleaning:** only rows that are *defective or illegible* are removed
(null name, null guess, non-positive guess). Outliers are kept. Galton kept
his fat tails and analysed them; we do the same.

**Error metric:** signed error and signed percent error against the true count,
matching Galton's own report of *"9 lb., or 0.8 per cent, of the whole weight
too high."*

**What we don't do:** we don't dedupe on first names alone, because two people
sharing a first name are almost certainly two people. We flag them for review
only.

---

## Sources (firsthand only)

- Galton, F. (1907). *Vox Populi*. Nature 75:450–451.
  [Full-text scan (Internet Archive)][vox-populi]
- Galton, F. (1907). *One Vote, One Value*. Nature 75:414.
  Reprinted verbatim in Levy, D. M. & Peart, S. J. (2002).
  *Galton's two papers on voting as robust estimation*. Public Choice 113:357–365.
  [PDF][one-vote]
- Apache Spark. [PySpark installation][pyspark-install]
- Apache Spark. [`pyspark.sql.functions.median`][pyspark-median]
- Databricks. [Runtime release notes][dbr-runtimes]
- Databricks. [Databricks Connect troubleshooting][dbx-connect-troubleshoot]
- Databricks. [Best practices for DBFS and Unity Catalog][dbfs-uc]

[vox-populi]: https://archive.org/stream/paper-doi-10_1038_075450a0/paper-doi-10_1038_075450a0_djvu.txt
[one-vote]: https://adrenaline.ucsd.edu/kirsh/fileupload/galton.pdf
[pyspark-install]: https://spark.apache.org/docs/latest/api/python/getting_started/install.html
[pyspark-median]: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.median.html
[dbr-runtimes]: https://docs.databricks.com/aws/en/release-notes/runtime/
[dbx-connect-troubleshoot]: https://docs.databricks.com/aws/en/dev-tools/databricks-connect/python/troubleshooting
[dbfs-uc]: https://docs.databricks.com/en/dbfs/unity-catalog.html

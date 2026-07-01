# SABD Project 2 — Real-Time Analysis of US Flight Delays with Apache Flink

**Author:** Daniel Garoz Vazquez
**Institution:** Università degli Studi di Roma Tor Vergata
**Course:** Sistemi e Architetture per Big Data — A.A. 2025/26
**Team size:** 1 student (Q1 + Q2 mandatory, Q3 skipped as per spec)

---

## Overview

A streaming data-processing system that analyses US Bureau of
Transportation Statistics flight events (~2.23 M records,
1 January – 30 April 2025) and answers the two mandatory queries of
SABD Project 2:

- **Q1** — hourly per-carrier statistics over event-time tumbling
  windows (streaming, PyFlink DataStream API).
- **Q2** — top-10 airports by severe departure delays over 1h, 6h and
  full-dataset windows (batch, pandas — motivated in the report).

The system is deployed as a **3-container Docker Compose** stack:

```
        events.csv (pre-sorted)
       /                       \
   [Feeder]                [pandas batch]
   TCP :9999                     |
      |                          v
   [Flink JobManager]        Q2 CSVs
      |
   [Flink TaskManager]
      |
    Q1 CSV
```

---

## Repository structure

```
project2/
├── docker/                    Docker cluster
│   ├── docker-compose.yml     3 services: feeder + JM + TM
│   └── Dockerfile.flink       Custom Flink + PyFlink venv image
├── feeder/                    TCP stream simulator
│   ├── feeder.py              Replays events.csv with acceleration
│   └── Dockerfile
├── src/                       Application code
│   ├── preprocess.py          Merge 4 monthly CSVs → sorted events.csv
│   ├── utils.py               Shared helpers
│   ├── query1.py              PyFlink Q1 (DataStream API)
│   ├── query2_batch.py        Pandas Q2 batch implementation
│   ├── query2.py              PyFlink Q2 (iteration v4, kept for reference)
│   └── postprocess_q2.py      Optional post-processor for Flink outputs
├── benchmarks/                Q1 throughput benchmarks
│   ├── run_q1_benchmarks.sh   Runs Q1 with parallelism {1,2,4}
│   └── q1_benchmark_results.csv
├── Results/                   Query outputs
│   ├── q1/                    Q1 output (9,765 rows)
│   ├── q1_p1/, q1_p2/, q1_p4/ Per-parallelism benchmark outputs
│   ├── q2_1h_final.csv        Q2 1-hour window (14,266 rows)
│   ├── q2_6h_final.csv        Q2 6-hour window (3,602 rows)
│   └── q2_global_final.csv    Q2 full dataset (10 rows)
├── Report/                    LaTeX documents
│   ├── report.tex             IEEE conference-style report (~6 pages)
│   ├── ai_declaration.tex     Generative AI usage declaration
│   └── README.md              Compilation instructions
├── slides/                    Oral presentation
│   └── slides.tex             Beamer slides (12 frames, 15-min talk)
├── requirements.txt           Python dependencies
├── .gitignore
└── README.md                  This file
```

---

## Prerequisites

- **Docker Desktop** + **docker-compose**
- **WSL2** or Linux host (tested on Debian 12 / Ubuntu 22.04)
- **Python 3.10+** with pip (for local pre-processing and Q2 batch)
- **8 GB RAM** minimum (TaskManager gets 1.7 GB)

Compile-time (report):
- **pdfLaTeX** with `IEEEtran`, `tikz`, `booktabs`, `hyperref`,
  `tcolorbox` (all available in Overleaf).

---

## Quick start

### 1. Get the dataset

Download the CSVs from the course URL specified in the spec
(2025 monthly files) and place them under `data/`:

```
data/2025_01.csv
data/2025_02.csv
data/2025_03.csv
data/2025_04.csv
```

### 2. Pre-process (once)

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python src/preprocess.py --input data --output data/events.csv
```

This produces the sorted `data/events.csv` that the feeder replays.

### 3. Start the cluster

```bash
docker-compose -f docker/docker-compose.yml up -d --build
sleep 15
docker-compose -f docker/docker-compose.yml ps
```

Flink UI: <http://localhost:8081>

### 4. Run Q1 (PyFlink streaming)

```bash
mkdir -p Results/q1 && chmod 777 Results/q1

docker exec sabd2_jobmanager flink run -d \
    -py /opt/flink/app/src/query1.py \
    -pyexec /opt/venv/bin/python \
    -pyclientexec /opt/venv/bin/python
```

Monitor progress:

```bash
docker logs sabd2_feeder --tail 5
find Results/q1 -type f -name "*.csv" ! -name "*.inprogress*"
```

Full replay takes ~3 minutes wall-clock at `α=60,000`.

### 5. Run Q2 (batch)

```bash
python src/query2_batch.py \
    --events-csv data/events.csv \
    --results-root Results
```

Produces `Results/q2_{1h,6h,global}_final.csv` in ~30 seconds.

### 6. Q1 parallelism benchmarks

```bash
bash benchmarks/run_q1_benchmarks.sh
cat benchmarks/q1_benchmark_results.csv
```

Runs Q1 sequentially with `parallelism ∈ {1, 2, 4}`.
Runtime: ~15 minutes total.

---

## Key design decisions

Full rationale in `Report/report.pdf`. In brief:

- **Feeder + socket TCP**, not Kafka: the spec explicitly allows
  simulated stream + CSV output for 1-student teams.
- **Acceleration factor α = 60,000**: full replay in ~3 minutes;
  event-time semantics are α-invariant.
- **Watermark**: `bounded_out_of_orderness(60s)` — defensive margin
  over the pre-sorted stream.
- **Q1**: full streaming, `AggregateFunction + ProcessWindowFunction`,
  sustained ~10,000 events/second.
- **Q2**: hybrid stream/batch. Four PyFlink streaming iterations hit
  Beam Python runtime ceilings (OOM, PICKLED throughput collapse). We
  document them in Report §IV.A and Table III. The final design is a
  pandas batch consumer of the same `events.csv`, preserving
  event-time semantics.

---

## Documents

- **Report**: `Report/report.pdf` — IEEE conference format, ~6 pages
- **AI Declaration**: `Report/ai_declaration.pdf` — separate document
- **Slides**: `slides/slides.pdf` — 12 Beamer frames, 15-minute talk

---

## Reproducibility notes

- All three benchmark runs of Q1 with `parallelism ∈ {1,2,4}` at
  `α=60,000` produce **bit-identical CSVs** (verified with `diff`).
- Q2 batch output is deterministic on the same input.
- The `data/events.csv` file is intentionally excluded from the repo
  (400 MB); regenerate it with `src/preprocess.py`.

---

## License

Academic project for university coursework — not intended for
production use.

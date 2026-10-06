# Freight Repositioning Pipeline — PySpark + dbt Medallion

**What it is:** A rebuild of a freight repositioning data pipeline as a modern medallion
architecture. Raw freight data (loads, market rates, terminal capacity) flows through
**PySpark** for bronze→silver ingestion and cleaning, then **dbt** for silver→gold
business marts (cost-per-load by lane, capacity utilization, market variance).

**Why it exists:** Demonstrates the warehouse-to-lakehouse stack — PySpark for
distributed ingestion, dbt for tested, documented, versioned transformations — with a
measured tuning experiment showing real Spark performance decisions.

> **Status: In progress.** Bronze→silver pipeline first, then dbt marts, then tuning.

## Architecture

```
                    ┌─────────────────────────────┐
  synthetic         │        BRONZE (raw)          │   Parquet
  freight    ──────▶│  loads / market_rates /      │ ──▶ PySpark
  generator         │  capacity as ingested        │   validate + clean + join
                    └──────────────┬──────────────┘
                                   ▼
                    ┌─────────────────────────────┐
                    │        SILVER (clean)         │   Parquet
                    │  validated loads joined to   │ ──▶ dbt (Core/DuckDB
                    │  market rates, typed,        │    or Snowflake trial)
                    │  deduplicated                │
                    └──────────────┬──────────────┘
                                   ▼
                    ┌─────────────────────────────┐
                    │         GOLD (marts)          │
                    │  • cost_per_load_by_lane     │
                    │  • terminal_capacity_util    │
                    │  • market_variance_daily     │
                    └─────────────────────────────┘
```

## Data

The `generate/` script produces
**synthetic freight data** (~5–10M rows, 1–2 GB Parquet) that mirrors the real domain:

| Table | Grain | Key fields |
|---|---|---|
| `loads` | one row per load | load_id, origin, destination, pickup_ts, delivery_ts, equipment, miles, cost |
| `market_rates` | lane × date | lane, date, rate_per_mile |
| `capacity` | terminal × date | terminal, date, available_trucks |

1–2 GB is deliberate: large enough to make partitioning, shuffle, and broadcast-join
decisions visible in timings, small enough to run on a free Databricks tier.

## How to run

```bash
# 1. Generate data
python generate/make_freight_data.py --rows 8000000 --out data/bronze

# 2. Bronze → Silver (PySpark, Databricks Community or local Spark)
spark-submit jobs/bronze_to_silver.py --input data/bronze --output data/silver

# 3. Silver → Gold (dbt)
cd dbt_project && dbt build --profiles-dir .
```

Prereqs: Python 3.10+, `pyspark`, `dbt-core` + `dbt-duckdb` (or a Snowflake trial).
Detailed setup for each step lives in its directory's README.

## Tuning experiment

One Spark decision, measured before and after, documented here:

| Run | Change | Join stage time | Total |
|---|---|---|---|
| Baseline | default partitions, no broadcast | — | — |
| v1 | broadcast small `capacity` dimension; repartition loads by lane before the big join | — | — |

Hypothesis: the lane-level join shuffles far less once the small dimension is
broadcast and the fact table is pre-partitioned by the join key. Results land here
when the pipeline runs end to end.

## Design decisions

- **Synthetic data, real domain.** Mirrors freight repositioning (the problem I know
  from production work) without touching proprietary data.
- **dbt for the warehouse layer.** Tests, docs, and lineage are first-class; the gold
  marts carry the business logic a stakeholder would actually query.
- **Spark for what it's good at.** Distributed ingest/clean/join at the bronze→silver
  boundary — the stage where partitioning and shuffle decisions matter.
- **One tuning experiment, done properly.** A single measured before/after beats a
  list of untested optimizations.

## Roadmap

- [ ] Repo + README *(tonight)*
- [ ] Synthetic data generator (`generate/`)
- [ ] Bronze→Silver PySpark job (`jobs/`)
- [ ] Silver→Gold dbt models with tests (`dbt_project/`)
- [ ] Tuning experiment with timings (this README)
- [ ] Architecture diagram polish + final writeup

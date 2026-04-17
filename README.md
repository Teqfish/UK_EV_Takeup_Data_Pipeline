# UK EV Takeup Data Pipeline

A batch data engineering pipeline and dashboard for analysing the relationship between UK vehicle fuel-type transition and energy price trends. The project ingests public transport and energy datasets, stores raw and prepared data in Google Cloud, transforms it in BigQuery with dbt, orchestrates the full workflow with Kestra, and serves the final analytical views in Streamlit.

![Dashboard screenshot](docs/images/dashboard_overview.png)

---

## Overview

This project explores a simple but important question:

**Are electric vehicles gaining share quickly enough to offset the worsening relative cost of electricity versus traditional fossil fuels?**

To answer that, the pipeline combines:

- UK vehicle registration data from the DVLA

- UK petroleum price data from DESNZ

- European wholesale electricity price data

- Bank of England FX data for EUR/GBP normalization

The final dashboard shows:

- how vehicle fuel-type share has changed over time

- how electricity and fossil fuel prices have changed over time

- how their relative ratios have changed from a selected baseline date

[Back to top](#uk-ev-takeup-data-pipeline)

---

## Table of Contents

- [Problem Statement](#problem-statement)

- [Objective](#objective)

- [Tech Stack](#tech-stack)

- [Architecture Diagram](#architecture-diagram)

- [Dataset](#dataset)

  - [Source Summary](#source-summary)

  - [Raw Tables](#raw-tables)

- [BigQuery Data Model](#bigquery-data-model)

  - [Key Intermediate Tables](#key-intermediate-tables)

  - [Final Marts](#final-marts)

  - [Partitioning and Clustering](#partitioning-and-clustering)

- [Pipeline Workflow](#pipeline-workflow)

- [Screenshots](#screenshots)

- [Key Findings](#key-findings)

- [Future Improvements](#future-improvements)

- [Reproducibility](#reproducibility)

  - [Prerequisites](#prerequisites)

  - [Quickstart](#quickstart)

[Back to top](#uk-ev-takeup-data-pipeline)

---

## Problem Statement

The UK vehicle fleet is transitioning away from fossil fuels, but the economics of that transition are not always moving in the same direction. While plugin and battery-electric vehicles are increasing in share, the relative price of electricity versus traditional fuels has also shifted over time. This makes it difficult to judge whether the transition is progressing under improving or worsening cost conditions.

[Back to top](#uk-ev-takeup-data-pipeline)

---

## Objective

The objective of this project is to build a reproducible batch pipeline that:

- ingests multiple public datasets with different formats and update cadences

- standardises and joins them into a usable analytical model

- stores data in a warehouse designed for dashboard access

- exposes the final outputs through a simple interactive dashboard

- supports both local execution and an eventual cloud VM execution mode

[Back to top](#uk-ev-takeup-data-pipeline)

---

## Tech Stack

This project is designed around a **two-mode architecture**:

- **Local mode**: compute runs on the assessor’s machine via Docker Compose, while cloud storage and warehousing live in GCP

- **Cloud mode**: planned next phase, where Terraform provisions a VM and the full stack runs remotely there

### Stack summary

| Layer | Tool | Purpose |
|---|---|---|
| Infrastructure | Terraform | Provisions local-mode cloud resources in GCP |
| Storage | Google Cloud Storage | Stores raw and prepared source files |
| Warehouse | BigQuery | Stores raw tables and analytical models |
| Transformation | dbt Core | Builds staging, intermediate, and mart models |
| Orchestration | Kestra | Runs source ingestion flows, final mart builds, and tests |
| Dashboard | Streamlit | Serves interactive analytical charts |
| Runtime | Docker Compose | Starts local services for Kestra, Postgres, and Streamlit |
| Automation | Makefile | Provides one-command project setup and run flow |
| Language | Python | Handles ingestion, wrangling, uploads, and app logic |

### Design notes

In local mode, Terraform provisions the GCP dependencies and Docker Compose runs the local services. Kestra orchestrates the end-to-end batch flow, dbt builds the warehouse layer in BigQuery, and Streamlit queries the final marts directly.

Cloud mode is planned as an extension of the same architecture. The only intended difference is that Terraform will provision a VM and the same operational stack will run there instead of on the assessor’s local machine.

[Back to top](#uk-ev-takeup-data-pipeline)

---

## Architecture Diagram

> Replace this placeholder with a proper illustrated architecture diagram.

Recommended diagram flow:

**Source websites → Python ingestion scripts → GCS raw/prepared zones → BigQuery raw tables → dbt staging/intermediate/marts → Kestra orchestration → Streamlit dashboard**

Suggested image path:
docs/images/architecture_diagram.png

![Architecture diagram](docs/images/architecture_diagram.png)

[Back to top](#uk-ev-takeup-data-pipeline)

---

## Dataset

### Source Summary

| Source | Provider | Format | Update Frequency | Coverage | Why it is used |
|---|---|---|---|---|---|
| EUR/GBP exchange rate | Bank of England | HTML table | Daily / periodic | Historical | Converts European electricity pricing into GBP terms |
| European wholesale electricity prices | Ember / European source | CSV | Monthly | Since 2015 | Provides electricity price series for comparison with transport fuel prices |
| Petroleum products prices (4.1.1) | DESNZ | Excel | Monthly | Since 1989 | Provides UK petrol/diesel pricing and crude oil index series |
| VEH1103 | DVLA | ODS | Monthly / quarterly source table | Since 2001 | Provides total licensed vehicles by fuel type |
| VEH1153 | DVLA | ODS | Monthly / quarterly source table | Since 2001 | Provides new vehicle registrations by fuel type |

[Back to top](#uk-ev-takeup-data-pipeline)

---

### Raw Tables

The pipeline stores each source in BigQuery raw tables after initial file preparation.

---

#### `raw_bank_of_england_eur_gbp_fx`

| Attribute | Value |
|---|---|
| Source | Bank of England |
| URL | Supplied via `.env` |
| File Type | HTML extracted to CSV |
| Grain | Daily FX observations |
| Important Columns | `date`, `gbp_eur_rate` |
| Description | EUR/GBP conversion series used to convert European electricity prices into GBP |

---

#### `raw_european_wholesale_electricity_prices`

| Attribute | Value |
|---|---|
| Source | European wholesale electricity dataset |
| URL | Supplied via `.env` |
| File Type | CSV |
| Grain | Monthly |
| Important Columns | `date`, `country`, `price_eur_mwh` |
| Description | Monthly wholesale electricity pricing used to derive UK-relevant electricity cost trends |

---

#### `raw_desnz_petroleum_products_prices`

| Attribute | Value |
|---|---|
| Source | DESNZ table 4.1.1 |
| URL | Supplied via `.env` |
| File Type | XLSX |
| Grain | Quarterly after preparation |
| Important Columns | `year`, `quarter`, `premium_unleaded`, `diesel`, `crude_oil_index` |
| Description | UK petroleum and crude oil series used to compare fossil fuel pricing with electricity |

---

#### `raw_dvla_veh1103`

| Attribute | Value |
|---|---|
| Source | DVLA VEH1103 |
| URL | Supplied via `.env` |
| File Type | ODS |
| Grain | Quarterly after preparation |
| Important Columns | `geography`, `date_label`, `body_type`, detailed fuel-type columns |
| Description | Total licensed vehicles by fuel type, used to measure the fuel composition of the entire fleet |

---

#### `raw_dvla_veh1153`

| Attribute | Value |
|---|---|
| Source | DVLA VEH1153 |
| URL | Supplied via `.env` |
| File Type | ODS |
| Grain | Quarterly after preparation |
| Important Columns | `geography`, `date_label`, `body_type`, `keepership`, detailed fuel-type columns |
| Description | New vehicle registrations by fuel type, used to measure current transition in new purchases |

[Back to top](#uk-ev-takeup-data-pipeline)

---

## BigQuery Data Model

### Key Intermediate Tables

| Table | Grain | Purpose |
|---|---|---|
| `int_eur_gbp_fx_monthly` | Month | Aligns FX data to monthly grain |
| `int_electricity_prices_gb_monthly` | Month | Converts electricity prices into GBP |
| `int_electricity_prices_gb_quarterly` | Quarter | Aggregates electricity prices to dashboard grain |
| `int_petroleum_prices_quarterly` | Quarter | Cleans and standardises fossil fuel price series |
| `int_vehicle_registrations_new_quarterly` | Quarter x fuel group | Aggregates new registrations into dashboard fuel groups |
| `int_vehicle_registrations_all_quarterly` | Quarter x fuel group | Aggregates all registrations into dashboard fuel groups |
| `int_vehicle_ratios_new_quarterly` | Quarter | Calculates plugin vs fossil ratio for new registrations |
| `int_vehicle_ratios_all_quarterly` | Quarter | Calculates plugin vs fossil ratio for all registrations |
| `int_fossil_electricity_ratio_quarterly` | Quarter | Combines fossil and electricity prices into comparable ratio series |
| `int_transition_ratios_quarterly` | Quarter | Combines energy and vehicle ratio trends for the dashboard |

[Back to top](#uk-ev-takeup-data-pipeline)

---

### Final Marts

| Mart | Grain | Purpose | Used by Dashboard |
|---|---|---|---|
| `mart_energy_prices_quarterly` | Quarter | Final energy price series including electricity and fossil measures | Yes |
| `mart_vehicle_registrations_new_by_fuel_group` | Quarter x fuel group | Final grouped series for new registrations | Yes |
| `mart_vehicle_registrations_all_by_fuel_group` | Quarter x fuel group | Final grouped series for all registrations | Yes |
| `mart_transition_ratios_quarterly` | Quarter | Final ratio series used for rebased trend chart | Yes |
| `mart_transition_scorecards` | Snapshot | Final headline scorecard metrics | Yes |

[Back to top](#uk-ev-takeup-data-pipeline)

---

### Partitioning and Clustering

To satisfy the warehouse optimization requirement, key final marts are physically optimised in BigQuery.

#### Partitioned marts

These tables are partitioned by `quarter_date`:

- `mart_energy_prices_quarterly`
- `mart_transition_ratios_quarterly`
- `mart_vehicle_registrations_new_by_fuel_group`
- `mart_vehicle_registrations_all_by_fuel_group`

#### Clustered marts

These vehicle marts are additionally clustered by `fuel_group`:

- `mart_vehicle_registrations_new_by_fuel_group`
- `mart_vehicle_registrations_all_by_fuel_group`

This improves scan efficiency for dashboard queries that commonly:
- filter on date windows
- group or filter by fuel group

[Back to top](#uk-ev-takeup-data-pipeline)

---

## Pipeline Workflow

### 1. Source retrieval challenges

The five sources are heterogeneous and awkward in different ways:

- some are directly downloadable files
- some are web pages containing tables
- some are Excel or ODS files with high-offset headers
- some use inconsistent date labels and mixed data types
- some require manual column name cleaning before BigQuery can ingest them

The ingestion layer standardises these source-specific issues before data enters the warehouse.

### 2. Raw ingestion and storage

Each source is downloaded locally by a Python ingestion script, then:

- raw files are uploaded to GCS under a `raw/` prefix
- cleaned/prepared files are written locally as parquet where needed
- prepared files are uploaded to GCS under a `prepared/` prefix
- BigQuery raw-load scripts ingest those prepared files into raw warehouse tables

### 3. Cleaning and type normalization

The raw data required substantial wrangling, including:

- cleaning invalid BigQuery column names
- casting mixed-type date and string fields
- reconstructing quarter dates from inconsistent labels
- normalising fuel-type columns
- converting wide source tables into reusable analytical forms

### 4. Joining and aggregation

dbt is used to transform raw warehouse tables into analytical layers:

- staging models rename and standardise fields
- intermediate models align grain and combine energy + vehicle data
- mart models expose dashboard-ready tables

The current dashboard grain is quarterly.

### 5. BigQuery optimisation

Final dashboard-facing marts are materialized as physical BigQuery tables. Relevant tables are partitioned by `quarter_date`, and vehicle marts are clustered by `fuel_group`.

### 6. Dashboard query layer

The dashboard queries final marts directly for the main charts and scorecards. Some temporary prototype drill-down views also query staging models directly to test detailed fuel-type charting before those structures are promoted upstream into dbt.

### 7. Infrastructure and orchestration design

The project uses:

- **Terraform** to provision GCP infrastructure for local mode
- **Docker Compose** to run the local service stack
- **Kestra** to orchestrate:
  - five source ingestion subflows
  - final mart rebuild
  - final dbt mart tests
- **Makefile** to wrap setup and operation into a minimal command surface

The main operational entrypoint is:

```bash
make run-local
```

which:

* applies Terraform
* starts Docker Compose
* waits for Kestra
* triggers the full batch pipeline

[Back to top](#uk-ev-takeup-data-pipeline)

## Screenshots

### Dashboard

![Dashboard screenshot](docs/images/dashboard_overview.png)

### Kestra batch flow

![Kestra batch flow](docs/images/kestra_batch_flow.png)

### dbt lineage graph

![dbt lineage graph](docs/images/dbt_lineage.png)

### BigQuery marts

![BigQuery marts screenshot](docs/images/bigquery_marts.png)

[Back to top](#uk-ev-takeup-data-pipeline)

---

## Key Findings

Broadly speaking, the dashboard suggests that:

- plugin and electric vehicles are increasing in share over time
- that increase is visible both in new registrations and, more slowly, across the total vehicle stock
- electricity prices have risen strongly over parts of the study period
- the relative cost relationship between electricity and traditional fossil fuels has not always improved in favour of electrification

In short, **electrical cars are increasing in share while the cost of electricity has, at times, risen relatively faster than traditional fossil fuels**.

[Back to top](#uk-ev-takeup-data-pipeline)

---

## Future Improvements

- complete the **cloud mode** by provisioning a VM and running the full stack remotely
- redesign the warehouse to support a **monthly** analytical grain rather than quarterly
- remove hardcoded date bounds from marts to support wider historical windows
- promote prototype detailed-fuel queries from Streamlit into dbt models
- add scheduled monthly batch updates in Kestra
- improve latest-file retrieval logic for each source website
- enrich dashboard controls for detailed vs aggregated series and rebasing options

[Back to top](#uk-ev-takeup-data-pipeline)

---

## Reproducibility

### Prerequisites

Before running the project, the assessor should have:

- Docker and Docker Compose
- Python / `uv`
- Terraform
- Google Cloud SDK (`gcloud`)
- a GCP project
- Application Default Credentials configured via:

```bash
gcloud auth application-default login
```

The assessor must also prepare:

- `.env`
- `terraform/terraform.tfvars`

from the provided example files.

[Back to top](#uk-ev-takeup-data-pipeline)

---

### Quickstart

There are two intended operating modes:

- **Local mode**: implemented now
- **Cloud mode**: planned next phase, where Terraform provisions a VM and the same stack runs remotely

### Local mode: one-command run

After the assessor has:

1. created their GCP project
2. authenticated with `gcloud auth application-default login`
3. copied `.env.example` to `.env`
4. copied `terraform/terraform.tfvars.example` to `terraform/terraform.tfvars`
5. filled both files with their values

they can run the full local workflow with:

``` bash
make run-local
```

This command:

- applies Terraform
- starts Kestra, Postgres, and Streamlit
- waits for Kestra to become reachable
- triggers the full `batch_uk_ev_pipeline` flow

Then:

- monitor Kestra at `http://localhost:8080`
- view the dashboard at `http://localhost:8501`

### Local mode: step-by-step run

If preferred, the assessor can run the pipeline in stages:

``` bash
make terraform-init
make terraform-plan
make terraform-apply
make up
make trigger-batch
```

Optional utility commands:

``` bash
make logs
make dbt-test
make dbt-docs
```

### Cloud mode

Cloud mode is planned but not yet fully implemented. The intended difference is that Terraform will provision a VM and the same pipeline stack will run there instead of on the assessor’s local machine.

[Back to top](#uk-ev-takeup-data-pipeline)

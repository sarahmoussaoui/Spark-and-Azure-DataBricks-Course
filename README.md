# Spark and Azure Databricks Course

Notes, notebooks, and a full end-to-end project from a hands-on **Apache Spark and Azure Databricks** course. The main project builds a data pipeline for **Formula 1 racing data** on Azure: raw files are ingested from Azure Data Lake Storage, transformed with PySpark and Spark SQL, stored as Delta Lake tables, and used to analyze the most dominant drivers and teams.

## Table of Contents

- [Topics Covered](#topics-covered)
- [Repository Structure](#repository-structure)
- [Formula 1 Project](#formula-1-project)
- [Unity Catalog](#unity-catalog)
- [Dataset](#dataset)
- [Documentation](#documentation)
- [Getting Started](#getting-started)

## Topics Covered

- **Azure Databricks:** architecture, clusters and cluster pools, pricing factors, notebooks
- **Securing access to Azure Data Lake:** access keys, SAS tokens, service principals, cluster-scoped credentials, secret scopes (`dbutils.secrets`), credential pass-through
- **Mounting** Azure Data Lake Storage Gen2 containers to DBFS
- **Spark:** cluster architecture, DataFrame and data source APIs, filter / aggregation / join transformations, Databricks workflows
- **Spark SQL:** databases, managed and external tables, views, window functions
- **Data loading patterns:** full load, incremental load, hybrid scenarios
- **Delta Lake:** lakehouse architecture, transaction logs, advanced features
- **Azure Data Factory:** components and pipeline orchestration
- **Unity Catalog:** governance, external locations, catalogs and schemas, medallion architecture

## Repository Structure

```
.
├── formula1 project/
│   ├── Formula1-Project-Solutions.dbc      # Databricks notebook archive (all sections)
│   ├── formula1_ergast_data_user_guide.txt # Ergast database user guide
│   ├── formula1_ergast_db_data_model.png   # Data model diagram
│   └── raw/                                # Raw Formula 1 data (CSV and JSON)
├── incremental_load_data/                  # Three batches of data for the incremental load demo
│   ├── 2021-03-21/
│   ├── 2021-03-28/
│   └── 2021-04-18/
├── unity catalog/
│   ├── databricks-course-uc.dbc            # Unity Catalog notebooks
│   └── (sample files: circuits.csv, drivers.json, results.json)
├── images for CheatSheet/                  # Diagrams used in the documentation
├── Documentation.md                        # Course notes
├── Azure+Databricks+Course+Slide+Deck+V4.pdf
└── README.md
```

## Formula 1 Project

The `Formula1-Project-Solutions.dbc` archive contains the notebooks for each section of the course. Import it into your Databricks workspace (**Workspace → Import**).

| Section | Content |
|---------|---------|
| Section 06 | Accessing Azure Data Lake: access keys, SAS token, service principal, cluster-scoped credentials, secrets utility |
| Section 07-08 | Mounting ADLS containers with a service principal, exploring DBFS |
| Section 11-12-13 | Ingestion notebooks: circuits, races, constructors, drivers, results, pit stops, lap times, qualifying |
| Section 14 | Ingestion with shared `includes` (configuration and common functions) and an "ingest all files" notebook |
| Section 16 | Transformations: race results, driver standings, constructor standings |
| Section 18-19-20 | Spark SQL raw tables, processed database, incremental load, and dominant drivers/teams analysis |
| Section 21 | Final ingestion and transformation pipeline with presentation database and calculated race results |
| Section 22 | Delta Lake version of the project with filter, join, aggregation, and SQL demos |
| End of Course | Complete final version of the project |

### Pipeline Overview

1. **Raw layer:** CSV and JSON files in Azure Data Lake (`raw/`)
2. **Ingestion (processed layer):** read each file with an explicit schema, rename columns, add ingestion date, and write as Parquet / Delta
3. **Transformation (presentation layer):** join the processed tables to produce race results, driver standings, and constructor standings
4. **Analysis:** SQL queries and visualizations to find the most dominant drivers and teams across seasons

## Unity Catalog

The `unity catalog/databricks-course-uc.dbc` archive contains:

- **unity-catalog-introduction:** querying tables through Unity Catalog and accessing external locations
- **unity-catalog-capabilities:** data discovery
- **unity-catalog-mini-project:** creating external locations, catalogs and schemas, and bronze, silver, and gold tables

## Dataset

The project uses the **Ergast Formula 1 dataset** (races from 1950 onward). The raw files include:

| File | Format |
|------|--------|
| `circuits.csv`, `races.csv` | CSV |
| `constructors.json`, `drivers.json`, `results.json`, `pit_stops.json` | JSON |
| `lap_times/` (5 split files) | CSV |
| `qualifying/` (2 split files) | JSON |

The `incremental_load_data/` folder holds three dated batches (2021-03-21, 2021-03-28, 2021-04-18) used to practice incremental loading. See `formula1_ergast_data_user_guide.txt` and the data model diagram for the table definitions.

## Documentation

`Documentation.md` contains my written notes covering every topic in the course, from Databricks architecture and securing Data Lake access to Spark SQL, Delta Lake, and Azure Data Factory.

## Getting Started

1. Create an Azure account with a **Databricks workspace** and an **Azure Data Lake Storage Gen2** account (containers: `raw`, `processed`, `presentation`).
2. Upload the files from `formula1 project/raw/` to the `raw` container.
3. Clone the repository:
   ```bash
   git clone https://github.com/sarahmoussaoui/Spark-and-Azure-DataBricks-Course.git
   ```
4. In Databricks, import `Formula1-Project-Solutions.dbc`.
5. Create a cluster, then set up access to your storage account (the `set-up` notebooks show each method).
6. Update the storage account and container names in the `includes/configuration` notebook, then run the `0.ingest_all_files` notebook followed by the transformation notebooks.

> Never hard-code storage keys, SAS tokens, or service principal secrets in notebooks. Store them in a secret scope and read them with `dbutils.secrets.get()`.

## Credits

The project follows an Azure Databricks and Spark course. The Formula 1 data comes from the [Ergast Developer API](http://ergast.com/mrd/). This repository contains my personal notes and work from the course, for learning purposes only.

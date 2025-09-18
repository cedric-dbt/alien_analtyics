# UFO Data Project

Exploring UFO sightings with real-world context (weather + aviation incidents) using **Snowflake** and **dbt**.

---

## Project Aim

This project centralizes UFO sighting reports and enriches them with **historical weather** and **airplane crash** data to:

- Identify temporal and geographic patterns in sightings.  
- Test environmental correlations (e.g., visibility, wind, precipitation).  
- Provide a reproducible analytics pipeline that demonstrates dbt best practices.  

---

## High-Level Flow

- **RAW Layer**: Data is ingested as-is from source files into Snowflake.  
- **STAGING Models**: Standardization, type casting, and light cleaning.  
- **CORE Models**: Deduplicated and business-ready tables (facts/dimensions).  
- **CURATED/ANALYTICS Models**: Final joined datasets for analysis (sightings + weather + aviation context).  

---

## Data Sources

**Database:** `CEDRIC_TURNER_DEMO`

- **Schema: UFO_RAW (source/landing)**  
  - `UFO_SIGHTINGS_RAW` — reports of UFOs (datetime, city, state, country, shape, duration, comments, lat/long).  
  - `WEATHER_HISTORICAL_DATA` — temperature, humidity, wind, precipitation, visibility, pressure, summaries.  
  - `AIRPLANE_CRASHES_SINCE_1908` — aviation incidents (date, time, location, operator, fatalities, etc.).  

- **Schema: DBT_TARGET (dbt builds)**  
  - `stg_*` models for each raw source.  
  - `*_core_*` models for cleaned, conformed datasets.  
  - `ufo_curated_*` and `ufo_analytics_*` for joined datasets and insights.  

---

## Key Questions

- When and where do UFO sightings cluster? (time of day, season, region)  
- Do weather conditions correlate with higher UFO reports?  
- Are there overlaps between UFO sighting areas and airplane crash sites?  
- Can hotspots or anomalies be detected from the combined datasets?  

---

## Tech Stack

- **Warehouse:** Snowflake  
- **Transformations:** dbt (Core or Cloud)  
- **Testing & Documentation:** dbt tests, `dbt docs`  
- **Visualization:** Optional BI tools or notebooks (Hex, Tableau, etc.)  

---

## Getting Started

### Prerequisites

- Python 3.10+  
- Snowflake account with access to `CEDRIC_TURNER_DEMO`  
- dbt adapter (`dbt-snowflake`)  

### Install

```bash
python -m venv .venv && source .venv/bin/activate
pip install --upgrade pip
pip install dbt-snowflake
```

## Modeling Conventions

- **Staging Models (`stg_*`)**  
  Mirror raw sources with standardized column names, type casting, and surrogate keys.  
  Example: `stg_ufo_sightings` cleans and normalizes `UFO_SIGHTINGS_RAW`.

- **Intermediate Models (`*_int_*`)**  
  Represent cleaned, business-ready datasets (facts and dimensions).  
  Example: `ufo_core_sightings` with deduplicated records, valid timestamps, and consistent location fields.

**Join Logic**  
- **Time-based alignment:** UFO sighting timestamps are matched to the closest weather intervals (rounded to hour/day).  
- **Location-based alignment:** Latitude/longitude from sightings matched against weather stations or crash coordinates within a distance threshold.

**Materializations**  
- **Views:** Default for lightweight staging models.  
- **Tables:** Core models and stable analytics outputs.  
- **Incremental Models:** Large fact tables keyed by unique IDs or event timestamps for efficient refreshes.

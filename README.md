# sctp_module2_team3_mini_proj
Architecture Diagram:
                 ┌──────────┐
Source CSVs  →   │ DuckDB   │  (local landing + validation)
                 └────┬─────┘
                      │
                      ▼
                ┌────────────┐
                │  Meltano   │  (ELT orchestration + Singer taps)
                └────┬───────┘
                     │
                     ▼
                ┌────────────┐
                │ BigQuery   │  (cloud warehouse, curated + serving)
                └────┬───────┘
                     │
                     |
                ┌────────┐
                │  dbt   │
                │models  │
                └────────┘
                     |
                ┌────────────┐
                │ BigQuery   │  (fct tables, dim tables, search_layers)
                └────┬───────┘
                     │
                     |
          ┌──────────┴───────────┐
          ▼                      ▼
     ┌────────────┐             ┌──────────┐
     │  Jypyter   │             │ Hybrid   │
     │(Analysis)  │             │ RAG      │
     └────────────┘             └──────────┘



Tech stack:
| Component     | Tool                                      |
| ------------- | ----------------------------------------- |
| Ingestion     | Pure Python scripts (pandas / connectors) |
| Data Warehouse| BigQuery                                  |
| Transform     | dbt                                       |
| Analysis      | Jupyter Notebooks                         |
| Presentation  | Google Slides / PowerPoint                |


# E-Commerce Data Analysis based on Hybrid RAG Architecture (SCTP Module 2 Project)

This project implements a data pipeline and a Hybrid Retrieval-Augmented Generation (RAG) system to analyze E-Commerce data (Olist dataset). It moves beyond standard SQL reporting by combining analytical precision with vector semantic search to answer "why" specific trends occur (e.g., reasons for negative reviews).

## Features

* **ELT Pipeline:** Extracts local CSV data using **Meltano** (`tap-csv`) and loads it into **Google BigQuery**.
* **Data Transformation:** Uses **dbt** to clean, join, and aggregate raw data into a "Single Source of Truth" (Data Foundation Layer).
* **Semantic Layer:** Transforms structured data into unstructured natural language text for context construction (`dim_embedded_knowledge`).
* **Vector Intelligence:** Utilizes **Vertex AI** to generate embeddings and build BigQuery Vector Indexes (`dim_embedded_vectors`).
* **Hybrid Search Application:** A **Streamlit** GUI ("Olist Smart Data Assistant") that routes queries between SQL analytics and vector semantic search.

## Requirements
* Python 3.10+ 
* Conda
* Google Cloud Platform (GCP) Account with **BigQuery** and **Vertex AI API** enabled 
* Vertex AI Administrator role enabled 
* 
**Meltano** and **dbt** 

## Installation
1. **Environment Setup**
Create and activate the Conda environment:
```bash
conda env create -f proj_env.yml
conda activate proj_env
```

2. **GCP Authentication**
Authenticate with Google Cloud:
```bash
gcloud auth application-default login
```

3. **Meltano Setup**
Initialize the project (if not already done) and add plugins:
```bash
meltano add extractor tap-csv
meltano add loader target-bigquery
```

## Configuration
### Meltano (`meltano.yml`)
Configure `tap-csv` with file paths and keys, and `target-bigquery` with your GCP details:

* 
**Extractors (`tap-csv`):** Map entities (e.g., `raw_orders`, `raw_reviews`) to local CSV paths (`db/olist_orders_dataset.csv`, etc.) and define keys. 


* 
**Loaders (`target-bigquery`):** Set `project`, `dataset`, `location` (US), and `credentials_path`. 



### dbt (`profiles.yml`)

Configure connection to BigQuery using `oauth` or service account. Set the location to **US**. 

## Usage

### 1. Ingest Data (ELT)

Test the configuration and run the pipeline to load CSVs into BigQuery:

```bash
meltano config tap-csv test
meltano run tap-csv target-bigquery

```

*Use `--full-refresh` if a refresh is required.* 

### 2. Transform and Vectorize (dbt)

Run the dbt models in the following order to build the search layers:

```bash
dbt run --select init_search_unioned       # Data Foundation
dbt run --select dim_embedded_knowledge    # Context Construction
dbt run --select dim_embedded_vectors      # Embedding & Indexing

```

### 3. Run Application (GUI)
Launch the Olist Smart Data Assistant:
```bash
streamlit run ./demo2.py
```
## Notes
* 
**Troubleshooting:** If the pipeline breaks, attempt to comment out half of the tables in the configuration and process them in batches. 
* 
**Analysis:** The application includes a "Semantic Galaxy & VoC Analytics" view for visualizing data clusters.

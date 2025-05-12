# Entity Extraction Pipeline

This repository implements an Airflow-based entity extraction and matching pipeline as per the Senior Data Engineer Challenge #2.

## Prerequisites
- **uv**: Package manager (install via `pip install uv` or follow [uv documentation](https://github.com/astral-sh/uv)).
- **Docker**: Container runtime (install from [Docker](https://www.docker.com/)).
- **Python 3.10+**: Required for Airflow and scripts.
- **Git**: To clone the repository.

## Setup Instructions
1. **Clone the Repository**:
   ```bash
   git clone https://github.com/yumanfai/bilby-entity-extraction-pipeline.git
   cd entity-extraction-pipeline
   ```

2. **Set Up Environment**:
   Set `AIRFLOW_HOME` to environment file:
   ```bash
   echo "AIRFLOW_HOME=$(pwd)/" >> .env
   ```

3. **Start Airflow**:
   ```bash
   uv run --env-file .env airflow standalone
   ```
   - Access the Airflow webserver at `http://localhost:8080`.
   - Credentials are displayed in the terminal and saved in `standalone_admin_password.txt`.

4. **Run the DAG**:
   - In the Airflow webserver, enable and trigger the `entity_extraction_pipeline` DAG.
   - Monitor logs for progress and errors.

5. **(Optional) Export SQLite Database to CSV**:
   - To generate a sample CSV output for the challenge deliverables, run:
     ```bash
     python scripts/export_to_csv.py
     ```
   - This creates `output/extracted_entities.csv`, containing all extracted and matched entities from `output/extracted_entities.db`.

## Docker Configuration
- The `Dockerfile` builds an image (`entity-extraction:latest`) for the entity extraction task.
- The GLiNER model is downloaded on the first run and cached in the container's filesystem.
- The container uses volume mounts for input (`/app/input.txt`) and output (`/app/output.json`).

## Output
- Results are stored in `output/extracted_entities.db` (SQLite database).
- Optional CSV output is generated at `output/extracted_entities.csv` using `export_to_csv.py`.
- The schema includes fields for document ID, entity type, text, positions, score, and matching details.

## Database Schema Design
The output is stored in a SQLite database (`output/extracted_entities.db`) with a single table:

```sql
CREATE TABLE extracted_entities (
    document_id TEXT,
    entity_type TEXT,
    entity_text TEXT,
    start_pos INTEGER,
    end_pos INTEGER,
    score REAL,
    is_matched BOOLEAN,
    matched_entity_id TEXT,
    matched_entity_name TEXT
);
```

### Schema Details
- **Fields**:
  - `document_id`: Links to the document (`uuid` in `documents.csv`).
  - `entity_type`: Type of entity (e.g., "Person", "Company", "Location").
  - `entity_text`: Extracted entity text (e.g., "Chairman Xi").
  - `start_pos`, `end_pos`: Character positions in the document text.
  - `score`: GLiNER model confidence score.
  - `is_matched`: Indicates if the entity matched a Source of Truth (SoT) entity.
  - `matched_entity_id`, `matched_entity_name`: SoT entity details (null if unmatched).
- **Design Rationale**:
  - **Single Table**: Simplifies storage and querying for a proof-of-concept.
  - **Normalized**: Avoids duplicating `documents.csv` data, reducing redundancy.
  - **Relational**: Enables easy SQL queries and CSV export.
  - **Nullable Fields**: `matched_entity_id` and `matched_entity_name` allow unmatched entities.
- **Comparison to Example Schema**: Unlike the task’s example (a denormalized table with nested entities and all `documents.csv` fields), this schema is flat and normalized, prioritizing entity-focused queries and simplicity over redundant document data.
- **Remarks**:
  - `matched_entity_id` is included and only copies from `matched_entity_name`, but can be updated when using the actual SoT entities database.
  - `document_id` is used instead of `uuid` to avoid confusion and enhance readability.

### Assumptions and Design Decisions
- **Assumptions**:
  - `documents.csv` has `uuid` and `body_en` columns.
  - `entity_aliases.csv` has `entity_type`, `name`, and `aliases` (comma-separated list).
  - Case-sensitive exact matching for aliases, per the task.
  - Small dataset size, so no primary key or indexes are needed.
  - SQLite is sufficient for local testing; production may use PostgreSQL or consider NoSQL options depending on situations.
- **Design Decisions**:
  - **SQLite**: Chosen for portability and ease of setup in a local environment.
  - **Flat Schema**: Avoids nested JSON (unlike example schema) for simpler querying and CSV export.
  - **Volume Mounts**: Used for Docker I/O to ensure reliable data transfer, avoiding log parsing.
  - **Sequential Processing**: Processes documents one-by-one for simplicity, but parallelization is possible.
  - **Minimal Indexes**: Omitted for small datasets to reduce complexity.

## Deliverables
- **Source Code**: Includes `dags/entity_extraction_dag.py`, `scripts/extract_entities.py`, `scripts/export_to_csv.py`, `Dockerfile`.
- **Docker Configuration**: `Dockerfile` for entity extraction.
- **Documentation**: This README, including schema design and assumptions.
- **Sample Output**: `output/extracted_entities.db` (SQLite) and `output/extracted_entities.csv` (CSV).

## Sample Output
The SQLite database (`output/extracted_entities.db`) contains rows like:
```
document_id: "4ebc2f39-0d4e-4494-ad8c-1d06d914e961"
entity_type: "Person"
entity_text: "Chairman Xi"
start_pos: 10
end_pos: 20
score: 0.936
is_matched: True
matched_entity_id: "Xi Jinping"
matched_entity_name: "Xi Jinping"
```
The CSV (`output/extracted_entities.csv`) mirrors this structure.

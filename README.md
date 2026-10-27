# Urban Mobility Platform

A local-first data engineering project that processes historical Citi Bike trips into analytics-ready PostgreSQL tables using Python, dbt, and Apache Airflow.

The project demonstrates a monthly batch pipeline: download public trip archives, extract CSV files, load a raw layer, and build analytical models. Development targets **Windows with WSL 2 / Ubuntu**, VS Code, and Docker Compose.

> **Status:** local development. GitHub Actions CI/CD is planned, not implemented. The repository is not a production deployment template. See [Known limitations](#known-limitations) before scheduling or reprocessing data.

## Contents

- [Architecture](#architecture)
- [Technology stack](#technology-stack)
- [Repository layout](#repository-layout)
- [Data source](#data-source)
- [Local setup](#local-setup)
- [Run ingestion locally](#run-ingestion-locally)
- [Run Airflow](#run-airflow)
- [Run dbt](#run-dbt)
- [Analytical models](#analytical-models)
- [Data quality](#data-quality)
- [Scheduling and backfills](#scheduling-and-backfills)
- [Operations](#operations)
- [Troubleshooting](#troubleshooting)
- [Known limitations](#known-limitations)
- [Roadmap](#roadmap)

## Architecture

```text
Citi Bike monthly ZIP archives
              |
              v
Python ingestion
 download -> extract -> load
              |
              v
PostgreSQL warehouse
 raw.citibike_trips
              |
              v
 dbt staging views
 public_staging.stg_citibike_trips
              |
              v
 dbt analytical tables
 public_marts.fct_*

Apache Airflow orchestrates:
 download_trips -> extract_trips -> load_trips -> dbt_run -> dbt_test
```

The warehouse and Airflow infrastructure are defined in **two separate Compose projects** connected through the external `urban-mobility-network` network.

| Component | Responsibility |
|---|---|
| Warehouse PostgreSQL | Stores raw trips and dbt models |
| Airflow metadata PostgreSQL | Stores orchestration metadata, not analytical trips |
| Redis | Message broker for the configured CeleryExecutor |
| Airflow services | API/UI, scheduler, DAG processor, worker, and triggerer |
| Local filesystem | Holds downloaded archives and extracted CSVs |

The root `compose.yaml` starts **only the warehouse**. It does not start the Airflow UI.

## Technology stack

| Technology | Purpose | Configuration |
|---|---|---|
| Python | Ingestion and validation | `>=3.14`; `.python-version` selects 3.14 locally |
| uv | Local dependency management | `pyproject.toml` and `uv.lock` |
| pandas | Chunked CSV processing | Local version resolved in `uv.lock` |
| SQLAlchemy / psycopg | Local PostgreSQL access | Declared in `pyproject.toml` |
| PostgreSQL | Warehouse and Airflow metadata | PostgreSQL 16 images |
| Apache Airflow | Monthly orchestration | Base image `apache/airflow:3.3.2` |
| dbt Core / dbt-postgres | SQL transformations and data tests | Installed in the Airflow image |
| Docker Compose | Local infrastructure | Root and Airflow Compose files |
| pytest / Ruff | Unit tests, linting, and formatting | uv development dependency group |

**Dependency boundary:** `uv sync` configures the local ingestion/test environment. It does not install Airflow or dbt. The Airflow image installs its own dependencies from `airflow/docker/requirements.txt`; these are not currently governed by `uv.lock`.

## Repository layout

```text
.
├── airflow/
│   ├── dags/                         # Pipeline, diagnostic DAGs, and callbacks
│   ├── docker/
│   │   ├── config/airflow.cfg         # Configuration mounted by the base Compose
│   │   ├── docker-compose.yaml       # Airflow, metadata database, and Redis
│   │   ├── docker-compose.override.yaml
│   │   ├── dockerfile                # Extended Airflow dependency image
│   │   └── requirements.txt
│   ├── config/
│   └── plugins/
├── data/
│   ├── raw/                          # Downloaded ZIP archives; ignored by Git
│   └── extracted/                    # Extracted CSV files; ignored by Git
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   ├── tests/
│   ├── dbt_project.yml
│   └── profiles.yml
├── src/
│   ├── download_trips.py
│   ├── extract_trips.py
│   └── load_trips.py
├── tests/
├── .env.example
├── .python-version
├── compose.yaml
├── pyproject.toml
└── uv.lock
```

## Data source

Trip histories are published through [Citi Bike System Data](https://citibikenyc.com/system-data). Access and use are subject to the [Citi Bike Data Sharing Policy](https://citibikenyc.com/data-sharing-policy).

The downloader currently builds URLs in this format:

```text
https://s3.amazonaws.com/tripdata/YYYYMM-citibike-tripdata.zip
```

The loader expects the modern trip schema, including `ride_id`, `rideable_type`, `started_at`, `ended_at`, station fields, coordinates, and `member_casual`. Older files using a different schema are not supported without additional normalization. Verify that the selected archive exists and matches the expected schema.

A monthly archive may contain multiple CSVs. The loader recursively discovers CSV files inside the specified input directory.

## Local setup

Run the following commands in the **WSL Ubuntu terminal**, from the repository root unless stated otherwise. Use VS Code connected to WSL and Docker Desktop with WSL integration enabled, or an equivalent Docker installation available inside WSL.

### Prerequisites

- Git
- [uv](https://docs.astral.sh/uv/getting-started/installation/)
- Docker Engine and Docker Compose v2
- Python 3.14, installed directly or managed by uv
- Available host ports `5432` and `8080`, unless the corresponding configuration is changed
- Sufficient disk space for monthly ZIPs, extracted CSVs, and PostgreSQL storage

Check the command-line tools:

```bash
git --version
uv --version
docker version
docker compose version
```

### Clone and synchronize

```bash
git clone https://github.com/DanRQ/urban-mobility-platform.git
cd urban-mobility-platform

uv python install 3.14
uv sync --locked --dev
```

[uv](https://docs.astral.sh/uv/guides/projects/) creates or synchronizes `.venv`. Virtual-environment activation is not required when using `uv run`. Keep `uv.lock` in Git; do not commit `.venv`.

### Configure the warehouse

For a new checkout, create the local environment file:

```bash
cp .env.example .env
```

Do not overwrite an existing `.env` containing your configuration. The supplied example uses local-development values:

```dotenv
WAREHOUSE_USER=warehouse_user
WAREHOUSE_PASSWORD=warehouse_password
WAREHOUSE_DB=warehouse_db
WAREHOUSE_HOST=localhost
WAREHOUSE_PORT=5432
```

| Execution location | Warehouse host | Warehouse port |
|---|---|---|
| Python running in WSL | `localhost` | Published port from `.env`, initially `5432` |
| Airflow containers | `warehouse-db` | Container port `5432` |

The Airflow Compose explicitly overrides the warehouse host and port for its containers. Keep `localhost` in the root `.env` when running ingestion directly from WSL.

### Start PostgreSQL

```bash
mkdir -p data/raw data/extracted

if ! docker network inspect urban-mobility-network >/dev/null 2>&1; then
  docker network create urban-mobility-network
fi

docker compose --env-file .env up -d warehouse-db
docker compose --env-file .env ps
```

Check readiness:

```bash
docker compose --env-file .env exec -T warehouse-db sh -c \
  'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

The ingestion loader creates `raw.citibike_trips` if necessary. The root Compose also declares a `sql/init` mount, but this repository currently contains no initialization SQL there; do not depend on that mount to initialize application tables.

## Run ingestion locally

Use one **published historical month** for the first run. The following example uses January 2024:

```bash
MONTH=202401

uv run --locked python src/download_trips.py \
  --month "$MONTH" \
  --output-dir data/raw

uv run --locked python src/extract_trips.py \
  --zip-path "data/raw/${MONTH}-citibike-tripdata.zip" \
  --output-dir "data/extracted/${MONTH}"

uv run --locked python src/load_trips.py \
  --input-dir "data/extracted/${MONTH}" \
  --month "$MONTH"
```

> **Reprocessing warning:** the loader deletes existing rows for `--month` before loading the CSVs. The replacement is not currently atomic. Check that all input files belong to that month, avoid concurrent loads, and do not use this command against a shared or production database without additional safeguards.

The raw table includes ingestion metadata:

| Column | Meaning |
|---|---|
| `source_month` | Month extracted from the CSV filename |
| `source_file` | CSV filename |
| `loaded_at` | Insertion timestamp generated by PostgreSQL |

Inspect loaded rows:

```bash
docker compose --env-file .env exec -T warehouse-db sh -c \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
    SELECT source_month, COUNT(*) AS total_rows
    FROM raw.citibike_trips
    GROUP BY source_month
    ORDER BY source_month;
  "'
```

## Run Airflow

Airflow is optional for the manual ingestion steps above. To execute the orchestrated pipeline or run dbt inside the provided image, configure the separate Airflow stack.

> These commands document the current two-Compose layout. Image build and runtime compatibility must still be verified in your Docker environment. The main DAG currently lacks an explicit `start_date`; add a fixed, timezone-aware start date before relying on scheduled runs. See [Known limitations](#known-limitations).

### Configure Airflow variables

For an initial setup, generate local keys and append them to the root `.env` **once**. Preserve an existing Fernet key after storing encrypted Airflow connections; do not regenerate it on every startup.

```bash
FERNET_KEY="$(uv run --no-project --with cryptography python -c \
  'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')"

JWT_SECRET="$(uv run --no-project python -c \
  'import secrets; print(secrets.token_hex(32))')"

printf '\nAIRFLOW_UID=%s\nFERNET_KEY=%s\nAIRFLOW__API_AUTH__JWT_SECRET=%s\n' \
  "$(id -u)" "$FERNET_KEY" "$JWT_SECRET" >> .env

unset FERNET_KEY JWT_SECRET
```

The temporary `uv run --no-project --with cryptography` invocation generates a key without adding a project dependency. Review `.env` for duplicate settings if you have configured Airflow previously.

Also add local administrator credentials to `.env`:

```dotenv
_AIRFLOW_WWW_USER_USERNAME=local_admin
_AIRFLOW_WWW_USER_PASSWORD=replace-with-your-own-local-password
```

Replace the example password before initialization. Keep `.env` untracked and never use these development settings as a production security configuration.

### Select the Compose files

Define this helper in the current Bash session, **from the repository root**:

```bash
mkdir -p airflow/logs airflow/plugins

export ENV_FILE_PATH="$PWD/.env"

airflow_compose() {
  docker compose \
    --project-name urban-mobility-airflow \
    --env-file "$ENV_FILE_PATH" \
    -f airflow/docker/docker-compose.yaml \
    -f airflow/docker/docker-compose.override.yaml \
    "$@"
}
```

The explicit project name keeps the Airflow services separate from the warehouse project. `ENV_FILE_PATH` points the service-level `env_file` at the root `.env`; `--env-file` supplies values for Compose interpolation. Both paths are configured intentionally; [Compose treats service `env_file` paths separately](https://docs.docker.com/compose/how-tos/environment-variables/set-environment-variables/) from CLI interpolation settings.

Do not combine the root and Airflow files into one `-f` list: they currently use different relative-path bases. Redefine the helper when opening a new terminal.

Validate configuration without printing resolved secrets:

```bash
airflow_compose config --quiet
```

### Build and initialize

Build the existing lowercase `dockerfile` explicitly, then initialize and start the services:

```bash
docker build \
  --file airflow/docker/dockerfile \
  --tag urban-mobility-airflow:3.3.2 \
  airflow/docker

airflow_compose up --no-build airflow-init
```

Continue only if `airflow-init` exits successfully:

```bash
airflow_compose up -d --no-build
airflow_compose ps
```

Open **http://localhost:8080** and sign in with the administrator credentials configured before initialization. Existing metadata volumes retain their users; changing environment values alone is not a password-reset procedure.

The main DAG ID is `citibike_tripdata_pipeline`. New DAGs are configured as paused; inspect imports, scheduling settings, and the intended processing interval before unpausing it.

Useful checks:

```bash
airflow_compose exec -T airflow-scheduler airflow dags list

airflow_compose exec -T airflow-scheduler airflow dags list-import-errors

airflow_compose exec -T airflow-worker python -c \
  'import pandas, sqlalchemy, psycopg; print("Ingestion imports available")'

airflow_compose exec -T airflow-worker dbt --version
```

A diagnostic listing is not a substitute for an automated test that fails on import errors.

## Run dbt

The local uv environment does **not** include dbt. The following commands use the running Airflow worker, where the dbt project is mounted at `/opt/airflow/dbt`.

After loading raw data:

```bash
airflow_compose exec -T airflow-worker dbt debug \
  --project-dir /opt/airflow/dbt \
  --profiles-dir /opt/airflow/dbt

airflow_compose exec -T airflow-worker dbt build \
  --project-dir /opt/airflow/dbt \
  --profiles-dir /opt/airflow/dbt
```

`dbt build` materializes models and runs their tests. The Airflow DAG currently uses separate `dbt run` and `dbt test` tasks.

The default profile uses `public` as the target schema. Combined with [dbt's default custom-schema naming](https://docs.getdbt.com/docs/build/custom-schemas), this produces:

| Layer | Schema | Materialization |
|---|---|---|
| Raw | `raw` | Python-managed table |
| Staging | `public_staging` | dbt views |
| Marts | `public_marts` | dbt tables |

To use dbt directly from WSL instead, provision a separate compatible dbt environment and export the `WAREHOUSE_*` settings. Do not assume that dbt automatically reads `.env` or that `uv sync` installs dbt in this repository.

## Analytical models

All marts depend on `stg_citibike_trips`, which derives trip date, hour, duration, and straight-line station distance. The staging model excludes records with missing trip timestamps or an end time earlier than the start time.

| Model | Grain | Main outputs |
|---|---|---|
| `fct_daily_trip_metrics` | One row per trip date | Trip counts, rider/bike segments, active start stations, average and median duration |
| `fct_hourly_trip_metrics` | One row per date and start hour | Hourly totals and member/casual counts |
| `fct_member_type_metrics` | One row per date and rider type | Trip totals and average/minimum/maximum duration |
| `fct_bike_type_metrics` | One row per date and bike type | Trip totals and average/minimum/maximum duration |
| `fct_station_trip_metrics` | One row per start-station ID/name pair | Counts and average duration across all loaded months |
| `fct_route_daily_metrics` | One row per station name and date | Counts of trips starting and ending at the station |

Despite its current name, `fct_route_daily_metrics` does **not** model origin/destination route pairs. `stations_distance_km` is the straight-line distance between station coordinates, not the distance actually ridden.

Example query after a successful dbt build:

```sql
SELECT
    trip_date,
    total_trips,
    member_trips,
    casual_trips,
    avg_trip_duration_minutes
FROM public_marts.fct_daily_trip_metrics
ORDER BY trip_date DESC
LIMIT 10;
```

## Data quality

Current validation includes:

- Unit tests for URL construction, source-month extraction, and required-column validation.
- Required-column checks while CSV chunks are processed.
- dbt source tests for identifiers, timestamps, rider types, and bike types.
- dbt mart tests for required values, accepted domains, and selected uniqueness rules.

Run local tests and static checks:

```bash
uv lock --check
uv run --locked pytest -q
uv run --locked ruff check .
uv run --locked ruff format --check .
```

Ruff is installed, but linting and formatting are not yet clean across the repository. The unit suite does not currently validate PostgreSQL transactions, HTTP failures, ZIP extraction, or Airflow execution. Passing unit tests alone does not establish end-to-end correctness.

## Scheduling and backfills

The main DAG declares `schedule="@monthly"` and `catchup=False`. The processing month is derived from the data interval, not the wall-clock month:

```jinja
{{ data_interval_start.strftime('%Y%m') }}
```

An interval starting on `2024-01-01` selects `202401`. Disabling catchup prevents automatic creation of all historical scheduled runs; historical processing must be requested explicitly.

Before creating a backfill:

1. Add an explicit fixed, timezone-aware `start_date` to the main DAG and validate its import.
2. Confirm that each archive is available and uses the supported schema.
3. Review existing data because monthly reprocessing deletes and reloads rows.
4. Limit concurrent backfill runs to avoid overlapping writes and mart rebuilds.
5. Preview the selected logical dates before executing.

Inspect the installed CLI and preview an example historical range; see the [Airflow backfill documentation](https://airflow.apache.org/docs/apache-airflow/3.3.2/core-concepts/backfill.html) for the scheduling and reprocessing rules:

```bash
airflow_compose exec -T airflow-scheduler airflow backfill create --help

airflow_compose exec -T airflow-scheduler airflow backfill create \
  --dag-id citibike_tripdata_pipeline \
  --from-date 2024-01-01 \
  --to-date 2024-03-01 \
  --max-active-runs 1 \
  --dry-run
```

Review the dry-run output and existing run states. Remove `--dry-run` only when the proposed intervals are correct and you are ready to load them. Backfill concurrency does not replace a deliberate DAG-level concurrency policy.

## Operations

### Inspect logs

```bash
docker compose --env-file .env logs --tail=100 warehouse-db

airflow_compose logs --tail=100 airflow-scheduler airflow-worker
```

Task output is also available in the Airflow UI. The current CLI scripts do not configure INFO logging themselves, so some application messages may be absent until logging setup is added to their entrypoints.

## Known limitations

- **Non-atomic reloads:** the existing month is deleted before all files are validated and inserted; a failure can leave missing or partial data.
- **Month consistency:** the deletion month comes from the CLI, while inserted `source_month` comes from filenames; calendar and cross-file consistency checks need improvement.
- **Split dependency management:** local uv dependencies and image requirements can diverge; image requirements are not version-pinned.
- **Partial downloads:** the downloader writes directly to the final ZIP path rather than publishing a validated temporary file atomically.
- **Development image:** application code, DAGs, and dbt files are supplied through bind mounts, not bundled into a release image.
- **Test coverage:** database integration, rollback behavior, HTTP/extraction failures, and automated DAG validation are not yet covered.

## Roadmap

The following items are planned, not implemented:

- [ ] Clean Ruff lint and formatting findings.
- [ ] Add GitHub Actions checks for locked dependencies, linting, formatting, and unit tests.
- [ ] Align and version the ingestion, dbt, and Airflow runtime dependencies.
- [ ] Add atomic monthly replacement and strict source-month validation.
- [ ] Add PostgreSQL/dbt integration tests and automated DAG validation.
- [ ] Complete the scheduling, concurrency, and CLI logging configuration.
- [ ] Package a versioned deployable artifact and define a delivery/rollback procedure.

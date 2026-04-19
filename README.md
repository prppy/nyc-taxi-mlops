# nyc-taxi-mlops

# Project Structure

```
nyc-taxi-mlops/
├── .github/
│   └── workflows/          # CI/CD pipelines (linting, tests, automated checks)
│
├── data/
│   └── taxi_zones/         # Raw shapefiles (.shp) for NYC taxi zone boundaries
│
├── docs/                   # Project documentation and design notes
│
├── source/                 # All application source code
│   ├── devops/
│   │   ├── backend/        # Python backend (Flask/FastAPI) + utility scripts
│   │   │   └── build_zone_shapes.py  # Rebuilds zone_shapes.json from shapefile
│   │   ├── frontend/       # TypeScript/React frontend app
│   │   │   └── src/
│   │   │       └── data/   # Static data assets (e.g. zone_shapes.json)
│   │   ├── Dockerfile      # Devops-specific container image
│   │   └── requirements.txt
│   └── ...                 # ETL DAGs, MLOps DAGs, model training code
│
├── .gitignore
├── Dockerfile              # Main application container
├── docker-compose.yml      # Full stack: Airflow, MLflow, Marquez, frontend, backend
├── docker-compose.prod.yml # Production-optimised compose configuration
├── config.yml              # Shared project configuration (paths, parameters)
├── pyproject.toml          # Python project metadata and tooling config (ruff, pytest)
└── README.md
```

## Key Directories at a Glance

- **`source/`** — All runnable code: DAGs, ML pipelines, backend API, frontend UI
- **`source/devops/backend/`** — Python backend services and utility scripts
- **`source/devops/frontend/`** — React frontend for visualising taxi demand and outputs
- **`data/`** — Static datasets (e.g. taxi zone shapefiles)
- **`docs/`** — Architecture diagrams and design documentation
- **`.github/workflows/`** — CI/CD pipelines (pytest + ruff)

## Infrastructure Files

- **`docker-compose.yml`** — Full local stack (Airflow, PostgreSQL, MLflow, Marquez, frontend, backend)
- **`docker-compose.prod.yml`** — Production configuration
- **`Dockerfile`** — Base image used across services
- **`config.yml`** — Centralised config for DAGs and scripts
- **`pyproject.toml`** — Tooling config (linting + testing)

# Running the project

to run the docker image:

first time setup

- docker compose up --build -d
- docker compose up airflow-init

start docker

- docker compose up

stop docker

- docker compose stop

docker full shutdown

- docker compose down

# if you want to run only the etl dags:

- having all the services (including the mlops, frontend and backend)running may be too tough on your computer
- consider using this command: docker compose up --build postgres airflow-init airflow-scheduler airflow-webserver marquez-db marquez-api marquez-web -d

# if you want to run only the mlops dags:

- consider using this command: docker compose up --build postgres mlflow mlops -d

## Rebuild frontend zone shapes from shapefile

`source/devops/frontend/src/data/zone_shapes.json` can be rebuilt from `data/taxi_zones/taxi_zones.shp` with:

### Local Python

1. Install dependency:
   - `python3 -m pip install pyshp`
2. Run generator from repo root:
   - `python3 source/devops/backend/build_zone_shapes.py`

Optional flags:

- `--input <path/to/taxi_zones.shp>`
- `--output <path/to/zone_shapes.json>`
- `--precision 6`
- `--simplify-step 4`

### Docker (no local Python deps)

Run from repo root:

1. Build the devops image (installs from `source/devops/requirements.txt`):
   - `docker build -f source/devops/Dockerfile -t nyc-taxi-devops .`
2. Run the generator inside that image:
   - `docker run --rm -v "$PWD":/app -w /app nyc-taxi-devops python source/devops/backend/build_zone_shapes.py`

# Devops (testing)

Run all tests locally

- pytest tests/ -v # Should see 43 passed  


Run linter locally

- ruff check source/ # Should see "All checks passed!"

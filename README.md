# nyc-taxi-mlops

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
- consider using this command: docker compose up --build postgres airflow-init mlops -d



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

# DevOps

This directory contains the **application delivery and serving layer** of the NYC Taxi project. It is responsible for exposing the trained model and processed data through backend APIs, and for providing the interactive frontend interface used to explore predictions, weather context, and drift monitoring outputs.

The DevOps layer connects the project’s upstream data and machine learning pipelines to the end user by packaging the backend and frontend into deployable services.

---

## Responsibilities

The `devops` module handles the following core tasks:

- **Backend API serving**  
  Exposes Flask endpoints for prediction, drift monitoring, and supporting backend services.

- **Frontend application delivery**  
  Hosts the React-based user interface used to interact with the system.

- **Prediction integration**  
  Connects user inputs from the frontend to backend inference workflows and returns ranked taxi zone recommendations.

- **Weather and zone enrichment**  
  Supports backend logic for weather lookup, nearby zone calculation, and zone metadata used during prediction.

- **Shape and map support**  
  Stores and prepares zone shape data required for frontend visualisation and geographic interaction.

- **Containerised deployment support**  
  Provides separate Dockerfiles for backend and frontend services so both can be built and deployed consistently.

---

## Directory Structure

The structure of this folder is as follows:

```text
devops/
├── backend/
│   ├── app.py
│   ├── build_zone_shapes.py
│   ├── model.py
│   ├── weather.py
│   ├── zone_shapes.json
│   └── zones.py
├── frontend/
│   ├── public/
│   ├── src/
│   ├── .gitignore
│   ├── eslint.config.js
│   ├── index.html
│   ├── package-lock.json
│   ├── package.json
│   ├── tsconfig.app.json
│   ├── tsconfig.json
│   ├── tsconfig.node.json
│   └── vite.config.ts
├── tests/
├── Dockerfile.backend
├── Dockerfile.frontend
└── README.md
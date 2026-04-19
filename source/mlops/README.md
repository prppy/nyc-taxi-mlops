# MLOps

This directory contains the **machine learning operations layer** of the NYC Taxi project. It is responsible for exploratory analysis, feature engineering, model training, evaluation, drift monitoring, and workflow orchestration for the demand forecasting pipeline.

The MLOps layer ensures that processed taxi and weather data can be transformed into reliable forecasting models, monitored over time, and retrained when data or model behaviour changes.

---

## Responsibilities

The `mlops` module handles the following core tasks:

- **Exploratory data analysis**  
  Investigates processed training data, identifies patterns, and supports feature selection and modelling decisions.

- **Feature engineering**  
  Builds the model-ready feature set used for training and inference, including temporal, weather, lag, and zone-related predictors.

- **Model training**  
  Trains the demand forecasting model and registers production-ready model artifacts for downstream inference.

- **Model evaluation**  
  Measures model performance using validation metrics and supports comparison across training runs.

- **Drift detection and monitoring**  
  Computes feature, label, and model drift statistics to track whether production data is deviating from training-time behaviour.

- **Pipeline orchestration**  
  Provides Airflow tasks and DAG logic so training, evaluation, and monitoring workflows can run automatically on schedule.

- **Model asset management**  
  Stores trained model outputs, configuration files, and metadata needed for reproducible deployment and inference.

---

## Directory Structure

The structure of this folder is as follows:

```text
mlops/
├── dags/
├── final_model_spark/
├── monitor/
├── Dockerfile
├── drift_detector.py
├── eda.ipynb
├── eda.py
├── evaluate.py
├── feature_engineering.py
├── model_name.txt
├── README.md
├── requirements.txt
├── tasks.py
└── train.py
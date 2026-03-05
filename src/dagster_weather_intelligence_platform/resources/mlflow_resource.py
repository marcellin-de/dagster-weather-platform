from dagster import EnvVar
from dagster_mlflow import mlflow_tracking

mlflow_resource = mlflow_tracking.configured(
    {
        "tracking_uri": EnvVar("MLFLOW_TRACKING_URI"),
        "experiment_name": EnvVar("MLFLOW_EXPERIMENT_NAME"),
    }
)

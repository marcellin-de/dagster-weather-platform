from dagster import AssetCheckExecutionContext, AssetCheckResult, AssetCheckSeverity, asset_check


@asset_check(asset="train_temp_forecast_model", blocking=True)
def model_mae_threshold(context: AssetCheckExecutionContext, train_temp_forecast_model: dict) -> AssetCheckResult:
    trained = bool(train_temp_forecast_model.get("trained", False))
    mae = train_temp_forecast_model.get("mae")
    if not trained or mae is None:
        return AssetCheckResult(
            passed=True,
            severity=AssetCheckSeverity.WARN,
            metadata={
                "status": "skipped",
                "reason": "insufficient_history_for_training",
                "available_days": int(train_temp_forecast_model.get("available_days", 0)),
                "required_days": int(train_temp_forecast_model.get("required_days", 5)),
            },
        )

    mae = float(mae)
    # We want MAE < 3 degrees before using the model for forecasting.
    threshold = 3.0
    passed = mae <= threshold

    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "mae": mae,
            "threshold": threshold,
        },
    )

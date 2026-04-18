"""
Unit tests for drift detection pure functions.

Tests compute_relative_shift, drift_label, should_alert, get_feature_type
from drift_detector.py without requiring PySpark or database connections.
"""
import os
import sys
from unittest.mock import MagicMock

# Mock heavy dependencies before importing drift_detector
# pandas, PySpark, dotenv, and airflow are not needed for pure function tests
sys.modules["pandas"] = MagicMock()
sys.modules["pyspark"] = MagicMock()
sys.modules["pyspark.sql"] = MagicMock()
sys.modules["pyspark.sql.functions"] = MagicMock()
sys.modules["pyspark.sql.types"] = MagicMock()
sys.modules["pyspark.ml"] = MagicMock()
sys.modules["pyspark.ml.feature"] = MagicMock()
sys.modules["pyspark.ml.regression"] = MagicMock()
sys.modules["pyspark.ml.evaluation"] = MagicMock()
sys.modules["dotenv"] = MagicMock()
sys.modules["airflow"] = MagicMock()
sys.modules["airflow.utils"] = MagicMock()
sys.modules["airflow.utils.email"] = MagicMock()

# Walk up: tests/ → devops/ → source/
SOURCE_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, SOURCE_ROOT)

from mlops.drift_detector import (  # noqa: E402
    compute_relative_shift,
    drift_label,
    should_alert,
    get_feature_type,
    CRITICAL_THRESHOLD,
    HIGH_THRESHOLD,
    MEDIUM_THRESHOLD,
)


class TestComputeRelativeShift:
    def test_no_change(self):
        assert compute_relative_shift(100, 100) == 0.0

    def test_positive_shift(self):
        result = compute_relative_shift(100, 150)
        assert result == 0.5

    def test_negative_shift(self):
        result = compute_relative_shift(100, 50)
        assert result == 0.5

    def test_both_zero(self):
        assert compute_relative_shift(0, 0) == 0.0

    def test_reference_zero_current_nonzero(self):
        assert compute_relative_shift(0, 5) == 1.0

    def test_clamped_to_one(self):
        """Large shifts should be clamped to 1.0."""
        result = compute_relative_shift(1, 100)
        assert result == 1.0

    def test_small_shift(self):
        result = compute_relative_shift(100, 105)
        assert abs(result - 0.05) < 1e-6


class TestDriftLabel:
    def test_critical(self):
        assert drift_label(CRITICAL_THRESHOLD) == "Critical"
        assert drift_label(0.95) == "Critical"

    def test_high(self):
        assert drift_label(HIGH_THRESHOLD) == "High"
        assert drift_label(0.70) == "High"

    def test_medium(self):
        assert drift_label(MEDIUM_THRESHOLD) == "Medium"
        assert drift_label(0.40) == "Medium"

    def test_healthy(self):
        assert drift_label(0.0) == "Healthy"
        assert drift_label(0.29) == "Healthy"


class TestShouldAlert:
    def test_critical_triggers_alert(self):
        report = {"criticalCount": 1, "highDriftCount": 0}
        assert should_alert(report) is True

    def test_high_drift_triggers_alert(self):
        report = {"criticalCount": 0, "highDriftCount": 2}
        assert should_alert(report) is True

    def test_no_drift_no_alert(self):
        report = {"criticalCount": 0, "highDriftCount": 0}
        assert should_alert(report) is False

    def test_single_high_drift_no_alert(self):
        """Only 1 high-drift feature should NOT trigger alert (need >= 2)."""
        report = {"criticalCount": 0, "highDriftCount": 1}
        assert should_alert(report) is False


class TestGetFeatureType:
    def test_numeric_feature(self):
        assert get_feature_type("target_demand") == "numeric_mean_shift"
        assert get_feature_type("temperature_mean") == "numeric_mean_shift"

    def test_categorical_feature(self):
        assert get_feature_type("borough_manhattan") == "categorical_proportion_shift"

    def test_unknown_feature(self):
        assert get_feature_type("nonexistent_feature") == "unknown"

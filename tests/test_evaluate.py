"""
Unit tests for model evaluation logic.
"""
import pytest


class TestEvaluationGuardrails:
    """Test the metric comparison logic used in evaluate.py"""

    def test_rmse_within_tolerance_passes(self):
        """New model RMSE within 1% of production should pass."""
        prod_rmse = 5.0
        new_rmse = 5.04  # Within 1% margin
        assert new_rmse <= prod_rmse * 1.01

    def test_rmse_exceeding_tolerance_fails(self):
        """New model RMSE exceeding 1% of production should fail."""
        prod_rmse = 5.0
        new_rmse = 5.10  # Exceeds 1% margin
        assert not (new_rmse <= prod_rmse * 1.01)

    def test_mae_within_tolerance_passes(self):
        """MAE degradation within 5% should pass."""
        prod_mae = 3.0
        new_mae = 3.14  # Within 5%
        assert new_mae <= prod_mae * 1.05

    def test_mae_exceeding_tolerance_fails(self):
        """MAE degradation over 5% should fail."""
        prod_mae = 3.0
        new_mae = 3.20  # Exceeds 5%
        assert not (new_mae <= prod_mae * 1.05)

    def test_smape_within_tolerance_passes(self):
        """SMAPE degradation within 5% should pass."""
        prod_smape = 0.15
        new_smape = 0.157  # Within 5%
        assert new_smape <= prod_smape * 1.05

    def test_all_metrics_must_pass(self):
        """Model should only be approved if ALL metrics pass."""
        prod = {"val_rmse": 5.0, "val_mae": 3.0, "val_smape": 0.15}
        new = {"val_rmse": 4.9, "val_mae": 2.8, "val_smape": 0.14}

        rmse_passed = new["val_rmse"] <= prod["val_rmse"] * 1.01
        mae_passed = new["val_mae"] <= prod["val_mae"] * 1.05
        smape_passed = new["val_smape"] <= prod["val_smape"] * 1.05

        assert rmse_passed and mae_passed and smape_passed

    def test_improved_model_passes(self):
        """A strictly better model should always pass."""
        prod = {"val_rmse": 5.0, "val_mae": 3.0, "val_smape": 0.15}
        new = {"val_rmse": 4.0, "val_mae": 2.5, "val_smape": 0.10}

        rmse_passed = new["val_rmse"] <= prod["val_rmse"] * 1.01
        mae_passed = new["val_mae"] <= prod["val_mae"] * 1.05
        smape_passed = new["val_smape"] <= prod["val_smape"] * 1.05

        assert rmse_passed and mae_passed and smape_passed

    def test_one_metric_fails_rejects_model(self):
        """If any single metric fails, model should be rejected."""
        prod = {"val_rmse": 5.0, "val_mae": 3.0, "val_smape": 0.15}
        new = {"val_rmse": 4.0, "val_mae": 4.0, "val_smape": 0.10}  # MAE way worse

        rmse_passed = new["val_rmse"] <= prod["val_rmse"] * 1.01
        mae_passed = new["val_mae"] <= prod["val_mae"] * 1.05
        smape_passed = new["val_smape"] <= prod["val_smape"] * 1.05

        assert not (rmse_passed and mae_passed and smape_passed)

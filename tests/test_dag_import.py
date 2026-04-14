"""
DAG validation tests.

Verifies that all Airflow DAG files have valid Python syntax
and contain proper DAG definitions. This is a standard Airflow CI practice
that catches syntax errors and missing imports before deployment.
"""
import ast
import os
import pytest

# Paths to DAG files
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DAG_FILES = {
    "taxi_data_pipeline": os.path.join(PROJECT_ROOT, "source", "dataops", "dag.py"),
    "ml_training_pipeline": os.path.join(PROJECT_ROOT, "source", "mlops", "dags", "training_dag.py"),
    "drift_monitoring": os.path.join(PROJECT_ROOT, "source", "mlops", "dags", "monitoring_dag.py"),
}


class TestDAGSyntax:
    """Verify all DAG files have valid Python syntax."""

    @pytest.mark.parametrize("dag_id,dag_path", DAG_FILES.items())
    def test_dag_file_has_valid_syntax(self, dag_id, dag_path):
        """Each DAG file should parse without syntax errors."""
        assert os.path.exists(dag_path), f"DAG file not found: {dag_path}"

        with open(dag_path, "r") as f:
            source = f.read()

        # ast.parse will raise SyntaxError if file has invalid syntax
        try:
            ast.parse(source)
        except SyntaxError as e:
            pytest.fail(f"Syntax error in {dag_id} ({dag_path}): {e}")


class TestDAGDefinitions:
    """Verify DAG files contain proper DAG definitions."""

    @pytest.mark.parametrize("dag_id,dag_path", DAG_FILES.items())
    def test_dag_file_contains_dag_definition(self, dag_id, dag_path):
        """Each DAG file should contain a DAG() instantiation."""
        with open(dag_path, "r") as f:
            source = f.read()

        assert "DAG(" in source, f"{dag_id} does not contain a DAG() definition"

    @pytest.mark.parametrize("dag_id,dag_path", DAG_FILES.items())
    def test_dag_file_has_correct_dag_id(self, dag_id, dag_path):
        """Each DAG file should reference its expected dag_id (directly or via variable)."""
        with open(dag_path, "r") as f:
            source = f.read()

        # DAG ID can be a literal string or imported variable (e.g. dag_id=DAG_ID)
        has_literal_id = dag_id in source
        has_variable_id = "dag_id=" in source
        assert has_literal_id or has_variable_id, (
            f"Expected dag_id '{dag_id}' or dag_id= assignment not found in {dag_path}"
        )

    @pytest.mark.parametrize("dag_id,dag_path", DAG_FILES.items())
    def test_dag_file_has_schedule(self, dag_id, dag_path):
        """Each DAG file should define a schedule."""
        with open(dag_path, "r") as f:
            source = f.read()

        has_schedule = "schedule" in source or "schedule_interval" in source
        assert has_schedule, f"{dag_id} does not define a schedule"

    @pytest.mark.parametrize("dag_id,dag_path", DAG_FILES.items())
    def test_dag_file_has_task_dependencies(self, dag_id, dag_path):
        """Each DAG file should define task dependencies using >>."""
        with open(dag_path, "r") as f:
            source = f.read()

        assert ">>" in source, f"{dag_id} does not define task dependencies (>>)"
